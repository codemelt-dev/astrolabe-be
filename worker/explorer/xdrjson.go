package explorer

import (
	"crypto/sha256"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// A generic XDR → JSON encoder for operation details (the decoded
// details, as jsonb). It walks the generated XDR types by reflection, so
// every operation type, including ones added by future SDK versions, gets
// complete, deterministic details:
//
//   - structs become objects with snake_case field names (extension points
//     are omitted);
//   - unions become {"<arm>": value}; a void arm becomes the discriminant's
//     name, e.g. "contract_executable_stellar_asset";
//   - enums become their snake_case name without the type prefix;
//   - 64-bit integers are decimal strings, 32-bit and smaller are numbers;
//   - accounts, muxed accounts, contracts, claimable balances and signer
//     keys are strkeys; assets are "native" or "CODE:ISSUER"; hashes and
//     other opaque bytes are lower-case hex (byte strings over 1 KiB, e.g.
//     uploaded WASM, are summarised as {"size": n, "sha256": "…"});
//   - ScVal uses the ScVal encoding (scval.go).

// maxInlineBytes is the largest byte string written out in full.
const maxInlineBytes = 1024

type unionType interface {
	SwitchFieldName() string
	ArmForSwitch(int32) (string, bool)
}

type enumType interface {
	String() string
	ValidEnum(int32) bool
}

var (
	unionIface = reflect.TypeFor[unionType]()
	enumIface  = reflect.TypeFor[enumType]()
)

// special encoders for types whose JSON is not their structure.
var special map[reflect.Type]func(dst []byte, v reflect.Value) ([]byte, error)

func init() {
	special = map[reflect.Type]func([]byte, reflect.Value) ([]byte, error){
		reflect.TypeFor[xdr.ScVal](): func(dst []byte, v reflect.Value) ([]byte, error) {
			return appendScVal(dst, v.Interface().(xdr.ScVal))
		},
		reflect.TypeFor[xdr.ScAddress](): func(dst []byte, v reflect.Value) ([]byte, error) {
			return appendScAddress(dst, v.Interface().(xdr.ScAddress))
		},
		reflect.TypeFor[xdr.AccountId](): func(dst []byte, v reflect.Value) ([]byte, error) {
			a := v.Interface().(xdr.AccountId)
			s, err := a.GetAddress()
			if err != nil {
				return nil, err
			}
			return appendJSONString(dst, s), nil
		},
		reflect.TypeFor[xdr.MuxedAccount](): func(dst []byte, v reflect.Value) ([]byte, error) {
			m := v.Interface().(xdr.MuxedAccount)
			s, err := m.GetAddress()
			if err != nil {
				return nil, err
			}
			return appendJSONString(dst, s), nil
		},
		reflect.TypeFor[xdr.ContractId](): func(dst []byte, v reflect.Value) ([]byte, error) {
			s, err := contractStrkey(v.Interface().(xdr.ContractId))
			if err != nil {
				return nil, err
			}
			return appendJSONString(dst, s), nil
		},
		reflect.TypeFor[xdr.ClaimableBalanceId](): func(dst []byte, v reflect.Value) ([]byte, error) {
			s, err := v.Interface().(xdr.ClaimableBalanceId).EncodeToStrkey()
			if err != nil {
				return nil, err
			}
			return appendJSONString(dst, s), nil
		},
		reflect.TypeFor[xdr.SignerKey](): func(dst []byte, v reflect.Value) ([]byte, error) {
			k := v.Interface().(xdr.SignerKey)
			s, err := k.GetAddress()
			if err != nil {
				return nil, err
			}
			return appendJSONString(dst, s), nil
		},
		reflect.TypeFor[xdr.Asset](): func(dst []byte, v reflect.Value) ([]byte, error) {
			return appendAsset(dst, v.Interface().(xdr.Asset))
		},
		reflect.TypeFor[xdr.ChangeTrustAsset](): func(dst []byte, v reflect.Value) ([]byte, error) {
			a := v.Interface().(xdr.ChangeTrustAsset)
			if a.Type == xdr.AssetTypeAssetTypePoolShare {
				dst = append(dst, `{"liquidity_pool":`...)
				dst, err := appendXDR(dst, reflect.ValueOf(a.LiquidityPool))
				if err != nil {
					return nil, err
				}
				return append(dst, '}'), nil
			}
			return appendAsset(dst, a.ToAsset())
		},
		reflect.TypeFor[xdr.TrustLineAsset](): func(dst []byte, v reflect.Value) ([]byte, error) {
			a := v.Interface().(xdr.TrustLineAsset)
			if a.Type == xdr.AssetTypeAssetTypePoolShare {
				dst = append(dst, `{"liquidity_pool_id":`...)
				id := a.MustLiquidityPoolId()
				dst = appendHexString(dst, id[:])
				return append(dst, '}'), nil
			}
			return appendAsset(dst, a.ToAsset())
		},
		reflect.TypeFor[xdr.AssetCode](): func(dst []byte, v reflect.Value) ([]byte, error) {
			c := v.Interface().(xdr.AssetCode)
			var code string
			switch c.Type {
			case xdr.AssetTypeAssetTypeCreditAlphanum4:
				a := c.MustAssetCode4()
				code = strings.TrimRight(string(a[:]), "\x00")
			case xdr.AssetTypeAssetTypeCreditAlphanum12:
				a := c.MustAssetCode12()
				code = strings.TrimRight(string(a[:]), "\x00")
			default:
				return nil, fmt.Errorf("unknown asset code type %d", c.Type)
			}
			return appendXDRString(dst, code), nil
		},
		reflect.TypeFor[xdr.Price](): func(dst []byte, v reflect.Value) ([]byte, error) {
			p := v.Interface().(xdr.Price)
			dst = append(dst, `{"n":`...)
			dst = strconv.AppendInt(dst, int64(p.N), 10)
			dst = append(dst, `,"d":`...)
			dst = strconv.AppendInt(dst, int64(p.D), 10)
			return append(dst, '}'), nil
		},
	}
}

func appendAsset(dst []byte, a xdr.Asset) ([]byte, error) {
	var typ, code, issuer string
	if err := a.Extract(&typ, &code, &issuer); err != nil {
		return nil, err
	}
	if a.Type == xdr.AssetTypeAssetTypeNative {
		return append(dst, `"native"`...), nil
	}
	if !textSafe(code) {
		return appendXDRString(dst, code+":"+issuer), nil
	}
	return appendJSONString(dst, code+":"+issuer), nil
}

// fieldNames caches snake_case field names per struct type.
var fieldNames sync.Map // reflect.Type → []string

func structFieldNames(t reflect.Type) []string {
	if v, ok := fieldNames.Load(t); ok {
		return v.([]string)
	}
	names := make([]string, t.NumField())
	for i := range names {
		names[i] = snakeCase(t.Field(i).Name)
	}
	fieldNames.Store(t, names)
	return names
}

var extensionPoint = reflect.TypeFor[xdr.ExtensionPoint]()

func appendXDR(dst []byte, v reflect.Value) ([]byte, error) {
	if !v.IsValid() {
		return append(dst, "null"...), nil
	}
	t := v.Type()
	if enc, ok := special[t]; ok {
		return enc(dst, v)
	}
	if t.Implements(unionIface) && t.Kind() == reflect.Struct {
		return appendUnion(dst, v)
	}
	switch t.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			return append(dst, "null"...), nil
		}
		return appendXDR(dst, v.Elem())
	case reflect.Struct:
		names := structFieldNames(t)
		dst = append(dst, '{')
		first := true
		for i := range t.NumField() {
			if t.Field(i).Type == extensionPoint || !t.Field(i).IsExported() {
				continue
			}
			dst = appendKey(dst, names[i], first)
			first = false
			var err error
			if dst, err = appendXDR(dst, v.Field(i)); err != nil {
				return nil, err
			}
		}
		return append(dst, '}'), nil
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return appendBytes(dst, v.Bytes()), nil
		}
		if v.IsNil() {
			return append(dst, "[]"...), nil
		}
		dst = append(dst, '[')
		for i := range v.Len() {
			if i > 0 {
				dst = append(dst, ',')
			}
			var err error
			if dst, err = appendXDR(dst, v.Index(i)); err != nil {
				return nil, err
			}
		}
		return append(dst, ']'), nil
	case reflect.Array:
		if t.Elem().Kind() == reflect.Uint8 {
			b := make([]byte, v.Len())
			reflect.Copy(reflect.ValueOf(b), v)
			return appendBytes(dst, b), nil
		}
		dst = append(dst, '[')
		for i := range v.Len() {
			if i > 0 {
				dst = append(dst, ',')
			}
			var err error
			if dst, err = appendXDR(dst, v.Index(i)); err != nil {
				return nil, err
			}
		}
		return append(dst, ']'), nil
	case reflect.String:
		return appendXDRString(dst, v.String()), nil
	case reflect.Bool:
		return strconv.AppendBool(dst, v.Bool()), nil
	case reflect.Int32:
		if t.Implements(enumIface) {
			return appendJSONString(dst, enumValueName(v)), nil
		}
		return strconv.AppendInt(dst, v.Int(), 10), nil
	case reflect.Int8, reflect.Int16:
		return strconv.AppendInt(dst, v.Int(), 10), nil
	case reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return strconv.AppendUint(dst, v.Uint(), 10), nil
	case reflect.Int64, reflect.Int:
		return appendQuotedInt(dst, v.Int()), nil
	case reflect.Uint64, reflect.Uint:
		return appendQuotedUint(dst, v.Uint()), nil
	default:
		return nil, fmt.Errorf("xdr json: unsupported type %s", t)
	}
}

// appendBytes writes opaque bytes as hex, or a digest for large ones.
func appendBytes(dst []byte, b []byte) []byte {
	if len(b) <= maxInlineBytes {
		return appendHexString(dst, b)
	}
	sum := sha256.Sum256(b)
	dst = append(dst, `{"size":`...)
	dst = strconv.AppendInt(dst, int64(len(b)), 10)
	dst = append(dst, `,"sha256":`...)
	dst = appendHexString(dst, sum[:])
	return append(dst, '}')
}

// enumValueName is the snake_case name of a generated enum value without
// its type-name prefix: HostFunctionTypeHostFunctionTypeInvokeContract →
// "host_function_type_invoke_contract".
func enumValueName(v reflect.Value) string {
	s := v.Interface().(enumType).String()
	return snakeCase(strings.TrimPrefix(s, v.Type().Name()))
}

func appendUnion(dst []byte, v reflect.Value) ([]byte, error) {
	u := v.Interface().(unionType)
	sw := v.FieldByName(u.SwitchFieldName())
	if !sw.IsValid() {
		return nil, fmt.Errorf("xdr json: %s has no switch field", v.Type())
	}
	var disc int32
	switch sw.Kind() {
	case reflect.Int32:
		disc = int32(sw.Int())
	case reflect.Uint32:
		disc = int32(sw.Uint())
	default:
		return nil, fmt.Errorf("xdr json: %s switch field has kind %s", v.Type(), sw.Kind())
	}
	arm, ok := u.ArmForSwitch(disc)
	if !ok {
		return nil, fmt.Errorf("xdr json: %s has no arm for %d", v.Type(), disc)
	}
	if arm == "" {
		// Void arm: the discriminant alone carries the meaning.
		if sw.Type().Implements(enumIface) {
			return appendJSONString(dst, enumValueName(sw)), nil
		}
		return appendJSONString(dst, "v"+strconv.Itoa(int(disc))), nil
	}
	dst = append(dst, '{')
	dst = appendKey(dst, snakeCase(arm), true)
	dst, err := appendXDR(dst, v.FieldByName(arm))
	if err != nil {
		return nil, err
	}
	return append(dst, '}'), nil
}

// OperationDetails returns the details of an operation body: the JSON of its
// arm's value. Operations without a body (inflation,
// end_sponsoring_future_reserves) have "{}"; a body that is not a struct
// (account_merge's destination) is wrapped as {"destination": …}.
func OperationDetails(body xdr.OperationBody) (string, error) {
	arm, ok := body.ArmForSwitch(int32(body.Type))
	if !ok {
		return "", fmt.Errorf("unknown operation type %d", body.Type)
	}
	if arm == "" {
		return "{}", nil
	}
	v := reflect.ValueOf(body).FieldByName(arm)
	if v.Kind() == reflect.Pointer && !v.IsNil() {
		v = v.Elem()
	}
	var b []byte
	var err error
	if v.Kind() == reflect.Struct && !v.Type().Implements(unionIface) && special[v.Type()] == nil {
		b, err = appendXDR(nil, v)
	} else {
		b = append(b, '{')
		b = appendKey(b, snakeCase(arm), true)
		if b, err = appendXDR(b, v); err == nil {
			b = append(b, '}')
		}
	}
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// invokeDetails is invoke_host_function's details, slimmed: the host
// function as OperationDetails writes it (contract, function and arguments,
// or the upload or creation), and "auth", the addresses that authorized the
// call, in order (the operation's source for source-account credentials).
// The authorization entries themselves, with their signatures, nonces and the
// invocation tree that repeats the call, stay in the archive.
func invokeDetails(op xdr.InvokeHostFunctionOp, source string) (string, error) {
	b, err := appendXDR(append([]byte(nil), `{"host_function":`...), reflect.ValueOf(op.HostFunction))
	if err != nil {
		return "", err
	}
	b = append(b, `,"auth":[`...)
	for i, a := range op.Auth {
		if i > 0 {
			b = append(b, ',')
		}
		switch a.Credentials.Type {
		case xdr.SorobanCredentialsTypeSorobanCredentialsSourceAccount:
			b = appendJSONString(b, source)
		case xdr.SorobanCredentialsTypeSorobanCredentialsAddress:
			b, err = appendScAddress(b, a.Credentials.MustAddress().Address)
		default: // a credentials type this build doesn't know: kept whole
			b, err = appendXDR(b, reflect.ValueOf(a.Credentials))
		}
		if err != nil {
			return "", err
		}
	}
	return string(append(b, "]}"...)), nil
}
