package explorer

import (
	"reflect"
	"slices"
	"strings"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// An account's or contract's part in a transaction: the clickhouse
// pipeline's account_activity has one row per address and transaction, with
// these roles as a bitmask. Addresses are G… (muxed accounts as their G…)
// and C…; claimable balances and liquidity pools are left out.
const (
	RoleSource       uint8 = 1 << iota // the transaction's source
	RoleFeePayer                       // pays the fee: the source, or a fee bump's payer
	RoleOpSource                       // an operation's source
	RoleParticipant                    // named in an operation's body: destination, trustor, sponsor, claimant, contract, argument, authorizer…
	RoleTransfer                       // either side of a token transfer
	RoleChanged                        // owns a ledger entry the transaction changed: an account, trustline, offer, data entry, or a contract's storage and SAC balances
	RoleTrade                          // trades: the source of an offer or liquidity pool operation, or either side of a fill
	RoleContractCall                   // the source of an invoke_host_function, extend_footprint_ttl or restore_footprint operation
)

// The operations that give their source RoleTrade or RoleContractCall,
// whether or not the transaction succeeded. The clickhouse package derives
// the same bits from the operations and trades tables (activity_roles.go).
var (
	TradeOperations = []xdr.OperationType{
		xdr.OperationTypeManageSellOffer, xdr.OperationTypeCreatePassiveSellOffer, xdr.OperationTypeManageBuyOffer,
		xdr.OperationTypeLiquidityPoolDeposit, xdr.OperationTypeLiquidityPoolWithdraw,
	}
	ContractCallOperations = []xdr.OperationType{
		xdr.OperationTypeInvokeHostFunction, xdr.OperationTypeExtendFootprintTtl, xdr.OperationTypeRestoreFootprint,
	}
)

// ActivityRow is one address's part in one transaction.
type ActivityRow struct {
	Account string
	TxIndex uint32
	Roles   uint8
}

// activitySet collects the addresses of one transaction.
type activitySet struct {
	tx    uint32
	roles map[string]uint8
}

func (s activitySet) add(address string, role uint8) {
	if strings.HasPrefix(address, "G") || strings.HasPrefix(address, "C") {
		s.roles[address] |= role
	}
}

// activityOf collects a transaction's addresses: from the envelope, the
// operations, the fills and the ledger entries it changed. Token transfers
// are added per ledger (activityRows).
func activityOf(tx ingest.LedgerTransaction, ops []OperationRow, trades []TradeRow, passphrase string) (activitySet, error) {
	s := activitySet{tx: tx.Index, roles: map[string]uint8{}}
	src, _, err := accountStrings(tx.Envelope.SourceAccount())
	if err != nil {
		return s, err
	}
	s.add(src, RoleSource)
	fee, _, err := accountStrings(tx.Envelope.FeeAccount())
	if err != nil {
		return s, err
	}
	s.add(fee, RoleFeePayer)
	for i, op := range tx.Envelope.Operations() {
		s.add(ops[i].SourceAccount, RoleOpSource)
		if slices.Contains(TradeOperations, op.Body.Type) {
			s.add(ops[i].SourceAccount, RoleTrade)
		}
		if slices.Contains(ContractCallOperations, op.Body.Type) {
			s.add(ops[i].SourceAccount, RoleContractCall)
		}
		if err := walkAddresses(reflect.ValueOf(op.Body), func(a string) { s.add(a, RoleParticipant) }); err != nil {
			return s, err
		}
	}
	for _, t := range trades {
		s.add(t.Seller, RoleTrade)
		s.add(t.Buyer, RoleTrade)
	}
	changes, err := tx.GetChanges()
	if err != nil {
		return s, err
	}
	for _, ch := range changes {
		e := ch.Post
		if e == nil {
			e = ch.Pre
		}
		if e == nil {
			continue
		}
		owners, err := entryOwners(*e, passphrase)
		if err != nil {
			return s, err
		}
		for _, o := range owners {
			s.add(o, RoleChanged)
		}
	}
	return s, nil
}

// activityRows adds each transfer's sides to its transaction and returns the
// rows, ordered by transaction and address.
func activityRows(seq uint32, sets []activitySet, transfers []TransferRow) []ActivityRow {
	byTx := make(map[uint32]activitySet, len(sets))
	for _, s := range sets {
		byTx[s.tx] = s
	}
	for _, t := range transfers {
		if s, ok := byTx[t.TxIndex]; ok {
			s.add(t.From, RoleTransfer)
			s.add(t.To, RoleTransfer)
		}
	}
	var out []ActivityRow
	for _, s := range sets {
		start := len(out)
		for a, r := range s.roles {
			out = append(out, ActivityRow{Account: a, TxIndex: s.tx, Roles: r})
		}
		slices.SortFunc(out[start:], func(a, b ActivityRow) int { return strings.Compare(a.Account, b.Account) })
	}
	return out
}

// entryOwners returns whom a ledger entry belongs to.
func entryOwners(e xdr.LedgerEntry, passphrase string) ([]string, error) {
	var id xdr.AccountId
	switch e.Data.Type {
	case xdr.LedgerEntryTypeAccount:
		id = e.Data.MustAccount().AccountId
	case xdr.LedgerEntryTypeTrustline:
		id = e.Data.MustTrustLine().AccountId
	case xdr.LedgerEntryTypeOffer:
		id = e.Data.MustOffer().SellerId
	case xdr.LedgerEntryTypeData:
		id = e.Data.MustData().AccountId
	case xdr.LedgerEntryTypeContractData:
		var out []string
		if c, ok := addressString(e.Data.MustContractData().Contract); ok {
			out = append(out, c)
		}
		if h, _, ok := sacBalance(e, passphrase); ok {
			c, err := contractStrkey(xdr.ContractId(h))
			if err != nil {
				return nil, err
			}
			out = append(out, c)
		}
		return out, nil
	default:
		return nil, nil
	}
	a, err := id.GetAddress()
	if err != nil {
		return nil, err
	}
	return []string{a}, nil
}

// addressString is an ScAddress's G… or C… (a muxed account's G…, a muxed
// contract's C…); false for claimable balances and liquidity pools.
func addressString(a xdr.ScAddress) (string, bool) {
	switch a.Type {
	case xdr.ScAddressTypeScAddressTypeAccount:
		s, err := a.AccountId.GetAddress()
		return s, err == nil
	case xdr.ScAddressTypeScAddressTypeContract:
		s, err := contractStrkey(*a.ContractId)
		return s, err == nil
	case xdr.ScAddressTypeScAddressTypeMuxedAccount:
		s, err := strkey.Encode(strkey.VersionByteAccountID, a.MuxedAccount.Ed25519[:])
		return s, err == nil
	case xdr.ScAddressTypeScAddressTypeMuxedContract:
		s, err := contractStrkey(a.MuxedContract.ContractId)
		return s, err == nil
	}
	return "", false
}

var (
	accountIDType    = reflect.TypeFor[xdr.AccountId]()
	muxedAccountType = reflect.TypeFor[xdr.MuxedAccount]()
	scAddressType    = reflect.TypeFor[xdr.ScAddress]()
	// Assets name their issuer, who takes no part in a payment of them.
	skipped = map[reflect.Type]bool{
		reflect.TypeFor[xdr.Asset]():            true,
		reflect.TypeFor[xdr.TrustLineAsset]():   true,
		reflect.TypeFor[xdr.ChangeTrustAsset](): true,
	}
)

// walkAddresses calls found for every account and contract address in an
// XDR value (an operation's body): accounts, muxed accounts and ScAddresses,
// at any depth (contract arguments and authorization trees included), but
// not the issuers of assets.
func walkAddresses(v reflect.Value, found func(string)) error {
	if !v.IsValid() {
		return nil
	}
	t := v.Type()
	switch {
	case skipped[t]:
		return nil
	case t == accountIDType:
		id := v.Interface().(xdr.AccountId)
		a, err := id.GetAddress()
		if err != nil {
			return err
		}
		found(a)
		return nil
	case t == muxedAccountType:
		id := v.Interface().(xdr.MuxedAccount).ToAccountId()
		a, err := id.GetAddress()
		if err != nil {
			return err
		}
		found(a)
		return nil
	case t == scAddressType:
		if a, ok := addressString(v.Interface().(xdr.ScAddress)); ok {
			found(a)
		}
		return nil
	}
	switch t.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			return nil
		}
		return walkAddresses(v.Elem(), found)
	case reflect.Struct:
		for i := range t.NumField() {
			if t.Field(i).IsExported() {
				if err := walkAddresses(v.Field(i), found); err != nil {
					return err
				}
			}
		}
	case reflect.Slice, reflect.Array:
		if t.Elem().Kind() == reflect.Uint8 {
			return nil
		}
		for i := range v.Len() {
			if err := walkAddresses(v.Index(i), found); err != nil {
				return err
			}
		}
	}
	return nil
}
