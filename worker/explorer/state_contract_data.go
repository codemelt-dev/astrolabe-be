package explorer

import (
	"crypto/sha256"
	"fmt"
	"math/big"

	"github.com/stellar/go-stellar-sdk/ingest/sac"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// contract_data: current Soroban contract storage (state_contract_data.sql).
// SAC balance entries also get balance_holder and balance (ingest/sac); a
// contract's instance entry gets executable, wasm_hash and (SACs) asset.

var contractDataTable = stateTable{archivable: true, table: table{
	name: "contract_data",
	key:  []string{"key_hash"},
	cols: []column{
		{"key_hash", "bytea"}, {"contract_id", "text"}, {"durability", "text"}, {"key_xdr", "bytea"},
		{"val_xdr", "bytea"}, {"key", "jsonb"}, {"val", "jsonb"}, {"balance_holder", "text"},
		{"balance", "numeric"}, {"executable", "text"}, {"wasm_hash", "bytea"}, {"asset", "text"},
		{"archived", "boolean"}, {"last_modified_ledger", "bigint"},
	},
}}

// keyHash is the identity of contract data, contract code and TTL rows:
// sha256(XDR(LedgerKey)), which is what TtlEntry.KeyHash holds.
func keyHash(k xdr.LedgerKey) ([]byte, error) {
	b, err := k.MarshalBinary()
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(b)
	return sum[:], nil
}

func durabilityName(d xdr.ContractDataDurability) (string, error) {
	switch d {
	case xdr.ContractDataDurabilityTemporary:
		return "temporary", nil
	case xdr.ContractDataDurabilityPersistent:
		return "persistent", nil
	}
	return "", fmt.Errorf("unknown contract data durability %d", d)
}

func contractDataValues(e xdr.LedgerEntry, passphrase string) ([]any, error) {
	cd := e.Data.MustContractData()
	key, err := e.LedgerKey()
	if err != nil {
		return nil, err
	}
	kh, err := keyHash(key)
	if err != nil {
		return nil, err
	}
	contract, err := cd.Contract.String()
	if err != nil {
		return nil, err
	}
	durability, err := durabilityName(cd.Durability)
	if err != nil {
		return nil, err
	}
	keyXDR, err := cd.Key.MarshalBinary()
	if err != nil {
		return nil, err
	}
	valXDR, err := cd.Val.MarshalBinary()
	if err != nil {
		return nil, err
	}
	keyJSON, err := ScValJSON(cd.Key)
	if err != nil {
		return nil, fmt.Errorf("contract data key: %w", err)
	}
	valJSON, err := ScValJSON(cd.Val)
	if err != nil {
		return nil, fmt.Errorf("contract data value: %w", err)
	}
	var holder, balance any
	if h, amount, ok := sacBalance(e, passphrase); ok {
		if holder, err = contractStrkey(xdr.ContractId(h)); err != nil {
			return nil, err
		}
		balance = amount.String()
	}
	executable, wasmHash, asset, err := instanceOf(e, passphrase)
	if err != nil {
		return nil, fmt.Errorf("contract %s instance: %w", contract, err)
	}
	return []any{
		kh, contract, durability, keyXDR,
		valXDR, keyJSON, valJSON, holder,
		balance, executable, wasmHash, asset,
		false, int64(e.LastModifiedLedgerSeq),
	}, nil
}

// instanceOf returns what a contract's instance entry says about the
// contract: its executable ("wasm" or "stellar_asset"), the Wasm hash, and
// a Stellar Asset Contract's asset. Other entries give three nils.
func instanceOf(e xdr.LedgerEntry, passphrase string) (executable, wasmHash, asset any, err error) {
	cd := e.Data.MustContractData()
	if cd.Key.Type != xdr.ScValTypeScvLedgerKeyContractInstance {
		return nil, nil, nil, nil
	}
	inst, ok := cd.Val.GetInstance()
	if !ok {
		return nil, nil, nil, fmt.Errorf("value is a %s, not an instance", cd.Val.Type)
	}
	switch inst.Executable.Type {
	case xdr.ContractExecutableTypeContractExecutableWasm:
		h := inst.Executable.MustWasmHash()
		return "wasm", h[:], nil, nil
	case xdr.ContractExecutableTypeContractExecutableStellarAsset:
		if a, ok := sac.AssetFromContractData(e, passphrase); ok {
			if asset, err = assetName(a); err != nil {
				return nil, nil, nil, err
			}
		} else if native, err := xdr.MustNewNativeAsset().ContractID(passphrase); err == nil &&
			cd.Contract.ContractId != nil && *cd.Contract.ContractId == native {
			asset = "native" // the SDK leaves XLM's contract out
		}
		return "stellar_asset", nil, asset, nil
	}
	return nil, nil, nil, fmt.Errorf("unknown executable type %d", inst.Executable.Type)
}

// sacBalance is ingest/sac's ContractBalanceFromContractData, which reads a
// contract's balance of a Stellar Asset Contract (the holder and the
// amount), but for XLM's contract too: the SDK leaves lumens out, for
// Horizon's asset stats, though their balance entries have the same shape.
func sacBalance(e xdr.LedgerEntry, passphrase string) ([32]byte, *big.Int, bool) {
	if h, amount, ok := sac.ContractBalanceFromContractData(e, passphrase); ok {
		return h, amount, true
	}
	cd := e.Data.MustContractData()
	native, err := xdr.MustNewNativeAsset().ContractID(passphrase)
	if err != nil || cd.Contract.ContractId == nil || *cd.Contract.ContractId != native {
		return [32]byte{}, nil, false
	}
	// The same entry under any other contract passes the SDK's checks. e and
	// cd are copies: the caller's entry is untouched.
	other := xdr.ContractId{}
	cd.Contract = xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeContract, ContractId: &other}
	e.Data.ContractData = &cd
	return sac.ContractBalanceFromContractData(e, passphrase)
}
