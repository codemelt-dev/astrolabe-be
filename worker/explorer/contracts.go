package explorer

import (
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// ContractCallRow is one invoke_contract operation: the clickhouse
// pipeline's contract_calls, a contract page's recent calls.
type ContractCallRow struct {
	TxIndex     uint32
	OpIndex     uint32
	ContractID  string
	Function    string
	Caller      string // the operation's source
	Successful  bool
	ReturnValue string // JSON (ScValJSON); "" when the call failed
}

// ContractCreationRow is a contract created by a transaction: its instance
// entry appeared in the transaction's changes. That includes contracts
// deployed by other contracts, which no operation names.
type ContractCreationRow struct {
	TxIndex    uint32
	OpIndex    uint32
	ContractID string
	Creator    string // the operation's source
	// ViaContract is the contract the operation invoked when that contract
	// deployed this one; "" for a create_contract operation.
	ViaContract string
	Executable  string // wasm or stellar_asset
	WasmHash    []byte // nil for a Stellar Asset Contract
	Asset       string // a Stellar Asset Contract's asset
}

// contractRowsOf returns a transaction's contract calls and the contracts it
// created.
func contractRowsOf(tx ingest.LedgerTransaction, ops []OperationRow, passphrase string) ([]ContractCallRow, []ContractCreationRow, error) {
	var calls []ContractCallRow
	var creations []ContractCreationRow
	for i, op := range tx.Envelope.Operations() {
		inv, ok := op.Body.GetInvokeHostFunctionOp()
		if !ok {
			continue
		}
		var invoked string
		if args, ok := inv.HostFunction.GetInvokeContract(); ok {
			if c, ok := addressString(args.ContractAddress); ok {
				invoked = c
			}
			calls = append(calls, ContractCallRow{
				TxIndex: tx.Index, OpIndex: uint32(i), ContractID: invoked, Function: cleanText(string(args.FunctionName)),
				Caller: ops[i].SourceAccount, Successful: tx.Successful(), ReturnValue: ops[i].ReturnValue,
			})
		}
		if !tx.Successful() {
			continue
		}
		changes, err := tx.GetOperationChanges(uint32(i))
		if err != nil {
			return nil, nil, err
		}
		for _, ch := range changes {
			if ch.ChangeType != xdr.LedgerEntryChangeTypeLedgerEntryCreated || ch.Post == nil ||
				ch.Post.Data.Type != xdr.LedgerEntryTypeContractData ||
				ch.Post.Data.MustContractData().Key.Type != xdr.ScValTypeScvLedgerKeyContractInstance {
				continue
			}
			id, ok := addressString(ch.Post.Data.MustContractData().Contract)
			if !ok {
				continue
			}
			executable, wasmHash, asset, err := instanceOf(*ch.Post, passphrase)
			if err != nil {
				return nil, nil, err
			}
			r := ContractCreationRow{TxIndex: tx.Index, OpIndex: uint32(i), ContractID: id, Creator: ops[i].SourceAccount}
			if invoked != "" && invoked != id {
				r.ViaContract = invoked
			}
			r.Executable, _ = executable.(string)
			r.WasmHash, _ = wasmHash.([]byte)
			r.Asset, _ = asset.(string)
			creations = append(creations, r)
		}
	}
	return calls, creations, nil
}
