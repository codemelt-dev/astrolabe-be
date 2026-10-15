package explorer

import (
	"fmt"
	"reflect"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// OperationRow is one operation, as the clickhouse pipeline stores it
// (clickhouse/operations.sql).
type OperationRow struct {
	LedgerSequence     uint32
	TxIndex            uint32 // 1-based
	OpIndex            uint32 // 0-based
	ID                 int64  // toid(ledger, tx_index, op_index + 1)
	Type               int32  // xdr.OperationType
	SourceAccount      string
	SourceAccountMuxed string
	Details            string // JSON (OperationDetails)
	Successful         bool   // the transaction's outcome
	// ResultCode is the operation's own result (payment_underfunded,
	// invoke_host_function_trapped, op_success's equivalents such as
	// payment_success, or op_bad_auth …); "" when the transaction failed
	// before its operations ran (its own result_code says why).
	ResultCode string
	// ReturnValue is a successful Soroban call's return value, as JSON.
	ReturnValue string
}

func (r *OperationRow) size() int64 { return 150 + int64(len(r.Details)+len(r.ReturnValue)) }

// operationRowsOf extracts the operations of one transaction:
// type, effective source, decoded details, result and the transaction's
// outcome.
func operationRowsOf(tx ingest.LedgerTransaction, seq uint32) ([]OperationRow, error) {
	ops := tx.Envelope.Operations()
	results, _ := tx.Result.OperationResults()
	ret, err := returnValueOf(tx)
	if err != nil {
		return nil, fmt.Errorf("return value: %w", err)
	}
	rows := make([]OperationRow, 0, len(ops))
	for i, op := range ops {
		r := OperationRow{
			LedgerSequence: seq,
			TxIndex:        tx.Index,
			OpIndex:        uint32(i),
			ID:             toid.New(int32(seq), int32(tx.Index), int32(i+1)).ToInt64(),
			Type:           int32(op.Body.Type),
			Successful:     tx.Successful(),
		}
		src := tx.Envelope.SourceAccount()
		if op.SourceAccount != nil {
			src = *op.SourceAccount
		}
		if r.SourceAccount, r.SourceAccountMuxed, err = accountStrings(src); err != nil {
			return nil, fmt.Errorf("operation %d source: %w", i, err)
		}
		if inv, ok := op.Body.GetInvokeHostFunctionOp(); ok {
			r.Details, err = invokeDetails(inv, r.SourceAccount)
			r.ReturnValue = ret
		} else {
			r.Details, err = OperationDetails(op.Body)
		}
		if err != nil {
			return nil, fmt.Errorf("operation %d (type %d) details: %w", i, op.Body.Type, err)
		}
		if i < len(results) {
			r.ResultCode = operationResultCode(results[i])
		}
		rows = append(rows, r)
	}
	return rows, nil
}

// operationResultCode names an operation's result: the code of the
// operation type's own result ("payment_underfunded"), or the generic one
// when the operation never ran ("op_bad_auth", "op_no_account").
func operationResultCode(r xdr.OperationResult) string {
	if r.Code != xdr.OperationResultCodeOpInner || r.Tr == nil {
		return enumValueName(reflect.ValueOf(r.Code))
	}
	arm, ok := r.Tr.ArmForSwitch(int32(r.Tr.Type))
	if !ok || arm == "" {
		return "op_inner"
	}
	v := reflect.ValueOf(*r.Tr).FieldByName(arm)
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return "op_inner"
		}
		v = v.Elem()
	}
	if u, ok := v.Interface().(unionType); ok {
		if code := v.FieldByName(u.SwitchFieldName()); code.IsValid() && code.Type().Implements(enumIface) {
			return enumValueName(code)
		}
	}
	return "op_inner"
}

// returnValueOf is a successful Soroban transaction's return value as JSON,
// or "".
func returnValueOf(tx ingest.LedgerTransaction) (string, error) {
	if !tx.Successful() || !tx.IsSorobanTx() {
		return "", nil
	}
	switch tx.UnsafeMeta.V {
	case 3:
		if m := tx.UnsafeMeta.MustV3(); m.SorobanMeta != nil {
			return ScValJSON(m.SorobanMeta.ReturnValue)
		}
	case 4:
		if m := tx.UnsafeMeta.MustV4(); m.SorobanMeta != nil && m.SorobanMeta.ReturnValue != nil {
			return ScValJSON(*m.SorobanMeta.ReturnValue)
		}
	}
	return "", nil
}
