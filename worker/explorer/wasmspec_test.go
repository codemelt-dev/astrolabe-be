package explorer

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// wasmModule builds a Wasm module from (id, payload) sections; a custom
// section's payload starts with its name.
func wasmModule(sections ...[]byte) []byte {
	m := []byte("\x00asm\x01\x00\x00\x00")
	for _, s := range sections {
		m = append(m, s[0])
		m = binary.AppendUvarint(m, uint64(len(s)-1))
		m = append(m, s[1:]...)
	}
	return m
}

func customSectionOf(name string, payload []byte) []byte {
	s := append([]byte{0}, binary.AppendUvarint(nil, uint64(len(name)))...)
	return append(append(s, name...), payload...)
}

func specPayload(t *testing.T, entries ...xdr.ScSpecEntry) []byte {
	t.Helper()
	var b bytes.Buffer
	for _, e := range entries {
		if _, err := xdr.Marshal(&b, e); err != nil {
			t.Fatal(err)
		}
	}
	return b.Bytes()
}

func TestWasmSpec(t *testing.T) {
	fn := func(name string) xdr.ScSpecEntry {
		return xdr.ScSpecEntry{Kind: xdr.ScSpecEntryKindScSpecEntryFunctionV0, FunctionV0: &xdr.ScSpecFunctionV0{
			Doc:     "says hello",
			Name:    xdr.ScSymbol(name),
			Inputs:  []xdr.ScSpecFunctionInputV0{{Name: "to", Type: xdr.ScSpecTypeDef{Type: xdr.ScSpecTypeScSpecTypeSymbol}}},
			Outputs: []xdr.ScSpecTypeDef{{Type: xdr.ScSpecTypeScSpecTypeVec, Vec: &xdr.ScSpecTypeVec{ElementType: xdr.ScSpecTypeDef{Type: xdr.ScSpecTypeScSpecTypeSymbol}}}},
		}}
	}
	// Other sections around it, and the spec split over two custom
	// sections: they are concatenated.
	code := wasmModule([]byte{1, 0x60, 0, 0}, customSectionOf("contractmetav0", []byte{1, 2, 3}),
		customSectionOf(specSection, specPayload(t, fn("hello"))), []byte{10, 0}, customSectionOf(specSection, specPayload(t, fn("bye"))))
	spec, ok := wasmSpec(code).(string)
	if !ok {
		t.Fatalf("no spec: %v", wasmSpec(code))
	}
	var entries []map[string]struct {
		Doc     string
		Name    string
		Inputs  []map[string]any
		Outputs []any
	}
	if err := json.Unmarshal([]byte(spec), &entries); err != nil {
		t.Fatalf("%v: %s", err, spec)
	}
	if len(entries) != 2 || entries[0]["function_v0"].Name != "hello" || entries[1]["function_v0"].Name != "bye" ||
		entries[0]["function_v0"].Doc != "says hello" || len(entries[0]["function_v0"].Inputs) != 1 || len(entries[0]["function_v0"].Outputs) != 1 {
		t.Fatalf("spec %s", spec)
	}

	// No spec, or one that doesn't decode: the code is stored without.
	for name, code := range map[string][]byte{
		"no section":       wasmModule([]byte{1, 0x60, 0, 0}, customSectionOf("contractmetav0", []byte{1})),
		"undecodable spec": wasmModule(customSectionOf(specSection, []byte{0, 0, 0, 9, 1})),
		"truncated":        wasmModule(customSectionOf(specSection, specPayload(t, fn("hello"))))[:20],
		"not wasm":         []byte("wwwwwwwwwwww"),
		"empty":            nil,
	} {
		if got := wasmSpec(code); got != nil {
			t.Errorf("%s: spec %v", name, got)
		}
	}
}
