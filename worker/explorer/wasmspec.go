package explorer

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// The functions of a contract, read from its Wasm: Soroban's compiler
// writes them into the custom section contractspecv0, as a stream of
// ScSpecEntry XDR (functions, their argument and result types, and the
// structs, unions, enums, errors and events they use).

const specSection = "contractspecv0"

// wasmSpec returns the code's contractspecv0 entries as JSON (an array of
// the XDR's JSON), or nil when the module has no such section or it doesn't
// decode: the code is stored either way.
func wasmSpec(code []byte) any {
	payload, err := customSection(code, specSection)
	if err != nil || payload == nil {
		return nil
	}
	var entries []xdr.ScSpecEntry
	r := bytes.NewReader(payload)
	for r.Len() > 0 {
		var e xdr.ScSpecEntry
		if _, err := xdr.Unmarshal(r, &e); err != nil {
			return nil
		}
		entries = append(entries, e)
	}
	b, err := appendXDR(nil, reflect.ValueOf(entries))
	if err != nil {
		return nil
	}
	return string(b)
}

// customSection returns the concatenated payloads of a Wasm module's custom
// sections with the given name (nil if there is none).
func customSection(code []byte, name string) ([]byte, error) {
	if len(code) < 8 || !bytes.Equal(code[:4], []byte("\x00asm")) {
		return nil, errors.New("not a Wasm module")
	}
	var out []byte
	r := bytes.NewReader(code[8:])
	for r.Len() > 0 {
		id, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		size, err := binary.ReadUvarint(r)
		if err != nil || size > uint64(r.Len()) {
			return nil, fmt.Errorf("section %d: bad size", id)
		}
		body := make([]byte, size)
		if _, err := r.Read(body); err != nil && size > 0 {
			return nil, err
		}
		if id != 0 {
			continue
		}
		br := bytes.NewReader(body)
		n, err := binary.ReadUvarint(br)
		if err != nil || n > uint64(br.Len()) {
			return nil, errors.New("custom section: bad name")
		}
		sectionName := make([]byte, n)
		_, _ = br.Read(sectionName)
		if string(sectionName) == name {
			out = append(out, body[len(body)-br.Len():]...)
		}
	}
	return out, nil
}
