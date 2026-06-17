package proxy

import (
	"testing"
)

func TestRawCodecRoundTrip(t *testing.T) {
	codec := rawCodec{}
	in := []byte{0, 1, 2, 3, 255}
	data, err := codec.Marshal(in)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	var out []byte
	if err := codec.Unmarshal(data, &out); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if string(out) != string(in) {
		t.Fatalf("Unmarshal() = %v, want %v", out, in)
	}
}

func TestRawCodecRejectsUnsupportedTypes(t *testing.T) {
	codec := rawCodec{}
	if _, err := codec.Marshal("payload"); err == nil {
		t.Fatal("Marshal() error = nil, want error")
	}
	var out string
	if err := codec.Unmarshal([]byte("payload"), &out); err == nil {
		t.Fatal("Unmarshal() error = nil, want error")
	}
}
