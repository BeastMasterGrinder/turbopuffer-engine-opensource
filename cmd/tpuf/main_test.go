package main

import (
	"bytes"
	"flag"
	"strings"
	"testing"
)

func TestParseVector(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      string
		want    []float32
		wantErr bool
	}{
		{name: "single", in: "0.5", want: []float32{0.5}},
		{name: "list", in: "0.1,0.2,0.3", want: []float32{0.1, 0.2, 0.3}},
		{name: "negative and integer", in: "-1,2,3.5", want: []float32{-1, 2, 3.5}},
		{name: "whitespace trimmed", in: " 0.1 , 0.2 ,0.3 ", want: []float32{0.1, 0.2, 0.3}},
		{name: "empty component", in: "0.1,,0.3", wantErr: true},
		{name: "non-numeric component", in: "0.1,abc,0.3", wantErr: true},
		{name: "empty string", in: "", wantErr: true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseVector(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parseVector(%q) error = nil, want non-nil", tt.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseVector(%q) error = %v, want nil", tt.in, err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("parseVector(%q) = %v, want %v", tt.in, got, tt.want)
			}
			for i := range tt.want {
				if got[i] != tt.want[i] {
					t.Errorf("parseVector(%q)[%d] = %v, want %v", tt.in, i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestParseNamespaceFlags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		args     []string
		wantName string
		wantErr  bool
	}{
		{name: "name only", args: []string{"demo"}, wantName: "demo"},
		{name: "name then flags", args: []string{"demo", "--dim", "4"}, wantName: "demo"},
		{name: "missing name", args: nil, wantErr: true},
		{name: "name after flags rejected", args: []string{"--dim", "4", "demo"}, wantErr: true},
		{name: "leading-dash name rejected", args: []string{"-demo"}, wantErr: true},
		{name: "trailing extra arg rejected", args: []string{"demo", "--dim", "4", "extra"}, wantErr: true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// A fresh FlagSet per case; ContinueOnError so a parse failure
			// returns rather than exits, and a discarded output so the usage
			// banner doesn't pollute test output.
			fs := flag.NewFlagSet("test", flag.ContinueOnError)
			fs.SetOutput(&bytes.Buffer{})
			fs.Int("dim", 0, "")
			got, err := parseNamespaceFlags(fs, tt.args, "test <ns>")
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parseNamespaceFlags(%v) error = nil, want non-nil", tt.args)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseNamespaceFlags(%v) error = %v, want nil", tt.args, err)
			}
			if got != tt.wantName {
				t.Errorf("parseNamespaceFlags(%v) = %q, want %q", tt.args, got, tt.wantName)
			}
		})
	}
}

func TestEnvOr(t *testing.T) {
	tests := []struct {
		name string
		val  string
		def  string
		want string
	}{
		{name: "empty returns default", val: "", def: "s3", want: "s3"},
		{name: "set returns value", val: "memory", def: "s3", want: "memory"},
	}

	const key = "TPUF_TEST_ENVOR"
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// envOr treats an empty value the same as unset, so seeding the
			// variable to "" exercises the default path without a manual unset.
			t.Setenv(key, tt.val)
			if got := envOr(key, tt.def); got != tt.want {
				t.Errorf("envOr(%q, %q) = %q, want %q", key, tt.def, got, tt.want)
			}
		})
	}
}

func TestUsageWritesToWriter(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	usage(&buf)
	out := buf.String()
	for _, want := range []string{"tpuf create", "tpuf upsert", "tpuf index", "tpuf query", "tpuf info"} {
		if !strings.Contains(out, want) {
			t.Errorf("usage output missing %q\ngot:\n%s", want, out)
		}
	}
}
