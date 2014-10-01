package pkg

import (
	"flag"
	"net/url"
	"os"
	"reflect"
	"testing"

	"github.com/coreos/etcd/pkg/flags"
	"github.com/coreos/etcd/pkg/transport"
)

func TestSetFlagsFromEnv(t *testing.T) {
	fs := flag.NewFlagSet("testing", flag.ExitOnError)
	fs.String("a", "", "")
	fs.String("b", "", "")
	fs.String("c", "", "")
	fs.Parse([]string{})

	os.Clearenv()
	// flags should be settable using env vars
	os.Setenv("ETCD_A", "foo")
	// and command-line flags
	if err := fs.Set("b", "bar"); err != nil {
		t.Fatal(err)
	}
	// command-line flags take precedence over env vars
	os.Setenv("ETCD_C", "woof")
	if err := fs.Set("c", "quack"); err != nil {
		t.Fatal(err)
	}

	// first verify that flags are as expected before reading the env
	for f, want := range map[string]string{
		"a": "",
		"b": "bar",
		"c": "quack",
	} {
		if got := fs.Lookup(f).Value.String(); got != want {
			t.Fatalf("flag %q=%q, want %q", f, got, want)
		}
	}

	// now read the env and verify flags were updated as expected
	SetFlagsFromEnv(fs)
	for f, want := range map[string]string{
		"a": "foo",
		"b": "bar",
		"c": "quack",
	} {
		if got := fs.Lookup(f).Value.String(); got != want {
			t.Errorf("flag %q=%q, want %q", f, got, want)
		}
	}
}

func TestURLsFromFlags(t *testing.T) {
	tests := []struct {
		args     []string
		tlsInfo  transport.TLSInfo
		wantURLs []url.URL
		wantFail bool
	}{
		// use -urls default when no flags defined
		{
			args:    []string{},
			tlsInfo: transport.TLSInfo{},
			wantURLs: []url.URL{
				url.URL{Scheme: "http", Host: "127.0.0.1:2379"},
			},
			wantFail: false,
		},

		// explicitly setting -urls should carry through
		{
			args:    []string{"-urls=https://192.0.3.17:2930,http://127.0.0.1:1024"},
			tlsInfo: transport.TLSInfo{},
			wantURLs: []url.URL{
				url.URL{Scheme: "http", Host: "127.0.0.1:1024"},
				url.URL{Scheme: "https", Host: "192.0.3.17:2930"},
			},
			wantFail: false,
		},

		// explicitly setting -addr should carry through
		{
			args:    []string{"-addr=192.0.2.3:1024"},
			tlsInfo: transport.TLSInfo{},
			wantURLs: []url.URL{
				url.URL{Scheme: "http", Host: "192.0.2.3:1024"},
			},
			wantFail: false,
		},

		// scheme prepended to -addr should be https if TLSInfo non-empty
		{
			args: []string{"-addr=192.0.2.3:1024"},
			tlsInfo: transport.TLSInfo{
				CertFile: "/tmp/foo",
				KeyFile:  "/tmp/bar",
			},
			wantURLs: []url.URL{
				url.URL{Scheme: "https", Host: "192.0.2.3:1024"},
			},
			wantFail: false,
		},

		// explicitly setting both -urls and -addr should fail
		{
			args:     []string{"-urls=https://127.0.0.1:1024", "-addr=192.0.2.3:1024"},
			tlsInfo:  transport.TLSInfo{},
			wantURLs: nil,
			wantFail: true,
		},
	}

	for i, tt := range tests {
		fs := flag.NewFlagSet("test", flag.PanicOnError)
		fs.Var(flags.NewURLsValue("http://127.0.0.1:2379"), "urls", "")
		fs.Var(&flags.IPAddressPort{}, "addr", "")

		if err := fs.Parse(tt.args); err != nil {
			t.Errorf("#%d: failed to parse flags: %v", i, err)
			continue
		}

		gotURLs, err := URLsFromFlags(fs, "urls", "addr", tt.tlsInfo)
		if tt.wantFail != (err != nil) {
			t.Errorf("#%d: wantFail=%t, got err=%v", i, tt.wantFail, err)
			continue
		}

		if !reflect.DeepEqual(tt.wantURLs, gotURLs) {
			t.Errorf("#%d: incorrect URLs\nwant=%#v\ngot=%#v", i, tt.wantURLs, gotURLs)
		}
	}
}
