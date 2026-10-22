package audit

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

// The sha256 a call's record holds of its result, as anyone computes it again from the result in
// Temporal's history (the package comment): each vector's bytes and sha256 are fixed, worked out by
// this package and, apart from the two strings Python reads otherwise, by tests/test_audit_log.py's
// encoder of its own.
func TestTheResultHashOfVectors(t *testing.T) {
	for _, v := range []struct{ name, valueJSON, encoded, sha256 string }{
		{"HTML characters", `{"a":"<b>&amp;</b>"}`,
			`{"a":"\u003cb\u003e\u0026amp;\u003c/b\u003e"}`, "41b180fc5feadfe174c70f094f5be3a54a04636ddff0edab86648f11f3ea43d5"},
		{"U+2028 and U+2029", `{"s":"line\u2028para\u2029"}`,
			`{"s":"line\u2028para\u2029"}`, "8bc00e0b590cbe34e217635fbe596ec086e1646efb3eb8a05f0c60767f04751c"},
		{"control characters, quotes, backslash and slash", `{"c":"\u0000\u0001\b\f\n\r\t\u001f\u007f\"\\/"}`,
			"{\"c\":\"\\u0000\\u0001\\b\\f\\n\\r\\t\\u001f\x7f\\\"\\\\/\"}", "4e9976302ec3d6acfbe57c68541de7472b7a630c12e32e12bc5a936afb7717ae"},
		{"non-ASCII and astral characters, keys sorted by their bytes", `{"t":"é 漢 😀","😀":1,"é":2,"z":3,"Z":4,"":5}`,
			`{"":5,"Z":4,"t":"é 漢 😀","z":3,"é":2,"😀":1}`, "fa181ea07c24b86605f5a16c6846fce09a46b6d2543528bef917d92b4ce2fc5a"},
		{"numbers as written", `{"n":[1.50,1e2,-0,12345678901234567890,0.1,-1.5E-7]}`,
			`{"n":[1.50,1e2,-0,12345678901234567890,0.1,-1.5E-7]}`, "6bc66f7a0c545d4905e17275b4f37318928778ddf1d6ec95ae1c1d660af908c6"},
		{"the anchor removed, at the top only", `{"x":1,"_audit":{"seq":3,"sha256":"ab"},"o":{"_audit":true}}`,
			`{"o":{"_audit":true},"x":1}`, "dc53944e69d3b1f5104f1298cd3ce6f0ccf45871bbd0e4213aa42f4605cf4403"},
		{"a key twice: the last value", `{"k":1,"k":2}`, `{"k":2}`, "1ddca3d1f7a33ce87c75de54e2b6a7ee0e52cf11cb8b8041e102b8477d5b1a23"},
		{"no space between tokens", ` { "spaced" : [ true , false , null , { } , [ ] ] } `,
			`{"spaced":[true,false,null,{},[]]}`, "f6452154773d5bb67ced368e0927c4d6f6db80794d184282b395157ccc909da2"},
		{"invalid UTF-8 and a lone surrogate", "{\"bad\":\"\xff\",\"lone\":\"\\ud800\"}",
			"{\"bad\":\"\ufffd\",\"lone\":\"\ufffd\"}", "15b2dfd79c1a70292f71251a1fff66ecbcd7af9eb2df9722ae434b926d5ac145"},
		{"empty", `{}`, `{}`, "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"},
	} {
		b, err := ResultJSON(v.valueJSON)
		if err != nil {
			t.Errorf("%s: %v", v.name, err)
			continue
		}
		sum := sha256.Sum256(b)
		got, err := ResultSHA256(v.valueJSON)
		switch {
		case err != nil:
			t.Errorf("%s: %v", v.name, err)
		case string(b) != v.encoded:
			t.Errorf("%s: encoded %q, want %q", v.name, b, v.encoded)
		case hex.EncodeToString(sum[:]) != v.sha256 || got != v.sha256:
			t.Errorf("%s: sha256 %s, want %s", v.name, got, v.sha256)
		}
	}
	// The record's sha256 is the same one, from the value agentd decoded (the call's own record).
	if v, err := ValueSHA256(map[string]any{"o": map[string]any{"_audit": true}, "x": 1, "_audit": "gone"}); err != nil ||
		v != "dc53944e69d3b1f5104f1298cd3ce6f0ccf45871bbd0e4213aa42f4605cf4403" {
		t.Errorf("ValueSHA256: %s %v", v, err)
	}
}

// A value_json that is not one JSON object has no result hash: a verifier reports the result
// unreadable.
func TestAResultThatIsNotOneObject(t *testing.T) {
	for _, in := range []string{``, `[]`, `null`, `"x"`, `{"a":1} {"b":2}`, `{"a":1} x`, `{"a":1`} {
		if b, err := ResultJSON(in); err == nil {
			t.Errorf("%q: %q", in, b)
		}
	}
}
