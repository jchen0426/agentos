// Package safetext makes text that came from a Worker safe to show on a terminal. Run results
// and failure messages are written by workflow code agentd does not trust: printed raw, an
// escape sequence in them could rewrite the operator's screen, and a newline could forge a
// line of agentctl's output.
package safetext

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf16"
	"unicode/utf8"
)

// Line returns s as one line of printable text. Every rune unicode.IsPrint rejects (controls,
// newlines and tabs, format characters such as bidi overrides, spaces other than the ASCII
// space), and every blank one, is written as a Go escape: \x1b, \n, \u202e, \u3164. Invalid
// UTF-8 becomes U+FFFD.
func Line(s string) string {
	var b strings.Builder
	for _, r := range s {
		if visible(r) {
			b.WriteRune(r)
			continue
		}
		q := strconv.QuoteRuneToASCII(r) // a blank rune is printable to strconv: it must be escaped all the same
		b.WriteString(q[1 : len(q)-1])
	}
	return b.String()
}

// Text is s as lines of printable text: like Line, but newlines and tabs stay, so that a patch
// or a reason reads as it was written. A carriage return is escaped: it could overwrite a line.
func Text(s string) string {
	var b strings.Builder
	for _, r := range s {
		if visible(r) || r == '\n' || r == '\t' {
			b.WriteRune(r)
			continue
		}
		q := strconv.QuoteRuneToASCII(r) // a blank rune is printable to strconv: it must be escaped all the same
		b.WriteString(q[1 : len(q)-1])
	}
	return b.String()
}

// JSON rewrites the JSON text b so that it decodes to the same value but holds no raw rune
// that is not printable: those become \u escapes. JSON encoders (encoding/json, protojson,
// slog) escape only what JSON requires, the runes below U+0020, and leave DEL, C1 controls
// and format characters raw. Valid JSON has only ASCII outside its strings, so only the
// strings change.
func JSON(b []byte) []byte {
	out := make([]byte, 0, len(b))
	for len(b) > 0 {
		r, n := utf8.DecodeRune(b)
		switch {
		case r < utf8.RuneSelf && r != 0x7f:
			out = append(out, byte(r))
		case r == utf8.RuneError && n == 1:
			out = append(out, `\ufffd`...)
		case visible(r):
			out = append(out, b[:n]...)
		default:
			out = appendEscape(out, r)
		}
		b = b[n:]
	}
	return out
}

// visible reports whether r shows on a terminal as itself: printable (unicode.IsPrint), and not
// one of the printable runes that show as nothing or as a blank, which could hide a name in a
// model's patch (a parameter called U+3164 looks like a space): the Hangul fillers, the Braille
// blank, a blank musical symbol, and the marks that draw nothing (variation selectors, the
// combining grapheme joiner, Khmer inherent vowels, Mongolian free variation selectors).
func visible(r rune) bool {
	switch {
	case r == 0x034f, r == 0x115f, r == 0x1160, r == 0x17b4, r == 0x17b5, r >= 0x180b && r <= 0x180d, r == 0x180f,
		r == 0x2800, r == 0x3164, r >= 0xfe00 && r <= 0xfe0f, r == 0xffa0, r == 0x1d159, r >= 0xe0100 && r <= 0xe01ef:
		return false
	}
	return unicode.IsPrint(r)
}

func appendEscape(out []byte, r rune) []byte {
	if r > 0xffff {
		r1, r2 := utf16.EncodeRune(r)
		return appendEscape(appendEscape(out, r1), r2)
	}
	return fmt.Appendf(out, `\u%04x`, r)
}

// JSONWriter passes everything written to w through JSON. Each Write must hold whole JSON
// texts, as slog's JSON handler writes one record per call.
func JSONWriter(w io.Writer) io.Writer { return jsonWriter{w} }

type jsonWriter struct{ w io.Writer }

func (j jsonWriter) Write(p []byte) (int, error) {
	if _, err := j.w.Write(JSON(p)); err != nil {
		return 0, err
	}
	return len(p), nil
}
