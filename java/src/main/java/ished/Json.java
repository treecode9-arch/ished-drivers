package ished;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

/** JSON as IshedDB's wire wants it (Java has none of its own): Date/Instant, Long beyond 2**53 and byte[] tagged. */
final class Json {
    private static final long SAFE_INT = (1L << 53) - 1;
    private static final java.time.format.DateTimeFormatter DATE =
        java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(java.time.ZoneOffset.UTC);

    private Json() {}

    static String write(Object v) {
        StringBuilder b = new StringBuilder();
        write(b, v);
        return b.toString();
    }

    private static void write(StringBuilder b, Object v) {
        if (v == null) b.append("null");
        else if (v instanceof String s) string(b, s);
        else if (v instanceof Boolean x) b.append(x);
        else if (v instanceof Date d) tagged(b, "~date", DATE.format(d.toInstant()));
        else if (v instanceof Instant i) tagged(b, "~date", DATE.format(i));
        else if (v instanceof byte[] bytes) tagged(b, "~bytes", Base64.getEncoder().encodeToString(bytes));
        else if (v instanceof Long l && Math.abs(l) > SAFE_INT) tagged(b, "~int", l.toString());
        else if (v instanceof Double d && (d.isNaN() || d.isInfinite())) throw new IshedException("invalid-argument", "a number must be finite", 0);
        else if (v instanceof Number n) b.append(n);
        else if (v instanceof Map<?, ?> m) {
            b.append('{');
            boolean first = true;
            for (Map.Entry<?, ?> e : m.entrySet()) {
                if (!first) b.append(',');
                first = false;
                string(b, String.valueOf(e.getKey()));
                b.append(':');
                write(b, e.getValue());
            }
            b.append('}');
        } else if (v instanceof Collection<?> c) {
            b.append('[');
            boolean first = true;
            for (Object x : c) {
                if (!first) b.append(',');
                first = false;
                write(b, x);
            }
            b.append(']');
        } else throw new IshedException("invalid-argument", "a value of class " + v.getClass().getSimpleName() + " cannot be kept", 0);
    }

    private static void tagged(StringBuilder b, String tag, String value) {
        b.append('{');
        string(b, tag);
        b.append(':');
        string(b, value);
        b.append('}');
    }

    private static void string(StringBuilder b, String s) {
        b.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> b.append("\\\"");
                case '\\' -> b.append("\\\\");
                case '\n' -> b.append("\\n");
                case '\r' -> b.append("\\r");
                case '\t' -> b.append("\\t");
                default -> {
                    if (c < 0x20) b.append(String.format("\\u%04x", (int) c));
                    else b.append(c);
                }
            }
        }
        b.append('"');
    }

    static Object read(String text) {
        Reader r = new Reader(text);
        Object v = r.value();
        r.space();
        if (r.i != text.length()) throw new IshedException("internal", "unexpected text in an answer", 0);
        return v;
    }

    private static final class Reader {
        final String s;
        int i;

        Reader(String s) { this.s = s; }

        void space() { while (i < s.length() && Character.isWhitespace(s.charAt(i))) i++; }

        Object value() {
            space();
            char c = s.charAt(i);
            if (c == '{') return object();
            if (c == '[') return array();
            if (c == '"') return string();
            if (s.startsWith("true", i)) { i += 4; return true; }
            if (s.startsWith("false", i)) { i += 5; return false; }
            if (s.startsWith("null", i)) { i += 4; return null; }
            return number();
        }

        Object object() {
            i++;
            Document d = new Document();
            space();
            if (s.charAt(i) == '}') { i++; return untag(d); }
            while (true) {
                space();
                String k = string();
                space();
                i++; // ':'
                d.put(k, value());
                space();
                if (s.charAt(i++) == '}') return untag(d);
            }
        }

        List<Object> array() {
            i++;
            List<Object> out = new ArrayList<>();
            space();
            if (s.charAt(i) == ']') { i++; return out; }
            while (true) {
                out.add(value());
                space();
                if (s.charAt(i++) == ']') return out;
            }
        }

        String string() {
            i++;
            StringBuilder b = new StringBuilder();
            while (s.charAt(i) != '"') {
                char c = s.charAt(i++);
                if (c != '\\') { b.append(c); continue; }
                char e = s.charAt(i++);
                switch (e) {
                    case 'n' -> b.append('\n');
                    case 'r' -> b.append('\r');
                    case 't' -> b.append('\t');
                    case 'b' -> b.append('\b');
                    case 'f' -> b.append('\f');
                    case 'u' -> { b.append((char) Integer.parseInt(s.substring(i, i + 4), 16)); i += 4; }
                    default -> b.append(e);
                }
            }
            i++;
            return b.toString();
        }

        Object number() {
            int start = i;
            while (i < s.length() && "+-0123456789.eE".indexOf(s.charAt(i)) >= 0) i++;
            String n = s.substring(start, i);
            if (n.contains(".") || n.contains("e") || n.contains("E")) return Double.parseDouble(n);
            long l = Long.parseLong(n);
            return l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE ? (Object) (int) l : (Object) l;
        }

        static Object untag(Document d) {
            if (d.size() == 1) {
                Map.Entry<String, Object> e = d.entrySet().iterator().next();
                if (e.getKey().equals("~date")) return Date.from(Instant.parse((String) e.getValue()));
                if (e.getKey().equals("~int")) return Long.parseLong((String) e.getValue());
                if (e.getKey().equals("~bytes")) return Base64.getDecoder().decode((String) e.getValue());
            }
            return d;
        }
    }
}
