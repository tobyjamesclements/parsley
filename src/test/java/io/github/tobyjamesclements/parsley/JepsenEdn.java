package io.github.tobyjamesclements.parsley;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The subset of EDN the Jepsen export uses: maps, vectors, strings, keywords, integers,
 * booleans and {@code nil}.
 *
 * <p>The export is read by the Clojure checker in {@code parsley-jepsen}, so it is written as
 * EDN rather than as a Java-specific format. This writer and reader exist so the Java side
 * can round-trip the same files: the harness's {@code check} command replays an export
 * through the {@link Oracle}, and the calibration test reads what the exporter wrote.
 * Keywords are {@link Kw}; map keys may be keywords, strings, integers or vectors.
 */
final class JepsenEdn {

    /** An EDN keyword, written as {@code :name}. */
    record Kw(String name) {
        @Override
        public String toString() {
            return ":" + name;
        }
    }

    private JepsenEdn() {
    }

    static Kw kw(String name) {
        return new Kw(name);
    }

    /** Builds a map whose keys are keywords, from alternating name, value pairs. */
    static Map<Object, Object> map(Object... namesAndValues) {
        if (namesAndValues.length % 2 != 0) {
            throw new IllegalArgumentException("map takes name, value pairs");
        }
        Map<Object, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < namesAndValues.length; i += 2) {
            Object key = namesAndValues[i];
            map.put(key instanceof String s ? kw(s) : key, namesAndValues[i + 1]);
        }
        return map;
    }

    static String write(Object value) {
        StringBuilder out = new StringBuilder();
        write(value, out, 0);
        return out.toString();
    }

    private static void write(Object value, StringBuilder out, int depth) {
        if (value == null) {
            out.append("nil");
        } else if (value instanceof Kw kw) {
            out.append(':').append(kw.name());
        } else if (value instanceof String s) {
            writeString(s, out);
        } else if (value instanceof Boolean || value instanceof Integer || value instanceof Long
                || value instanceof Short || value instanceof Byte) {
            out.append(value);
        } else if (value instanceof Enum<?> e) {
            out.append(':').append(e.name());
        } else if (value instanceof java.util.UUID uuid) {
            writeString(uuid.toString(), out);
        } else if (value instanceof Map<?, ?> map) {
            out.append('{');
            boolean first = true;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (!first) {
                    if (depth == 0) {
                        out.append('\n').append(' ');
                    } else {
                        out.append(", ");
                    }
                }
                first = false;
                write(entry.getKey(), out, depth + 1);
                out.append(' ');
                write(entry.getValue(), out, depth + 1);
            }
            out.append('}');
        } else if (value instanceof Iterable<?> items) {
            out.append('[');
            boolean first = true;
            for (Object item : items) {
                if (!first) {
                    if (depth <= 1) {
                        out.append('\n').append("  ");
                    } else {
                        out.append(' ');
                    }
                }
                first = false;
                write(item, out, depth + 1);
            }
            out.append(']');
        } else if (value instanceof Object[] array) {
            write(List.of(array), out, depth);
        } else {
            throw new IllegalArgumentException("cannot write " + value.getClass() + " as EDN");
        }
    }

    private static void writeString(String s, StringBuilder out) {
        out.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                case '\t' -> out.append("\\t");
                case '\r' -> out.append("\\r");
                default -> out.append(c);
            }
        }
        out.append('"');
    }

    /** Parses one EDN value; trailing whitespace and comments are permitted, nothing else. */
    static Object read(String text) {
        Reader reader = new Reader(text);
        Object value = reader.readValue();
        reader.skipWhitespace();
        if (!reader.atEnd()) {
            throw new IllegalArgumentException("trailing content at offset " + reader.pos);
        }
        return value;
    }

    private static final class Reader {
        private final String text;
        private int pos;

        Reader(String text) {
            this.text = text;
        }

        boolean atEnd() {
            return pos >= text.length();
        }

        void skipWhitespace() {
            while (!atEnd()) {
                char c = text.charAt(pos);
                if (c == ';') {
                    while (!atEnd() && text.charAt(pos) != '\n') {
                        pos++;
                    }
                } else if (Character.isWhitespace(c) || c == ',') {
                    pos++;
                } else {
                    return;
                }
            }
        }

        Object readValue() {
            skipWhitespace();
            if (atEnd()) {
                throw new IllegalArgumentException("unexpected end of EDN");
            }
            char c = text.charAt(pos);
            switch (c) {
                case '{':
                    pos++;
                    return readMap();
                case '[':
                    pos++;
                    return readSequence(']');
                case '(':
                    pos++;
                    return readSequence(')');
                case '#':
                    if (pos + 1 < text.length() && text.charAt(pos + 1) == '{') {
                        pos += 2;
                        return new java.util.LinkedHashSet<>(readSequence('}'));
                    }
                    throw new IllegalArgumentException("unsupported dispatch at offset " + pos);
                case '"':
                    pos++;
                    return readString();
                case ':':
                    pos++;
                    return new Kw(readToken());
                default:
                    String token = readToken();
                    return atom(token);
            }
        }

        private Object atom(String token) {
            switch (token) {
                case "nil":
                    return null;
                case "true":
                    return Boolean.TRUE;
                case "false":
                    return Boolean.FALSE;
                default:
                    break;
            }
            char first = token.charAt(0);
            if (Character.isDigit(first) || ((first == '-' || first == '+') && token.length() > 1)) {
                String digits = token.endsWith("N") ? token.substring(0, token.length() - 1) : token;
                try {
                    return Long.parseLong(digits);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("unsupported number " + token);
                }
            }
            return token; // a symbol, kept as its text
        }

        private String readToken() {
            int start = pos;
            while (!atEnd()) {
                char c = text.charAt(pos);
                if (Character.isWhitespace(c) || c == ',' || c == '}' || c == ']' || c == ')' || c == '{'
                        || c == '[' || c == '(' || c == '"' || c == ';') {
                    break;
                }
                pos++;
            }
            if (start == pos) {
                throw new IllegalArgumentException("unexpected character at offset " + pos);
            }
            return text.substring(start, pos);
        }

        private String readString() {
            StringBuilder out = new StringBuilder();
            while (true) {
                if (atEnd()) {
                    throw new IllegalArgumentException("unterminated string");
                }
                char c = text.charAt(pos++);
                if (c == '"') {
                    return out.toString();
                }
                if (c == '\\') {
                    char escaped = text.charAt(pos++);
                    switch (escaped) {
                        case 'n' -> out.append('\n');
                        case 't' -> out.append('\t');
                        case 'r' -> out.append('\r');
                        case '"' -> out.append('"');
                        case '\\' -> out.append('\\');
                        default -> throw new IllegalArgumentException("unsupported escape \\" + escaped);
                    }
                } else {
                    out.append(c);
                }
            }
        }

        private List<Object> readSequence(char close) {
            List<Object> items = new ArrayList<>();
            while (true) {
                skipWhitespace();
                if (atEnd()) {
                    throw new IllegalArgumentException("unterminated sequence");
                }
                if (text.charAt(pos) == close) {
                    pos++;
                    return items;
                }
                items.add(readValue());
            }
        }

        private Map<Object, Object> readMap() {
            Map<Object, Object> map = new LinkedHashMap<>();
            while (true) {
                skipWhitespace();
                if (atEnd()) {
                    throw new IllegalArgumentException("unterminated map");
                }
                if (text.charAt(pos) == '}') {
                    pos++;
                    return map;
                }
                Object key = readValue();
                Object value = readValue();
                map.put(key, value);
            }
        }
    }

    // Typed access over what read() returns.

    @SuppressWarnings("unchecked")
    static Map<Object, Object> asMap(Object value) {
        if (value == null) {
            return Map.of();
        }
        return (Map<Object, Object>) value;
    }

    @SuppressWarnings("unchecked")
    static List<Object> asList(Object value) {
        if (value == null) {
            return List.of();
        }
        return (List<Object>) value;
    }

    static Object get(Object map, String key) {
        return asMap(map).get(kw(key));
    }

    static String string(Object map, String key) {
        Object value = get(map, key);
        return value == null ? null : value.toString();
    }

    static long integer(Object map, String key) {
        Object value = get(map, key);
        if (value == null) {
            throw new IllegalArgumentException("missing :" + key);
        }
        return ((Number) value).longValue();
    }

    static Long optionalInteger(Object map, String key) {
        Object value = get(map, key);
        return value == null ? null : ((Number) value).longValue();
    }

    static String keywordName(Object value) {
        if (value == null) {
            return null;
        }
        return value instanceof Kw kw ? kw.name() : value.toString();
    }
}
