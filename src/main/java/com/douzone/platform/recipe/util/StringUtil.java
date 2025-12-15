package com.douzone.platform.recipe.util;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * description    :
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2025. 9. 17.        osh8242       최초 생성
 */
public class StringUtil {
    public static String getText(JsonNode n, String field, String defaultValue) {
        if (n == null) return defaultValue;
        JsonNode v = n.get(field);
        return v != null && !v.isNull() ? v.asText() : defaultValue;
    }

    public static boolean hasText(String text) {
        return text != null && !text.trim().isEmpty();
    }

    public static boolean isPyIdent(String s) {
        return s != null && s.matches("[A-Za-z_][A-Za-z0-9_]*");
    }

    public static String pyString(String s) {
        if (s == null) return "None";
        StringBuilder sb = new StringBuilder(s.length() + 10);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '\\':
                    sb.append("\\\\");
                    break;
                case '"':
                    sb.append("\\\"");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                case '\b':
                    sb.append("\\b");
                    break;
                case '\f':
                    sb.append("\\f");
                    break;
                default:
                    if (c < 0x20 || c == 0x7f) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
            }
        }
        return "\"" + sb + "\"";
    }

    public static String pyBool(boolean value) {
        return value ? "True" : "False";
    }

    public static String appendAliasIfExists(JsonNode e, String str) {
        if (e == null) {
            return str;
        }

        JsonNode aliasNode = null;
        if (e.hasNonNull("alias")) {
            aliasNode = e.get("alias");
        } else if (e.hasNonNull("as")) {
            aliasNode = e.get("as");
        }

        if (aliasNode != null) {
            String alias = aliasNode.asText();
            if (!alias.isEmpty()) {
                str += ".alias(" + pyString(alias) + ")";
            }
        }

        return str;
    }
}
