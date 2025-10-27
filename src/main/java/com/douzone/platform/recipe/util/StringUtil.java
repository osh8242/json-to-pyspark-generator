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
    public static String getText(JsonNode n, String field, String def) {
        if (n == null) return def;
        JsonNode v = n.get(field);
        return v != null && !v.isNull() ? v.asText() : def;
    }

    public static String pyString(String s) {
        if (s == null) return "None";
        String esc = s.replace("\\", "\\\\").replace("\"", "\\\"");
        return "\"" + esc + "\"";
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
