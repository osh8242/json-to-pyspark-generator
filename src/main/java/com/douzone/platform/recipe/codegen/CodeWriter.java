// 파일: src/main/java/com/douzone/platform/recipe/codegen/CodeWriter.java
package com.douzone.platform.recipe.codegen;

import com.douzone.platform.recipe.exception.RecipeStepException;
import com.douzone.platform.recipe.util.StringUtil;

/**
 * 문자열 조립 규칙(개행, persist 삽입 등)을 중앙화.
 *
 * 정책:
 * - DF assignment / DF chain assignment 에서만 persist를 허용
 * - statement(action)에는 persist를 절대 삽입하지 않음
 */
public class CodeWriter {

    private final StringBuilder out = new StringBuilder();

    public void appendDfAssignment(String outputDf, String expr, boolean persist) {
        if (!StringUtil.hasText(outputDf)) {
            throw new RecipeStepException("outputDf must not be empty.");
        }
        String exprCore = stripTrailingNewlines(expr).trim();
        String core = outputDf + " = " + exprCore;
        core = persistIfNeeded(core, persist);
        out.append(core).append("\n");
    }

    public void appendDfChainAssignment(String outputDf, String inputDf, String chainFragment, boolean persist) {
        if (!StringUtil.hasText(outputDf)) {
            throw new RecipeStepException("outputDf must not be empty.");
        }
        if (!StringUtil.hasText(inputDf)) {
            throw new RecipeStepException("inputDf must not be empty.");
        }
        String frag = chainFragment == null ? "" : chainFragment;
        String core = stripTrailingNewlines((outputDf + " = " + inputDf + frag)).trim();
        core = persistIfNeeded(core, persist);
        out.append(core).append("\n");
    }

    public void appendStatement(String stmt) {
        if (stmt == null || stmt.isEmpty()) return;
        out.append(ensureTrailingNewline(stmt));
    }

    @Override
    public String toString() {
        return out.toString();
    }

    private static String persistIfNeeded(String codeCore, boolean persist) {
        if (!persist) return codeCore;
        String trimmed = codeCore.trim();
        if (trimmed.endsWith(".persist()")) return codeCore;
        return codeCore + ".persist()";
    }

    private static String ensureTrailingNewline(String s) {
        if (s.endsWith("\n")) return s;
        if (s.endsWith("\r\n")) return s;
        return s + "\n";
    }

    public static String stripTrailingNewlines(String code) {
        if (code == null) return "";
        int cut = code.length();
        while (cut > 0) {
            char c = code.charAt(cut - 1);
            if (c == '\n' || c == '\r') cut--;
            else break;
        }
        return code.substring(0, cut);
    }
}
