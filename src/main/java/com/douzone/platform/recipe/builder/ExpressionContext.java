package com.douzone.platform.recipe.builder;

import lombok.Getter;

/**
 * Simple context holder used while rendering expressions.
 * It currently tracks how literal values should be emitted
 * (as column expressions via F.lit or as raw Python literals).
 */
@Getter
public class ExpressionContext {

    private final LiteralMode literalMode;

    private ExpressionContext(LiteralMode literalMode) {
        this.literalMode = literalMode;
    }

    public static ExpressionContext columnContext() {
        return new ExpressionContext(LiteralMode.COLUMN);
    }

    public static ExpressionContext rawContext() {
        return new ExpressionContext(LiteralMode.RAW);
    }

    public ExpressionContext withLiteralMode(LiteralMode mode) {
        if (mode == this.literalMode) return this;
        return new ExpressionContext(mode);
    }

    public enum LiteralMode {
        COLUMN,
        RAW,
        COLUMN_COERCE_NUMERIC  // F.lit(...)은 유지하되 "2" 같은 문자열을 숫자로 강제
    }
}
