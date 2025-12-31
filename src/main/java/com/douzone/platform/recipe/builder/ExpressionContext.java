package com.douzone.platform.recipe.builder;

/**
 * Simple context holder used while rendering expressions.
 * It currently tracks how literal values should be emitted
 * (as column expressions via F.lit or as raw Python literals).
 */
public class ExpressionContext {

    public enum LiteralMode {
        COLUMN,
        RAW
    }

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

    public LiteralMode getLiteralMode() {
        return literalMode;
    }

    public ExpressionContext withLiteralMode(LiteralMode mode) {
        if (mode == this.literalMode) return this;
        return new ExpressionContext(mode);
    }
}
