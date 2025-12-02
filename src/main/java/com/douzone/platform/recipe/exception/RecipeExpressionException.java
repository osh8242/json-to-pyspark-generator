package com.douzone.platform.recipe.exception;

public class RecipeExpressionException extends RuntimeException {

    public RecipeExpressionException(String message) {
        super(message);
    }

    public RecipeExpressionException(String message, Throwable cause) {
        super(message, cause);
    }
}
