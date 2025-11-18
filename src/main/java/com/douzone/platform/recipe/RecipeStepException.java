package com.douzone.platform.recipe;

/**
 * Runtime exception thrown when a recipe step definition is invalid or
 * missing required attributes. The generator intentionally fails fast so that
 * callers can surface actionable validation errors to their users instead of
 * emitting malformed PySpark code.
 */
public class RecipeStepException extends RuntimeException {

    public RecipeStepException(String message) {
        super(message);
    }

    public RecipeStepException(String message, Throwable cause) {
        super(message, cause);
    }
}
