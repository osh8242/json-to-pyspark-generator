package com.douzone.platform.recipe;

import com.douzone.platform.recipe.exception.RecipeStepException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

class ValidationTest {

    @Test
    @DisplayName("Root recipe without steps should fail fast")
    void missingStepsArray() {
        assertThrows(RecipeStepException.class, () -> PySparkChainGenerator.generate("{}"));
    }

    @Test
    @DisplayName("Select step requires non-empty columns array")
    void selectWithoutColumns() {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"select\",\n"
                + "      \"params\": {}\n"
                + "    }\n"
                + "  ]\n"
                + "}";
        assertThrows(RecipeStepException.class, () -> PySparkChainGenerator.generate(json));
    }

    @Test
    @DisplayName("Load step without source is rejected")
    void loadWithoutSource() {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"load\",\n"
                + "      \"params\": {}\n"
                + "    }\n"
                + "  ]\n"
                + "}";
        assertThrows(RecipeStepException.class, () -> PySparkChainGenerator.generate(json));
    }
}
