package com.douzone.platform.recipe.sample;

import com.douzone.platform.recipe.PySparkChainGenerator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.douzone.platform.recipe.util.TestUtil.buildFullScript;
import static com.douzone.platform.recipe.util.TestUtil.printTestInfo;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SampleClauseTest {

    @Test
    @DisplayName("Sample: withReplacement True 및 숫자 시드")
    void testSampleWithReplacementAndNumericSeed() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    { \"step\": \"sample\", \"withReplacement\": true, \"fraction\": 0.1, \"seed\": 42 }\n"
                + "  ]\n"
                + "}";

        String expectedSteps = "  .sample(True, 0.1, 42)\n";
        String expected = buildFullScript(expectedSteps);
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testSampleWithReplacementAndNumericSeed", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Sample: 문자열 시드")
    void testSampleWithStringSeed() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    { \"step\": \"sample\", \"fraction\": 0.25, \"seed\": \"abc123\" }\n"
                + "  ]\n"
                + "}";

        String expectedSteps = "  .sample(False, 0.25, \"abc123\")\n";
        String expected = buildFullScript(expectedSteps);
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testSampleWithStringSeed", json, actual);
        assertEquals(expected, actual);
    }
}
