package com.douzone.platform.recipe.json;

import com.douzone.platform.recipe.PySparkChainGenerator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.douzone.platform.recipe.util.TestUtil.buildFullScript;
import static com.douzone.platform.recipe.util.TestUtil.printTestInfo;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * description    : toJSON() step 지원 테스트
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2025. 10. 27.     ChatGPT            최초 생성
 */
public class ToJsonStepTest {

    @Test
    @DisplayName("toJSON: 기본 체인 생성")
    void testToJsonStepGeneratesChain() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    { \"step\": \"toJSON\" }\n"
                + "  ]\n"
                + "}";

        String expected = buildFullScript("  .toJSON()\n");
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testToJsonStepGeneratesChain", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("toJSON: take 옵션을 사용하여 결과 제한")
    void testToJsonStepWithTakeOption() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"step\": \"filter\",\n"
                + "      \"condition\": {\n"
                + "        \"type\": \"op\",\n"
                + "        \"op\": \"=\",\n"
                + "        \"left\": { \"type\": \"col\", \"name\": \"status\" },\n"
                + "        \"right\": { \"type\": \"lit\", \"value\": \"ACTIVE\" }\n"
                + "      }\n"
                + "    },\n"
                + "    { \"step\": \"toJSON\", \"take\": 20 }\n"
                + "  ]\n"
                + "}";

        String expectedSteps = "  .filter((F.col(\"status\") == F.lit(\"ACTIVE\")))\n"
                + "  .toJSON()\n"
                + "  .take(20)\n";
        String expected = buildFullScript(expectedSteps);
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testToJsonStepWithTakeOption", json, actual);
        assertEquals(expected, actual);
    }
}
