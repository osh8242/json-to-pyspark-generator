package com.douzone.platform.recipe.json;

import com.douzone.platform.recipe.PySparkChainGenerator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

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
    @DisplayName("toJSON: 단일 스텝 변환")
    void testToJsonStepGeneratesChain() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"toJSON\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df_json\"\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected = "df_json = df.toJSON()\n";
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testToJsonStepGeneratesChain", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("toJSON: 단일 스텝 + take 옵션")
    void testToJsonStepGeneratesChainWithTake() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"toJSON\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df_json\",\n"
                + "      \"params\": {\n"
                + "        \"take\": 50\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        // buildToJson 이 ".toJSON().take(50)\\n" 형태를 반환한다는 가정
        String expected = "df_json = df.toJSON().take(50)\n";
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testToJsonStepGeneratesChainWithTake", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("toJSON: filter 이후 toJSON + take")
    void testToJsonStepWithTakeOption() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"filter\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df_filtered\",\n"
                + "      \"params\": {\n"
                + "        \"condition\": {\n"
                + "          \"type\": \"op\",\n"
                + "          \"op\": \"=\",\n"
                + "          \"left\": { \"type\": \"col\", \"name\": \"status\" },\n"
                + "          \"right\": { \"type\": \"lit\", \"value\": \"ACTIVE\" }\n"
                + "        }\n"
                + "      }\n"
                + "    },\n"
                + "    {\n"
                + "      \"node\": \"toJSON\",\n"
                + "      \"input\": \"df_filtered\",\n"
                + "      \"output\": \"df_json\",\n"
                + "      \"params\": {\n"
                + "        \"take\": 20\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected =
                "df_filtered = df.filter((F.col(\"status\") == F.lit(\"ACTIVE\")))\n" +
                        "df_json = df_filtered.toJSON().take(20)\n";

        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testToJsonStepWithTakeOption", json, actual);
        assertEquals(expected, actual);
    }
}
