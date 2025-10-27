package com.douzone.platform.recipe.action;

import com.douzone.platform.recipe.PySparkChainGenerator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.douzone.platform.recipe.util.TestUtil.buildFullScript;
import static com.douzone.platform.recipe.util.TestUtil.printTestInfo;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * description    : 파라미터가 없는 DataFrame 액션(step) 테스트
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2025. 10. 24.     osh8242       최초 생성
 */
public class NoArgActionTest {

    @Test
    @DisplayName("No-arg Action: count() 호출")
    void testCountAction() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    { \"step\": \"count\" }\n"
                + "  ]\n"
                + "}";

        String expected = buildFullScript("", "result_df.count()\n");
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testCountAction", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("No-arg Action: 변환 + 다중 액션 순서 보존")
    void testActionOrderPreserved() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    { \"step\": \"filter\", \"condition\": { \"type\": \"op\", \"op\": \"=\", \"left\": { \"type\": \"col\", \"name\": \"status\" }, \"right\": { \"type\": \"lit\", \"value\": \"ACTIVE\" } } },\n"
                + "    { \"step\": \"count\" },\n"
                + "    { \"step\": \"show\", \"n\": 5 },\n"
                + "    { \"step\": \"collect\" }\n"
                + "  ]\n"
                + "}";

        String expectedStep = "  .filter((F.col(\"status\") == F.lit(\"ACTIVE\")))\n";
        String expected = buildFullScript(expectedStep,
                "result_df.count()\n",
                "result_df.show(5)\n",
                "result_df.collect()\n");

        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testActionOrderPreserved", json, actual);
        assertEquals(expected, actual);
    }
}
