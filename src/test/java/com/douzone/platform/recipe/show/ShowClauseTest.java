package com.douzone.platform.recipe.show;

import com.douzone.platform.recipe.PySparkChainGenerator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.douzone.platform.recipe.util.TestUtil.buildFullScript;
import static com.douzone.platform.recipe.util.TestUtil.printTestInfo;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * description    : show() step 테스트
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2025. 10. 24.     osh8242       최초 생성
 */
public class ShowClauseTest {

    @Test
    @DisplayName("Show: 기본 출력 (20행)")
    void testBasicShow() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    { \"step\": \"show\" }\n"
                + "  ]\n"
                + "}";

        String expected = buildFullScript("", "result_df.show(20)\n");
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testBasicShow", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Show: 행 수와 옵션 지정 (5행, 전체 출력, 세로 형식)")
    void testShowWithOptions() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    { \"step\": \"show\", \"n\": 5, \"truncate\": false, \"vertical\": true }\n"
                + "  ]\n"
                + "}";

        String expected = buildFullScript("", "result_df.show(5, truncate=False, vertical=True)\n");
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testShowWithOptions", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Show: truncate 길이 지정")
    void testShowWithTruncateLength() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    { \"step\": \"show\", \"truncate\": 50 }\n"
                + "  ]\n"
                + "}";

        String expected = buildFullScript("", "result_df.show(20, truncate=50)\n");
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testShowWithTruncateLength", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Show: 다단계 체인 끝에 show 적용")
    void testShowInMultiStepPipeline() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    { \"step\": \"filter\", \"condition\": { \"type\": \"op\", \"op\": \">\", \"left\": { \"type\": \"col\", \"name\": \"salary\" }, \"right\": { \"type\": \"lit\", \"value\": 50000 } } },\n"
                + "    { \"step\": \"groupBy\", \"keys\": [{ \"type\": \"col\", \"name\": \"department\" }] },\n"
                + "    { \"step\": \"agg\", \"aggs\": [{ \"expr\": { \"type\": \"func\", \"name\": \"avg\", \"args\": [{ \"type\": \"col\", \"name\": \"salary\" }] }, \"alias\": \"avg_salary\" }] },\n"
                + "    { \"step\": \"show\", \"n\": 10 }\n"
                + "  ]\n"
                + "}";

        String expectedSteps = "  .filter((F.col(\"salary\") > F.lit(50000)))\n"
                + "  .groupBy(F.col(\"department\"))\n"
                + "  .agg(\n"
                + "      F.avg(F.col(\"salary\")).alias(\"avg_salary\")\n"
                + "  )\n";
        String expected = buildFullScript(expectedSteps, "result_df.show(10)\n");
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testShowInMultiStepPipeline", json, actual);
        assertEquals(expected, actual);
    }
}
