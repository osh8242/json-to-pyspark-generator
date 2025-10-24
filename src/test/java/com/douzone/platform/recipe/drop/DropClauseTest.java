package com.douzone.platform.recipe.drop;

import com.douzone.platform.recipe.PySparkChainGenerator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.douzone.platform.recipe.util.TestUtil.buildFullScript;
import static com.douzone.platform.recipe.util.TestUtil.printTestInfo;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * description    : drop() step 테스트
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2025. 10. 24.     osh8242       최초 생성
 */
public class DropClauseTest {

    @Test
    @DisplayName("Drop: 단일 컬럼 제거")
    void testSingleColumnDrop() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    { \"step\": \"drop\", \"cols\": [\"temp_col\"] }\n"
                + "  ]\n"
                + "}";

        String expectedStep = "  .drop(\"temp_col\")\n";
        String expected = buildFullScript(expectedStep);
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testSingleColumnDrop", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Drop: 여러 컬럼 제거")
    void testMultipleColumnsDrop() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    { \"step\": \"drop\", \"cols\": [\"id\", \"timestamp\", \"unused_flag\"] }\n"
                + "  ]\n"
                + "}";

        String expectedStep = "  .drop(\"id\", \"timestamp\", \"unused_flag\")\n";
        String expected = buildFullScript(expectedStep);
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testMultipleColumnsDrop", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Drop: 빈 배열 (빈 drop 호출)")
    void testEmptyColumnsDrop() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    { \"step\": \"drop\", \"cols\": [] }\n"
                + "  ]\n"
                + "}";

        String expectedStep = "  .drop()\n";
        String expected = buildFullScript(expectedStep);
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testEmptyColumnsDrop", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Drop: 다단계 체인에 포함 (filter 후 drop)")
    void testDropInMultiStepPipeline() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    { \"step\": \"filter\", \"condition\": { \"type\": \"op\", \"op\": \">\", \"left\": { \"type\": \"col\", \"name\": \"age\" }, \"right\": { \"type\": \"lit\", \"value\": 25 } } },\n"
                + "    { \"step\": \"drop\", \"cols\": [\"birth_date\", \"ssn\"] },\n"
                + "    { \"step\": \"limit\", \"n\": 100 }\n"
                + "  ]\n"
                + "}";

        String expectedSteps = "  .filter((F.col(\"age\") > F.lit(25)))\n"
                + "  .drop(\"birth_date\", \"ssn\")\n"
                + "  .limit(100)\n";
        String expected = buildFullScript(expectedSteps);
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testDropInMultiStepPipeline", json, actual);
        assertEquals(expected, actual);
    }
}
