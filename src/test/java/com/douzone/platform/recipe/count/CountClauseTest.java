package com.douzone.platform.recipe.count;

import com.douzone.platform.recipe.PySparkChainGenerator;
import com.douzone.platform.recipe.exception.RecipeStepException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.douzone.platform.recipe.util.TestUtil.printTestInfo;
import static org.junit.jupiter.api.Assertions.*;

public class CountClauseTest {

    @Test
    @DisplayName("Count: 기본(count) 테스트 (print=false 기본)")
    void testCountBasicNoPrint() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"count\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"row_count\"\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected = "row_count = df.count()\n";
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testCountBasicNoPrint", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Count: print=true 옵션 테스트")
    void testCountWithPrintTrue() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"count\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"row_count\",\n"
                + "      \"params\": {\n"
                + "        \"print\": true\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected = "row_count = df.count()\n"
                + "print(row_count)\n";
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testCountWithPrintTrue", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Count: input 변경 테스트")
    void testCountWithCustomInput() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"count\",\n"
                + "      \"input\": \"df_filtered\",\n"
                + "      \"output\": \"cnt\"\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected = "cnt = df_filtered.count()\n";
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testCountWithCustomInput", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Count: output trim 처리 테스트")
    void testCountOutputTrim() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"count\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"  row_count  \"\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected = "row_count = df.count()\n";
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testCountOutputTrim", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Count: output 누락 시 예외")
    void testCountMissingOutputThrows() {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"count\",\n"
                + "      \"input\": \"df\"\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        RecipeStepException ex = assertThrows(RecipeStepException.class, () -> PySparkChainGenerator.generate(json));
        // 메시지까지 검증하고 싶으면 아래 주석 해제
         assertTrue(ex.getMessage().contains("Step requires non-empty 'output'"));
    }

    @Test
    @DisplayName("Count: output이 Python 식별자가 아니면 예외")
    void testCountInvalidOutputIdentifierThrows() {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"count\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"row-count\"\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        RecipeStepException ex = assertThrows(RecipeStepException.class, () -> PySparkChainGenerator.generate(json));
         assertTrue(ex.getMessage().contains("valid Python identifier"));
    }

    @Test
    @DisplayName("Count: output이 input과 같으면(DF 덮어쓰기) 예외")
    void testCountOutputEqualsInputThrows() {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"count\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df\"\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        RecipeStepException ex = assertThrows(RecipeStepException.class, () -> PySparkChainGenerator.generate(json));
         assertTrue(ex.getMessage().contains("must be different from 'input'"));
    }
}
