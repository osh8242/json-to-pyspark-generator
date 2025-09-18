package com.douzone.platform.recipe.filter;

import com.douzone.platform.recipe.PySparkChainGenerator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.douzone.platform.recipe.util.TestUtil.buildFullScript;
import static com.douzone.platform.recipe.util.TestUtil.printTestInfo;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * description    :
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2025. 9. 17.        osh8242       최초 생성
 */
public class FilterClauseTest {
    @Test
    @DisplayName("Filter: 단일 조건(>=) 테스트")
    void testFilterWithSingleCondition() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"step\": \"filter\",\n"
                + "      \"condition\": {\n"
                + "        \"type\": \"op\",\n"
                + "        \"op\": \">=\",\n"
                + "        \"left\": { \"type\": \"col\", \"name\": \"age\" },\n"
                + "        \"right\": { \"type\": \"lit\", \"value\": 20 }\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expectedStep = "  .filter((F.col(\"age\") >= F.lit(20)))\n";
        String expected = buildFullScript(expectedStep);
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testFilterWithSingleCondition", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Filter: AND 연산자로 두 조건 결합")
    void testFilterWithAnd() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"step\": \"filter\",\n"
                + "      \"condition\": {\n"
                + "        \"type\": \"op\", \"op\": \"and\",\n"
                + "        \"left\": { \"type\": \"op\", \"op\": \">\", \"left\": { \"type\": \"col\", \"name\": \"age\" }, \"right\": { \"type\": \"lit\", \"value\": 30 } },\n"
                + "        \"right\": { \"type\": \"op\", \"op\": \"=\", \"left\": { \"type\": \"col\", \"name\": \"gender\" }, \"right\": { \"type\": \"lit\", \"value\": \"M\" } }\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expectedStep = "  .filter(((F.col(\"age\") > F.lit(30)) & (F.col(\"gender\") == F.lit(\"M\"))))\n";
        String expected = buildFullScript(expectedStep);
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testFilterWithAnd", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Filter: OR 연산자와 괄호를 포함한 복합 조건")
    void testFilterWithOrAndParentheses() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"step\": \"filter\",\n"
                + "      \"condition\": {\n"
                + "        \"type\": \"op\", \"op\": \"and\",\n"
                + "        \"left\": { \"type\": \"op\", \"op\": \"=\", \"left\": { \"type\": \"col\", \"name\": \"status\" }, \"right\": { \"type\": \"lit\", \"value\": \"active\" } },\n"
                + "        \"right\": {\n"
                + "          \"type\": \"op\", \"op\": \"or\",\n"
                + "          \"left\": { \"type\": \"op\", \"op\": \"=\", \"left\": { \"type\": \"col\", \"name\": \"country\" }, \"right\": { \"type\": \"lit\", \"value\": \"USA\" } },\n"
                + "          \"right\": { \"type\": \"op\", \"op\": \"=\", \"left\": { \"type\": \"col\", \"name\": \"country\" }, \"right\": { \"type\": \"lit\", \"value\": \"CAN\" } }\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expectedStep = "  .filter(((F.col(\"status\") == F.lit(\"active\")) & ((F.col(\"country\") == F.lit(\"USA\")) | (F.col(\"country\") == F.lit(\"CAN\")))))";
        // 참고: PySpark의 `&`와 `|` 연산자 우선순위 때문에 외부 괄호가 없어도 되지만, 빌더가 안전하게 괄호를 생성하는 것을 테스트합니다.
        String expected = buildFullScript(expectedStep + "\n");
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testFilterWithOrAndParentheses", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Filter: BETWEEN 연산자 테스트")
    void testFilterWithBetween() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"step\": \"filter\",\n"
                + "      \"condition\": {\n"
                + "        \"type\": \"between\",\n"
                + "        \"expr\": { \"type\": \"col\", \"name\": \"age\" },\n"
                + "        \"low\": { \"type\": \"lit\", \"value\": 20 },\n"
                + "        \"high\": { \"type\": \"lit\", \"value\": 29 }\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expectedStep = "  .filter((F.col(\"age\")).between(F.lit(20), F.lit(29)))\n";
        String expected = buildFullScript(expectedStep);
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testFilterWithBetween", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Filter: NOT BETWEEN 연산자 테스트")
    void testFilterWithNotBetween() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"step\": \"filter\",\n"
                + "      \"condition\": {\n"
                + "        \"type\": \"between\",\n"
                + "        \"not\": true,\n"
                + "        \"expr\": { \"type\": \"col\", \"name\": \"age\" },\n"
                + "        \"low\": { \"type\": \"lit\", \"value\": 20 },\n"
                + "        \"high\": { \"type\": \"lit\", \"value\": 29 }\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expectedStep = "  .filter(~((F.col(\"age\")).between(F.lit(20), F.lit(29))))\n";
        String expected = buildFullScript(expectedStep);
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testFilterWithNotBetween", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Filter: isNull 함수 테스트")
    void testFilterWithFuncIsnull() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"step\": \"where\",\n"
                + "      \"condition\": {\n"
                + "        \"type\": \"func\",\n"
                + "        \"name\": \"isnull\",\n"
                + "        \"args\": [ { \"type\": \"col\", \"name\": \"email\" } ]\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";
        // 이 테스트는 F.isnull(col) 형태의 생성 로직을 검증합니다 (이 역시 유효한 PySpark 코드).
        String expectedStep = "  .filter(F.isnull(F.col(\"email\")))\n";
        String expected = buildFullScript(expectedStep);
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testFilterWithIsNull", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Filter: NOT 연산자 테스트")
    void testFilterWithNot() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"step\": \"filter\",\n"
                + "      \"condition\": {\n"
                + "        \"type\": \"op\",\n"
                + "        \"op\": \"not\",\n"
                + "        \"expr\": {\n"
                + "          \"type\": \"op\",\n"
                + "          \"op\": \"=\",\n"
                + "          \"left\": { \"type\": \"col\", \"name\": \"status\" },\n"
                + "          \"right\": { \"type\": \"lit\", \"value\": \"ARCHIVED\" }\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expectedStep = "  .filter((~((F.col(\"status\") == F.lit(\"ARCHIVED\")))))\n";
        String expected = buildFullScript(expectedStep);
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testFilterWithNot", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Filter: CASE WHEN 결과를 조건으로 사용")
    void testFilterWithCaseWhen() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"step\": \"filter\",\n"
                + "      \"condition\": {\n"
                + "        \"type\": \"op\",\n"
                + "        \"op\": \"=\",\n"
                + "        \"left\": {\n"
                + "          \"type\": \"case\",\n"
                + "          \"when\": [\n"
                + "            {\n"
                + "              \"if\": { \"type\": \"op\", \"op\": \">\", \"left\": { \"type\": \"col\", \"name\": \"score\" }, \"right\": { \"type\": \"lit\", \"value\": 90 } },\n"
                + "              \"then\": { \"type\": \"lit\", \"value\": \"PASS\" }\n"
                + "            }\n"
                + "          ],\n"
                + "          \"else\": { \"type\": \"lit\", \"value\": \"FAIL\" }\n"
                + "        },\n"
                + "        \"right\": { \"type\": \"lit\", \"value\": \"PASS\" }\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expectedStep = "  .filter(((F.when((F.col(\"score\") > F.lit(90)), F.lit(\"PASS\"))).otherwise(F.lit(\"FAIL\")) == F.lit(\"PASS\")))\n";
        String expected = buildFullScript(expectedStep);
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testFilterWithCaseWhen", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Filter: 컬럼에 isNull 테스트")
    void testFilterWithColumnIsNull() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"step\": \"where\",\n"
                + "      \"condition\": {\n"
                + "        \"type\": \"isNull\",\n"
                + "        \"expr\": { \"type\": \"col\", \"name\": \"email\" }\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expectedStep = "  .filter((F.col(\"email\")).isNull())\n";
        String expected = buildFullScript(expectedStep);
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testFilterWithIsNull", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Filter: isNotNull 테스트")
    void testFilterWithIsNotNull() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"step\": \"where\",\n"
                + "      \"condition\": {\n"
                + "        \"type\": \"isNotNull\",\n"
                + "        \"expr\": { \"type\": \"col\", \"name\": \"last_login\" }\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expectedStep = "  .filter((F.col(\"last_login\")).isNotNull())\n";
        String expected = buildFullScript(expectedStep);
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testFilterWithIsNotNull", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Filter: isin 테스트")
    void testFilterWithIsin() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"step\": \"filter\",\n"
                + "      \"condition\": {\n"
                + "        \"type\": \"isin\",\n"
                + "        \"expr\": { \"type\": \"col\", \"name\": \"country\" },\n"
                + "        \"values\": [\n"
                + "          { \"type\": \"lit\", \"value\": \"USA\" },\n"
                + "          { \"type\": \"lit\", \"value\": \"CAN\" },\n"
                + "          { \"type\": \"lit\", \"value\": \"MEX\" }\n"
                + "        ]\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expectedStep = "  .filter((F.col(\"country\")).isin(F.lit(\"USA\"), F.lit(\"CAN\"), F.lit(\"MEX\")))\n";
        String expected = buildFullScript(expectedStep);
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testFilterWithIsin", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Filter: NOT isin 테스트")
    void testFilterWithNotIsin() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"step\": \"filter\",\n"
                + "      \"condition\": {\n"
                + "        \"type\": \"isin\",\n"
                + "        \"not\": true,\n"
                + "        \"expr\": { \"type\": \"col\", \"name\": \"category\" },\n"
                + "        \"values\": [\n"
                + "          { \"type\": \"lit\", \"value\": \"A\" },\n"
                + "          { \"type\": \"lit\", \"value\": \"B\" }\n"
                + "        ]\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expectedStep = "  .filter(~((F.col(\"category\")).isin(F.lit(\"A\"), F.lit(\"B\"))))\n";
        String expected = buildFullScript(expectedStep);
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testFilterWithNotIsin", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Filter: like 테스트")
    void testFilterWithLike() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"step\": \"filter\",\n"
                + "      \"condition\": {\n"
                + "        \"type\": \"like\",\n"
                + "        \"expr\": { \"type\": \"col\", \"name\": \"name\" },\n"
                + "        \"pattern\": \"J%\"\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expectedStep = "  .filter((F.col(\"name\")).like(\"J%\"))\n";
        String expected = buildFullScript(expectedStep);
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testFilterWithLike", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Filter: NOT like 테스트")
    void testFilterWithNotLike() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"step\": \"filter\",\n"
                + "      \"condition\": {\n"
                + "        \"type\": \"like\",\n"
                + "        \"not\": true,\n"
                + "        \"expr\": { \"type\": \"col\", \"name\": \"product_code\" },\n"
                + "        \"pattern\": \"ERR-%\"\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expectedStep = "  .filter(~((F.col(\"product_code\")).like(\"ERR-%\")))\n";
        String expected = buildFullScript(expectedStep);
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testFilterWithNotLike", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Filter: like에서 리터럴 '%' 문자 검색")
    void testFilterWithLikeLiteralPercent() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"step\": \"filter\",\n"
                + "      \"condition\": {\n"
                + "        \"type\": \"like\",\n"
                + "        \"expr\": { \"type\": \"col\", \"name\": \"discount_rate\" },\n"
                + "        \"pattern\": \"10\\\\%\"\n" // JSON에서 \\% 로 표현
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        // 생성될 Python 코드에서는 .like("10\\%") 형태가 되어야 합니다.
        String expectedStep = "  .filter((F.col(\"discount_rate\")).like(\"10\\\\%\"))\n";
        String expected = buildFullScript(expectedStep);
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testFilterWithLikeLiteralPercent", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Filter: like에서 리터럴 '_' 문자 검색")
    void testFilterWithLikeLiteralUnderscore() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"step\": \"filter\",\n"
                + "      \"condition\": {\n"
                + "        \"type\": \"like\",\n"
                + "        \"expr\": { \"type\": \"col\", \"name\": \"file_name\" },\n"
                + "        \"pattern\": \"%\\\\_backup%\"\n" // JSON에서 %\\_backup% 로 표현
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        // 생성될 Python 코드에서는 .like("%\\_backup%") 형태가 되어야 합니다.
        String expectedStep = "  .filter((F.col(\"file_name\")).like(\"%\\\\_backup%\"))\n";
        String expected = buildFullScript(expectedStep);
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testFilterWithLikeLiteralUnderscore", json, actual);
        assertEquals(expected, actual);
    }


}
