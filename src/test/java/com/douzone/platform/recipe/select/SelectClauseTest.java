package com.douzone.platform.recipe.select;

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
public class SelectClauseTest {

    @Test
    @DisplayName("Select with 2 Columns: 기본적인 두개 컬럼 선택")
    void testSimpleSelect() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"step\": \"select\",\n"
                + "      \"columns\": [\n"
                + "        { \"expr\": { \"type\": \"col\", \"name\": \"name\" } },\n"
                + "        { \"expr\": { \"type\": \"col\", \"name\": \"age\" } }\n"
                + "      ]\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expectedStep = "  .select(F.col(\"name\"), F.col(\"age\"))\n";
        String expected = buildFullScript(expectedStep);
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testSimpleSelect", json, actual);

        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Select with Alias: 컬럼에 별칭(alias)을 부여하여 선택")
    void testSelectWithAlias() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"step\": \"select\",\n"
                + "      \"columns\": [\n"
                + "        { \"expr\": { \"type\": \"col\", \"name\": \"user_id\" }, \"alias\": \"id\" },\n"
                + "        { \"expr\": { \"type\": \"col\", \"name\": \"first_name\" } }\n"
                + "      ]\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expectedStep = "  .select(F.col(\"user_id\").alias(\"id\"), F.col(\"first_name\"))\n";
        String expected = buildFullScript(expectedStep);
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testSelectWithAlias", json, actual);

        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Select with Function: 함수(F.upper)를 사용하고 별칭 부여")
    void testSelectWithFunctionAndAlias() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"step\": \"select\",\n"
                + "      \"columns\": [\n"
                + "        { \"expr\": { \"type\": \"col\", \"name\": \"id\" } },\n"
                + "        {\n"
                + "          \"expr\": {\n"
                + "            \"type\": \"func\",\n"
                + "            \"name\": \"upper\",\n"
                + "            \"args\": [ { \"type\": \"col\", \"name\": \"name\" } ]\n"
                + "          },\n"
                + "          \"alias\": \"upper_name\"\n"
                + "        }\n"
                + "      ]\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expectedStep = "  .select(F.col(\"id\"), F.upper(F.col(\"name\")).alias(\"upper_name\"))\n";
        String expected = buildFullScript(expectedStep);
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testSelectWithFunctionAndAlias", json, actual);

        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Select with Arithmetic Operation: 산술 연산(*) 결과를 선택하고 별칭 부여")
    void testSelectWithArithmeticOperation() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"step\": \"select\",\n"
                + "      \"columns\": [\n"
                + "        { \"expr\": { \"type\": \"col\", \"name\": \"product_name\" } },\n"
                + "        {\n"
                + "          \"expr\": {\n"
                + "            \"type\": \"op\",\n"
                + "            \"op\": \"*\",\n"
                + "            \"left\": { \"type\": \"col\", \"name\": \"price\" },\n"
                + "            \"right\": { \"type\": \"lit\", \"value\": 1.1 }\n"
                + "          },\n"
                + "          \"alias\": \"price_with_tax\"\n"
                + "        }\n"
                + "      ]\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expectedStep = "  .select(F.col(\"product_name\"), (F.col(\"price\") * F.lit(1.1)).alias(\"price_with_tax\"))\n";
        String expected = buildFullScript(expectedStep);
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testSelectWithArithmeticOperation", json, actual);

        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Select with Case When: CASE WHEN 표현식을 사용하여 새로운 컬럼 생성")
    void testSelectWithCaseWhen() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"step\": \"select\",\n"
                + "      \"columns\": [\n"
                + "        { \"expr\": { \"type\": \"col\", \"name\": \"id\" } },\n"
                + "        {\n"
                + "          \"expr\": {\n"
                + "            \"type\": \"case\",\n"
                + "            \"when\": [\n"
                + "              {\n"
                + "                \"if\": {\n"
                + "                  \"type\": \"op\",\n"
                + "                  \"op\": \">=\",\n"
                + "                  \"left\": { \"type\": \"col\", \"name\": \"age\" },\n"
                + "                  \"right\": { \"type\": \"lit\", \"value\": 20 }\n"
                + "                },\n"
                + "                \"then\": { \"type\": \"lit\", \"value\": \"Adult\" }\n"
                + "              }\n"
                + "            ],\n"
                + "            \"else\": { \"type\": \"lit\", \"value\": \"Minor\" }\n"
                + "          },\n"
                + "          \"alias\": \"age_group\"\n"
                + "        }\n"
                + "      ]\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expectedStep = "  .select(F.col(\"id\"), (F.when((F.col(\"age\") >= F.lit(20)), F.lit(\"Adult\"))).otherwise(F.lit(\"Minor\")).alias(\"age_group\"))\n";
        String expected = buildFullScript(expectedStep);
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testSelectWithCaseWhen", json, actual);

        assertEquals(expected, actual);
    }
}
