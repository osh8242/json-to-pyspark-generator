package com.douzone.platform.recipe.groupby;

import com.douzone.platform.recipe.PySparkChainGenerator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.douzone.platform.recipe.util.TestUtil.printTestInfo;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * description    :
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2025. 9. 17.        osh8242       최초 생성
 */
public class GroupbyClauseTest {

    @Test
    @DisplayName("GroupBy: 단일 컬럼으로 그룹화")
    void testGroupBySingleColumn() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"groupBy\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df_grouped\",\n"
                + "      \"params\": {\n"
                + "        \"keys\": [\n"
                + "          { \"type\": \"col\", \"name\": \"department\" }\n"
                + "        ]\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected = ""
                + "df_grouped = df.groupBy(F.col(\"department\"))\n";

        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testGroupBySingleColumn", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("GroupBy: 여러 컬럼으로 그룹화")
    void testGroupByMultipleColumns() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"groupBy\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df_grouped\",\n"
                + "      \"params\": {\n"
                + "        \"keys\": [\n"
                + "          { \"type\": \"col\", \"name\": \"department\" },\n"
                + "          { \"type\": \"col\", \"name\": \"gender\" }\n"
                + "        ]\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected = ""
                + "df_grouped = df.groupBy(F.col(\"department\"), F.col(\"gender\"))\n";

        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testGroupByMultipleColumns", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("GroupBy: 함수 결과를 기준으로 그룹화")
    void testGroupByWithFunction() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"groupBy\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df_grouped\",\n"
                + "      \"params\": {\n"
                + "        \"keys\": [\n"
                + "          {\n"
                + "            \"type\": \"func\",\n"
                + "            \"name\": \"year\",\n"
                + "            \"args\": [ { \"type\": \"col\", \"name\": \"hire_date\" } ]\n"
                + "          }\n"
                + "        ]\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected = ""
                + "df_grouped = df.groupBy(F.year(F.col(\"hire_date\")))\n";

        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testGroupByWithFunction", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("GroupBy: 복합 표현식(연산 및 캐스팅)으로 그룹화 - 연령대별 그룹화")
    void testGroupByWithComplexExpression() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"groupBy\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df_grouped\",\n"
                + "      \"params\": {\n"
                + "        \"keys\": [\n"
                + "          {\n"
                + "            \"type\": \"op\",\n"
                + "            \"op\": \"*\",\n"
                + "            \"left\": {\n"
                + "              \"type\": \"cast\",\n"
                + "              \"to\": \"integer\",\n"
                + "              \"expr\": {\n"
                + "                \"type\": \"op\",\n"
                + "                \"op\": \"/\",\n"
                + "                \"left\": { \"type\": \"col\", \"name\": \"age\" },\n"
                + "                \"right\": { \"type\": \"lit\", \"value\": 10 }\n"
                + "              }\n"
                + "            },\n"
                + "            \"right\": { \"type\": \"lit\", \"value\": 10 }\n"
                + "          }\n"
                + "        ]\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected = ""
                + "df_grouped = df.groupBy((((F.col(\"age\") / F.lit(10))).cast(\"integer\") * F.lit(10)))\n";

        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testGroupByWithComplexExpression", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("GroupBy: 전체 집계를 위한 빈 그룹화")
    void testGroupByForGlobalAggregation() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"groupBy\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df_grouped\",\n"
                + "      \"params\": {\n"
                + "        \"keys\": []\n"
                + "      }\n"
                + "    },\n"
                + "    {\n"
                + "      \"node\": \"agg\",\n"
                + "      \"input\": \"df_grouped\",\n"
                + "      \"output\": \"df_aggregated\",\n"
                + "      \"params\": {\n"
                + "        \"aggs\": [\n"
                + "          { \"expr\": { \"type\": \"func\", \"name\": \"count\", \"args\": [ { \"type\": \"lit\", \"value\": 1 } ] }, \"alias\": \"total_count\" }\n"
                + "        ]\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected = ""
                + "df_grouped = df.groupBy()\n"
                + "df_aggregated = df_grouped.agg(\n"
                + "      F.count(F.lit(1)).alias(\"total_count\")\n"
                + "  )\n";

        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testGroupByForGlobalAggregation", json, actual);
        assertEquals(expected, actual);
    }

}
