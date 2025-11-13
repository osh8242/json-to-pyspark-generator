package com.douzone.platform.recipe.aggregation;

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
 * 2025. 9. 18.        osh8242       최초 생성
 */
public class AggregationClauseTest {

    @Test
    @DisplayName("Agg: 기본 집계 함수(sum, avg)와 별칭(alias) 사용")
    void testBasicAggregationsWithAlias() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"groupBy\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df_grouped\",\n"
                + "      \"params\": {\n"
                + "        \"keys\": [{ \"type\": \"col\", \"name\": \"department\" }]\n"
                + "      }\n"
                + "    },\n"
                + "    {\n"
                + "      \"node\": \"agg\",\n"
                + "      \"input\": \"df_grouped\",\n"
                + "      \"output\": \"df_aggregated\",\n"
                + "      \"params\": {\n"
                + "        \"aggs\": [\n"
                + "          { \"expr\": { \"type\": \"func\", \"name\": \"sum\", \"args\": [{ \"type\": \"col\", \"name\": \"salary\" }] }, \"alias\": \"total_salary\" },\n"
                + "          { \"expr\": { \"type\": \"func\", \"name\": \"avg\", \"args\": [{ \"type\": \"col\", \"name\": \"salary\" }] }, \"alias\": \"avg_salary\" },\n"
                + "          { \"expr\": { \"type\": \"func\", \"name\": \"count\", \"args\": [{ \"type\": \"lit\", \"value\": 1 }] }, \"alias\": \"num_employees\" }\n"
                + "        ]\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected = ""
                + "df_grouped = df.groupBy(F.col(\"department\"))\n"
                + "df_aggregated = df_grouped.agg(\n"
                + "      F.sum(F.col(\"salary\")).alias(\"total_salary\"),\n"
                + "      F.avg(F.col(\"salary\")).alias(\"avg_salary\"),\n"
                + "      F.count(F.lit(1)).alias(\"num_employees\")\n"
                + "  )\n";

        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testBasicAggregationsWithAlias", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Agg: 고유값 개수(countDistinct) 집계")
    void testCountDistinct() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"groupBy\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df_grouped\",\n"
                + "      \"params\": {\n"
                + "        \"keys\": [{ \"type\": \"col\", \"name\": \"category\" }]\n"
                + "      }\n"
                + "    },\n"
                + "    {\n"
                + "      \"node\": \"agg\",\n"
                + "      \"input\": \"df_grouped\",\n"
                + "      \"output\": \"df_aggregated\",\n"
                + "      \"params\": {\n"
                + "        \"aggs\": [\n"
                + "          { \"expr\": { \"type\": \"func\", \"name\": \"countDistinct\", \"args\": [{ \"type\": \"col\", \"name\": \"product_id\" }] }, \"alias\": \"distinct_products\" }\n"
                + "        ]\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected = ""
                + "df_grouped = df.groupBy(F.col(\"category\"))\n"
                + "df_aggregated = df_grouped.agg(\n"
                + "      F.countDistinct(F.col(\"product_id\")).alias(\"distinct_products\")\n"
                + "  )\n";

        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testCountDistinct", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Agg: 조건부 집계 (CASE WHEN 사용)")
    void testConditionalAggregation() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"groupBy\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df_grouped\",\n"
                + "      \"params\": {\n"
                + "        \"keys\": [{ \"type\": \"col\", \"name\": \"department\" }]\n"
                + "      }\n"
                + "    },\n"
                + "    {\n"
                + "      \"node\": \"agg\",\n"
                + "      \"input\": \"df_grouped\",\n"
                + "      \"output\": \"df_aggregated\",\n"
                + "      \"params\": {\n"
                + "        \"aggs\": [\n"
                + "          {\n"
                + "            \"expr\": {\n"
                + "              \"type\": \"func\", \"name\": \"sum\",\n"
                + "              \"args\": [{\n"
                + "                \"type\": \"case\",\n"
                + "                \"when\": [{\n"
                + "                  \"if\": { \"type\": \"op\", \"op\": \">=\", \"left\": { \"type\": \"col\", \"name\": \"age\" }, \"right\": { \"type\": \"lit\", \"value\": 40 } },\n"
                + "                  \"then\": { \"type\": \"lit\", \"value\": 1 }\n"
                + "                }],\n"
                + "                \"else\": { \"type\": \"lit\", \"value\": 0 }\n"
                + "              }]\n"
                + "            },\n"
                + "            \"alias\": \"employees_over_40\"\n"
                + "          }\n"
                + "        ]\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected = ""
                + "df_grouped = df.groupBy(F.col(\"department\"))\n"
                + "df_aggregated = df_grouped.agg(\n"
                + "      F.sum((F.when((F.col(\"age\") >= F.lit(40)), F.lit(1))).otherwise(F.lit(0))).alias(\"employees_over_40\")\n"
                + "  )\n";

        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testConditionalAggregation", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Agg: 표현식에 대한 집계 (sum(price * quantity))")
    void testAggregationOnExpression() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"groupBy\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df_grouped\",\n"
                + "      \"params\": {\n"
                + "        \"keys\": [{ \"type\": \"col\", \"name\": \"order_id\" }]\n"
                + "      }\n"
                + "    },\n"
                + "    {\n"
                + "      \"node\": \"agg\",\n"
                + "      \"input\": \"df_grouped\",\n"
                + "      \"output\": \"df_aggregated\",\n"
                + "      \"params\": {\n"
                + "        \"aggs\": [\n"
                + "          {\n"
                + "            \"expr\": {\n"
                + "              \"type\": \"func\", \"name\": \"sum\",\n"
                + "              \"args\": [{\n"
                + "                \"type\": \"op\", \"op\": \"*\",\n"
                + "                \"left\": { \"type\": \"col\", \"name\": \"price\" },\n"
                + "                \"right\": { \"type\": \"col\", \"name\": \"quantity\" }\n"
                + "              }]\n"
                + "            },\n"
                + "            \"alias\": \"total_amount\"\n"
                + "          }\n"
                + "        ]\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected = ""
                + "df_grouped = df.groupBy(F.col(\"order_id\"))\n"
                + "df_aggregated = df_grouped.agg(\n"
                + "      F.sum((F.col(\"price\") * F.col(\"quantity\"))).alias(\"total_amount\")\n"
                + "  )\n";

        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testAggregationOnExpression", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Agg: collect_list와 collect_set 집계")
    void testCollectAggregations() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"groupBy\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df_grouped\",\n"
                + "      \"params\": {\n"
                + "        \"keys\": [{ \"type\": \"col\", \"name\": \"department\" }]\n"
                + "      }\n"
                + "    },\n"
                + "    {\n"
                + "      \"node\": \"agg\",\n"
                + "      \"input\": \"df_grouped\",\n"
                + "      \"output\": \"df_aggregated\",\n"
                + "      \"params\": {\n"
                + "        \"aggs\": [\n"
                + "          { \"expr\": { \"type\": \"func\", \"name\": \"collect_list\", \"args\": [{ \"type\": \"col\", \"name\": \"employee_name\" }] }, \"alias\": \"employee_list\" },\n"
                + "          { \"expr\": { \"type\": \"func\", \"name\": \"collect_set\", \"args\": [{ \"type\": \"col\", \"name\": \"project_id\" }] }, \"alias\": \"project_set\" }\n"
                + "        ]\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected = ""
                + "df_grouped = df.groupBy(F.col(\"department\"))\n"
                + "df_aggregated = df_grouped.agg(\n"
                + "      F.collect_list(F.col(\"employee_name\")).alias(\"employee_list\"),\n"
                + "      F.collect_set(F.col(\"project_id\")).alias(\"project_set\")\n"
                + "  )\n";

        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testCollectAggregations", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Agg: groupBy 없는 전체(Global) 집계")
    void testGlobalAggregationWithoutGroupBy() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"agg\",\n"
                + "      \"input\": \"sales_df\",\n"
                + "      \"output\": \"result_df\",\n"
                + "      \"params\": {\n"
                + "        \"aggs\": [\n"
                + "          { \"expr\": { \"type\": \"func\", \"name\": \"sum\", \"args\": [{ \"type\": \"col\", \"name\": \"amount\" }] }, \"alias\": \"total_sales\" },\n"
                + "          { \"expr\": { \"type\": \"func\", \"name\": \"max\", \"args\": [{ \"type\": \"col\", \"name\": \"transaction_date\" }] }, \"alias\": \"latest_transaction\" }\n"
                + "        ]\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected = ""
                + "result_df = sales_df.agg(\n"
                + "      F.sum(F.col(\"amount\")).alias(\"total_sales\"),\n"
                + "      F.max(F.col(\"transaction_date\")).alias(\"latest_transaction\")\n"
                + "  )\n";

        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testGlobalAggregationWithoutGroupBy", json, actual);
        assertEquals(expected, actual);
    }

}
