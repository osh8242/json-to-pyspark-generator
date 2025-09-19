package com.douzone.platform.recipe.complex;

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
 * 2025. 9. 19.        osh8242       최초 생성
 */
public class MultiStepPipelineTest {

    @Test
    @DisplayName("다단계 파이프라인 1: Filter -> GroupBy -> Agg -> OrderBy -> Limit")
    void testFullEtlPipeline() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    { \"step\": \"filter\", \"condition\": { \"type\": \"op\", \"op\": \"=\", \"left\": { \"type\": \"col\", \"name\": \"is_completed\" }, \"right\": { \"type\": \"lit\", \"value\": true } } },\n"
                + "    { \"step\": \"groupBy\", \"keys\": [{ \"type\": \"col\", \"name\": \"category\" }] },\n"
                + "    {\n"
                + "      \"step\": \"agg\",\n"
                + "      \"aggs\": [\n"
                + "        { \"expr\": { \"type\": \"func\", \"name\": \"sum\", \"args\": [{ \"type\": \"col\", \"name\": \"amount\" }] }, \"alias\": \"total_amount\" },\n"
                + "        { \"expr\": { \"type\": \"func\", \"name\": \"count\", \"args\": [{ \"type\": \"col\", \"name\": \"order_id\" }] }, \"alias\": \"order_count\" }\n"
                + "      ]\n"
                + "    },\n"
                + "    { \"step\": \"orderBy\", \"keys\": [{ \"expr\": { \"type\": \"col\", \"name\": \"total_amount\" }, \"asc\": false }] },\n"
                + "    { \"step\": \"limit\", \"n\": 10 }\n"
                + "  ]\n"
                + "}";

        // buildOp 규칙에 따라 (left op right) 형태의 괄호만 생성됩니다.
        String expectedSteps = "  .filter((F.col(\"is_completed\") == F.lit(True)))\n"
                + "  .groupBy(F.col(\"category\"))\n"
                + "  .agg(\n"
                + "      F.sum(F.col(\"amount\")).alias(\"total_amount\"),\n"
                + "      F.count(F.col(\"order_id\")).alias(\"order_count\")\n"
                + "  )\n"
                + "  .orderBy(F.col(\"total_amount\").desc())\n"
                + "  .limit(10)\n";

        String expected = buildFullScript(expectedSteps);
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testFullEtlPipeline", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("다단계 파이프라인 2: Join -> Select -> withColumn -> OrderBy")
    void testJoinAndTransformationPipeline() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"step\": \"join\",\n"
                + "      \"right\": \"departments\",\n"
                + "      \"how\": \"left\",\n"
                + "      \"leftAlias\": \"e\",\n"
                + "      \"rightAlias\": \"d\",\n"
                + "      \"on\": { \"type\": \"op\", \"op\": \"=\", \"left\": { \"type\": \"col\", \"name\": \"dept_id\", \"table\": \"e\" }, \"right\": { \"type\": \"col\", \"name\": \"id\", \"table\": \"d\" } }\n"
                + "    },\n"
                + "    {\n"
                + "      \"step\": \"select\",\n"
                + "      \"columns\": [\n"
                + "        { \"expr\": { \"type\": \"col\", \"name\": \"name\", \"table\": \"e\" }, \"alias\": \"employee_name\" },\n"
                + "        { \"expr\": { \"type\": \"col\", \"name\": \"salary\", \"table\": \"e\" } },\n"
                + "        { \"expr\": { \"type\": \"col\", \"name\": \"dept_name\", \"table\": \"d\" } }\n"
                + "      ]\n"
                + "    },\n"
                + "    {\n"
                + "      \"step\": \"withColumn\",\n"
                + "      \"name\": \"salary_grade\",\n"
                + "      \"expr\": {\n"
                + "        \"type\": \"case\",\n"
                + "        \"when\": [ { \"if\": { \"type\": \"op\", \"op\": \">\", \"left\": { \"type\": \"col\", \"name\": \"salary\" }, \"right\": { \"type\": \"lit\", \"value\": 100000 } }, \"then\": { \"type\": \"lit\", \"value\": \"High\" } } ],\n"
                + "        \"else\": { \"type\": \"lit\", \"value\": \"Standard\" }\n"
                + "      }\n"
                + "    },\n"
                + "    { \"step\": \"orderBy\", \"keys\": [{ \"expr\": { \"type\": \"col\", \"name\": \"salary\" }, \"asc\": false }] }\n"
                + "  ]\n"
                + "}";

        // on 조건과 case문의 if 조건 괄호를 실제 생성 로직에 맞게 수정합니다.
        String expectedSteps = "  .alias(\"e\")\n"
                + "  .join((departments).alias(\"d\"), (F.col(\"e.dept_id\") == F.col(\"d.id\")), \"left\")\n"
                + "  .select(F.col(\"e.name\").alias(\"employee_name\"), F.col(\"e.salary\"), F.col(\"d.dept_name\"))\n"
                + "  .withColumn(\"salary_grade\", (F.when((F.col(\"salary\") > F.lit(100000)), F.lit(\"High\"))).otherwise(F.lit(\"Standard\")))\n"
                + "  .orderBy(F.col(\"salary\").desc())\n";

        String expected = buildFullScript(expectedSteps);
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testJoinAndTransformationPipeline", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("다단계 파이프라인 3: 재귀 조인(서브쿼리) 포함")
    void testRecursiveJoinPipeline() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"step\": \"join\",\n"
                + "      \"right\": {\n"
                + "        \"input\": \"logs\",\n"
                + "        \"steps\": [\n"
                + "          { \"step\": \"filter\", \"condition\": { \"type\": \"op\", \"op\": \"=\", \"left\": { \"type\": \"col\", \"name\": \"action\" }, \"right\": { \"type\": \"lit\", \"value\": \"purchase\" } } },\n"
                + "          { \"step\": \"groupBy\", \"keys\": [{ \"type\": \"col\", \"name\": \"user_id\" }] },\n"
                + "          { \"step\": \"agg\", \"aggs\": [{ \"expr\": { \"type\": \"func\", \"name\": \"count\", \"args\": [{ \"type\": \"lit\", \"value\": 1 }] }, \"alias\": \"purchase_count\" }] }\n"
                + "        ]\n"
                + "      },\n"
                + "      \"how\": \"inner\",\n"
                + "      \"rightAlias\": \"purchases\",\n"
                + "      \"on\": { \"type\": \"op\", \"op\": \"=\", \"left\": { \"type\": \"col\", \"name\": \"id\" }, \"right\": { \"type\": \"col\", \"name\": \"user_id\", \"table\": \"purchases\" } }\n"
                + "    },\n"
                + "    { \"step\": \"filter\", \"condition\": { \"type\": \"op\", \"op\": \">\", \"left\": { \"type\": \"col\", \"name\": \"purchase_count\" }, \"right\": { \"type\": \"lit\", \"value\": 5 } } }\n"
                + "  ]\n"
                + "}";

        // 서브쿼리의 filter 조건과 최종 filter 조건의 괄호를 실제 생성 로직에 맞게 수정합니다.
        String expectedSteps = "  .join(((\n"
                + "    logs\n"
                + "    .filter((F.col(\"action\") == F.lit(\"purchase\")))\n"
                + "    .groupBy(F.col(\"user_id\"))\n"
                + "    .agg(\n"
                + "        F.count(F.lit(1)).alias(\"purchase_count\")\n"
                + "    )\n"
                + "  )).alias(\"purchases\"), (F.col(\"id\") == F.col(\"purchases.user_id\")), \"inner\")\n"
                + "  .filter((F.col(\"purchase_count\") > F.lit(5)))\n";

        String expected = buildFullScript(expectedSteps);
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testRecursiveJoinPipeline", json, actual);
        assertEquals(expected, actual);
    }

}
