package com.douzone.platform.recipe.complex;

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
 * 2025. 9. 19.        osh8242       최초 생성
 */
public class MultiStepPipelineTest {

    @Test
    @DisplayName("다단계 파이프라인 1: Filter -> GroupBy -> Agg -> OrderBy -> Limit")
    void testFullEtlPipeline() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"filter\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df_filtered\",\n"
                + "      \"params\": {\n"
                + "        \"condition\": { \"type\": \"op\", \"op\": \"=\", \"left\": { \"type\": \"col\", \"name\": \"is_completed\" }, \"right\": { \"type\": \"lit\", \"value\": true } }\n"
                + "      }\n"
                + "    },\n"
                + "    {\n"
                + "      \"node\": \"groupBy\",\n"
                + "      \"input\": \"df_filtered\",\n"
                + "      \"output\": \"df_grouped\",\n"
                + "      \"params\": {\n"
                + "        \"keys\": [ { \"type\": \"col\", \"name\": \"category\" } ]\n"
                + "      }\n"
                + "    },\n"
                + "    {\n"
                + "      \"node\": \"agg\",\n"
                + "      \"input\": \"df_grouped\",\n"
                + "      \"output\": \"df_aggregated\",\n"
                + "      \"params\": {\n"
                + "        \"aggs\": [\n"
                + "          { \"expr\": { \"type\": \"func\", \"name\": \"sum\", \"args\": [ { \"type\": \"col\", \"name\": \"amount\" } ] }, \"alias\": \"total_amount\" },\n"
                + "          { \"expr\": { \"type\": \"func\", \"name\": \"count\", \"args\": [ { \"type\": \"col\", \"name\": \"order_id\" } ] }, \"alias\": \"order_count\" }\n"
                + "        ]\n"
                + "      }\n"
                + "    },\n"
                + "    {\n"
                + "      \"node\": \"orderBy\",\n"
                + "      \"input\": \"df_aggregated\",\n"
                + "      \"output\": \"df_ordered\",\n"
                + "      \"params\": {\n"
                + "        \"keys\": [ { \"expr\": { \"type\": \"col\", \"name\": \"total_amount\" }, \"asc\": false } ]\n"
                + "      }\n"
                + "    },\n"
                + "    {\n"
                + "      \"node\": \"limit\",\n"
                + "      \"input\": \"df_ordered\",\n"
                + "      \"output\": \"df_limited\",\n"
                + "      \"params\": {\n"
                + "        \"n\": 10\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        // buildOp 규칙에 따라 (left op right) 형태의 괄호만 생성됩니다.
        String expectedSteps = "df_filtered = df.filter((F.col(\"is_completed\") == F.lit(True)))\n" +
                "df_grouped = df_filtered.groupBy(F.col(\"category\"))\n" +
                "df_aggregated = df_grouped.agg(\n" +
                "      F.sum(F.col(\"amount\")).alias(\"total_amount\"),\n" +
                "      F.count(F.col(\"order_id\")).alias(\"order_count\")\n" +
                "  )\n" +
                "df_ordered = df_aggregated.orderBy(F.col(\"total_amount\").desc())\n" +
                "df_limited = df_ordered.limit(10)\n";

        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testFullEtlPipeline", json, actual);
        assertEquals(expectedSteps, actual);
    }

    @Test
    @DisplayName("다단계 파이프라인 2: Join -> Select -> withColumn -> OrderBy")
    void testJoinAndTransformationPipeline() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"join\",\n"
                + "      \"input\": \"employees\",\n"
                + "      \"output\": \"employees_join_dept\",\n"
                + "      \"params\": {\n"
                + "        \"right\": \"departments\",\n"
                + "        \"how\": \"left\",\n"
                + "        \"leftAlias\": \"e\",\n"
                + "        \"rightAlias\": \"d\",\n"
                + "        \"on\": { \"type\": \"op\", \"op\": \"=\", \"left\": { \"type\": \"col\", \"name\": \"dept_id\", \"table\": \"e\" }, \"right\": { \"type\": \"col\", \"name\": \"id\", \"table\": \"d\" } }\n"
                + "      }\n"
                + "    },\n"
                + "    {\n"
                + "      \"node\": \"select\",\n"
                + "      \"input\": \"employees_join_dept\",\n"
                + "      \"output\": \"employees_selected\",\n"
                + "      \"params\": {\n"
                + "        \"columns\": [\n"
                + "          { \"expr\": { \"type\": \"col\", \"name\": \"name\", \"table\": \"e\" }, \"alias\": \"employee_name\" },\n"
                + "          { \"expr\": { \"type\": \"col\", \"name\": \"salary\", \"table\": \"e\" } },\n"
                + "          { \"expr\": { \"type\": \"col\", \"name\": \"dept_name\", \"table\": \"d\" } }\n"
                + "        ]\n"
                + "      }\n"
                + "    },\n"
                + "    {\n"
                + "      \"node\": \"withColumn\",\n"
                + "      \"input\": \"employees_selected\",\n"
                + "      \"output\": \"employees_with_salary_grade\",\n"
                + "      \"params\": {\n"
                + "        \"name\": \"salary_grade\",\n"
                + "        \"expr\": {\n"
                + "          \"type\": \"case\",\n"
                + "          \"when\": [ { \"if\": { \"type\": \"op\", \"op\": \">\", \"left\": { \"type\": \"col\", \"name\": \"salary\" }, \"right\": { \"type\": \"lit\", \"value\": 100000 } }, \"then\": { \"type\": \"lit\", \"value\": \"High\" } } ],\n"
                + "          \"else\": { \"type\": \"lit\", \"value\": \"Standard\" }\n"
                + "        }\n"
                + "      }\n"
                + "    },\n"
                + "    {\n"
                + "      \"node\": \"orderBy\",\n"
                + "      \"input\": \"employees_with_salary_grade\",\n"
                + "      \"output\": \"employees_ordered\",\n"
                + "      \"params\": {\n"
                + "        \"keys\": [ { \"expr\": { \"type\": \"col\", \"name\": \"salary\" }, \"asc\": false } ]\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";


        // on 조건과 case문의 if 조건 괄호를 실제 생성 로직에 맞게 수정합니다.
        String expectedSteps = "employees_join_dept = employees.alias(\"e\").join(\n" +
                "  (departments).alias(\"d\"),\n" +
                "  (F.col(\"e.dept_id\") == F.col(\"d.id\")),\n" +
                "  \"left\"\n" +
                ")\n" +
                "employees_selected = employees_join_dept.select(F.col(\"e.name\").alias(\"employee_name\"), F.col(\"e.salary\"), F.col(\"d.dept_name\"))\n" +
                "employees_with_salary_grade = employees_selected.withColumn(\"salary_grade\", (F.when((F.col(\"salary\") > F.lit(100000)), F.lit(\"High\"))).otherwise(F.lit(\"Standard\")))\n" +
                "employees_ordered = employees_with_salary_grade.orderBy(F.col(\"salary\").desc())\n";

        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testJoinAndTransformationPipeline", json, actual);
        assertEquals(expectedSteps, actual);
    }
}
