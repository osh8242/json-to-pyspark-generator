package com.douzone.platform.recipe.show;

import com.douzone.platform.recipe.PySparkChainGenerator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

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
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"show\",\n"
                + "      \"input\": \"df\"\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected = "df.show(20)\n";
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testBasicShow", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Show: 행 수와 옵션 지정 (5행, 전체 출력, 세로 형식)")
    void testShowWithOptions() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"show\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"params\": {\n"
                + "        \"n\": 5,\n"
                + "        \"truncate\": false,\n"
                + "        \"vertical\": true\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected = "df.show(5, truncate=False, vertical=True)\n";
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testShowWithOptions", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Show: truncate 길이 지정")
    void testShowWithTruncateLength() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"show\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"params\": {\n"
                + "        \"truncate\": 50\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected = "df.show(20, truncate=50)\n";
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testShowWithTruncateLength", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Show: 다단계 체인 끝에 show 적용")
    void testShowInMultiStepPipeline() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"filter\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df_filtered\",\n"
                + "      \"params\": {\n"
                + "        \"condition\": {\n"
                + "          \"type\": \"op\", \"op\": \">\",\n"
                + "          \"left\": { \"type\": \"col\", \"name\": \"salary\" },\n"
                + "          \"right\": { \"type\": \"lit\", \"value\": 50000 }\n"
                + "        }\n"
                + "      }\n"
                + "    },\n"
                + "    {\n"
                + "      \"node\": \"groupBy\",\n"
                + "      \"input\": \"df_filtered\",\n"
                + "      \"output\": \"df_grouped\",\n"
                + "      \"params\": {\n"
                + "        \"keys\": [ { \"type\": \"col\", \"name\": \"department\" } ]\n"
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
                + "              \"type\": \"func\", \"name\": \"avg\",\n"
                + "              \"args\": [ { \"type\": \"col\", \"name\": \"salary\" } ]\n"
                + "            },\n"
                + "            \"alias\": \"avg_salary\"\n"
                + "          }\n"
                + "        ]\n"
                + "      }\n"
                + "    },\n"
                + "    {\n"
                + "      \"node\": \"show\",\n"
                + "      \"input\": \"df_aggregated\",\n"
                + "      \"params\": { \"n\": 10 }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected =
                "df_filtered = df.filter((F.col(\"salary\") > F.lit(50000)))\n" +
                        "df_grouped = df_filtered.groupBy(F.col(\"department\"))\n" +
                        "df_aggregated = df_grouped.agg(\n" +
                        "      F.avg(F.col(\"salary\")).alias(\"avg_salary\")\n" +
                        "  )\n" +
                        "df_aggregated.show(10)\n";

        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testShowInMultiStepPipeline", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Show: 다중 show 스텝 순서 보존")
    void testMultipleShowStepsPreserveOrder() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"show\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"params\": { \"n\": 3 }\n"
                + "    },\n"
                + "    {\n"
                + "      \"node\": \"filter\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df_filtered\",\n"
                + "      \"params\": {\n"
                + "        \"condition\": {\n"
                + "          \"type\": \"op\", \"op\": \"==\",\n"
                + "          \"left\": { \"type\": \"col\", \"name\": \"status\" },\n"
                + "          \"right\": { \"type\": \"lit\", \"value\": \"ACTIVE\" }\n"
                + "        }\n"
                + "      }\n"
                + "    },\n"
                + "    {\n"
                + "      \"node\": \"show\",\n"
                + "      \"input\": \"df_filtered\",\n"
                + "      \"params\": { \"n\": 8, \"truncate\": false }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected =
                "df.show(3)\n" +
                        "df_filtered = df.filter((F.col(\"status\") == F.lit(\"ACTIVE\")))\n" +
                        "df_filtered.show(8, truncate=False)\n";

        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testMultipleShowStepsPreserveOrder", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Show: 중간 show 가 체인을 분할하여 중간 결과를 출력")
    void testShowSplitsPipelineForIntermediateResults() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"withColumn\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df_with_flag\",\n"
                + "      \"params\": {\n"
                + "        \"name\": \"flag\",\n"
                + "        \"expr\": { \"type\": \"lit\", \"value\": 1 }\n"
                + "      }\n"
                + "    },\n"
                + "    {\n"
                + "      \"node\": \"show\",\n"
                + "      \"input\": \"df_with_flag\"\n"
                + "    },\n"
                + "    {\n"
                + "      \"node\": \"filter\",\n"
                + "      \"input\": \"df_with_flag\",\n"
                + "      \"output\": \"df_filtered\",\n"
                + "      \"params\": {\n"
                + "        \"condition\": {\n"
                + "          \"type\": \"op\", \"op\": \">\",\n"
                + "          \"left\": { \"type\": \"col\", \"name\": \"age\" },\n"
                + "          \"right\": { \"type\": \"lit\", \"value\": 30 }\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected =
                "df_with_flag = df.withColumn(\"flag\", F.lit(1))\n" +
                        "df_with_flag.show(20)\n" +
                        "df_filtered = df_with_flag.filter((F.col(\"age\") > F.lit(30)))\n";

        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testShowSplitsPipelineForIntermediateResults", json, actual);
        assertEquals(expected, actual);
    }
}
