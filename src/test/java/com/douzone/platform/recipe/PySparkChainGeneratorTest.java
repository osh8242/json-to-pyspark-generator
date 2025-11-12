package com.douzone.platform.recipe;

import com.douzone.platform.recipe.util.TestUtil;
import com.douzone.platform.util.FormatUtil;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static com.douzone.platform.recipe.util.TestUtil.toNodeJson;

/**
 * description    :
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2025. 9. 16.        osh8242       최초 생성
 */
public class PySparkChainGeneratorTest {

    private static String lines(String... lines) {
        return String.join("\n", lines);
    }

    private String generate(String json) throws Exception {
        return PySparkChainGenerator.generate(json);
    }

    @Test
    @DisplayName("node 기반 파이프라인은 단계별 코드 라인을 생성")
    void testNodeBasedPipelineProducesStepAssignments() throws Exception {
        String json = toNodeJson("{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"input\": \"df\",\n"
                + "      \"node\": \"filter\",\n"
                + "      \"condition\": {\n"
                + "        \"type\": \"op\",\n"
                + "        \"op\": \"and\",\n"
                + "        \"left\": {\n"
                + "          \"type\": \"op\",\n"
                + "          \"op\": \">=\",\n"
                + "          \"left\": {\"type\": \"col\", \"name\": \"age\"},\n"
                + "          \"right\": {\"type\": \"lit\", \"value\": 65}\n"
                + "        },\n"
                + "        \"right\": {\n"
                + "          \"type\": \"op\",\n"
                + "          \"op\": \"=\",\n"
                + "          \"left\": {\"type\": \"col\", \"name\": \"gender\"},\n"
                + "          \"right\": {\"type\": \"lit\", \"value\": \"F\"}\n"
                + "        }\n"
                + "      },\n"
                + "      \"output\": \"filter1\"\n"
                + "    },\n"
                + "    { \"input\": \"filter1\", \"node\": \"groupBy\", \"keys\": [ { \"type\": \"col\", \"name\": \"disease_code\" } ], \"output\": \"groupby1\" },\n"
                + "    { \"input\": \"groupby1\", \"node\": \"agg\", \"aggs\": [ { \"expr\": { \"type\": \"func\", \"name\": \"count\", \"args\": [ { \"type\": \"col\", \"name\": \"*\" } ] }, \"alias\": \"cnt\" } ], \"output\": \"agg1\" },\n"
                + "    { \"input\": \"agg1\", \"node\": \"orderBy\", \"keys\": [ { \"expr\": { \"type\": \"col\", \"name\": \"cnt\" }, \"asc\": false } ], \"output\": \"orderby1\" },\n"
                + "    { \"input\": \"orderby1\", \"node\": \"limit\", \"n\": 100, \"output\": \"limit1\" },\n"
                + "    { \"input\": \"filter1\", \"node\": \"groupBy\", \"keys\": [ { \"type\": \"col\", \"name\": \"gender\" } ], \"output\": \"groupby2\" }\n"
                + "  ]\n"
                + "}";

        String code = generate(json);

        String expected = lines(
                "filter1 = df.filter(((F.col(\"age\") >= F.lit(65)) & (F.col(\"gender\") == F.lit(\"F\"))))",
                "groupby1 = filter1.groupBy(F.col(\"disease_code\"))",
                "agg1 = groupby1.agg(",
                "      F.count(F.col(\"*\")).alias(\"cnt\")",
                "  )",
                "orderby1 = agg1.orderBy(F.col(\"cnt\").desc())",
                "limit1 = orderby1.limit(100)",
                "groupby2 = filter1.groupBy(F.col(\"gender\"))",
                "");

        Assertions.assertThat(code).isEqualTo(expected);
    }

    @Test
    public void testBasicPipeline_containsExpectedFragments() throws Exception {
        String json = toNodeJson("{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"input\": \"df\",\n"
                + "      \"node\": \"filter\",\n"
                + "      \"condition\": {\n"
                + "        \"type\": \"op\",\n"
                + "        \"op\": \"and\",\n"
                + "        \"left\": {\n"
                + "          \"type\": \"op\",\n"
                + "          \"op\": \">=\",\n"
                + "          \"left\": {\"type\": \"col\", \"name\": \"age\"},\n"
                + "          \"right\": {\"type\": \"lit\", \"value\": 65}\n"
                + "        },\n"
                + "        \"right\": {\n"
                + "          \"type\": \"op\",\n"
                + "          \"op\": \"=\",\n"
                + "          \"left\": {\"type\": \"col\", \"name\": \"gender\"},\n"
                + "          \"right\": {\"type\": \"lit\", \"value\": \"F\"}\n"
                + "        }\n"
                + "      },\n"
                + "      \"output\": \"filtered\"\n"
                + "    },\n"
                + "    { \"input\": \"filtered\", \"node\": \"groupBy\", \"keys\": [ { \"type\": \"col\", \"name\": \"disease_code\" } ], \"output\": \"grouped\" },\n"
                + "    { \"input\": \"grouped\", \"node\": \"agg\", \"aggs\": [ { \"expr\": { \"type\": \"func\", \"name\": \"count\", \"args\": [ { \"type\": \"col\", \"name\": \"*\" } ] }, \"alias\": \"cnt\" } ], \"output\": \"aggregated\" },\n"
                + "    { \"input\": \"aggregated\", \"node\": \"orderBy\", \"keys\": [ { \"expr\": { \"type\": \"col\", \"name\": \"cnt\" }, \"asc\": false } ], \"output\": \"ordered\" },\n"
                + "    { \"input\": \"ordered\", \"node\": \"limit\", \"n\": 100, \"output\": \"limited\" }\n"
                + "  ]\n"
                + "}";

        String code = generate(json);

        assertTrue(code.contains("F.col(\"age\")"), "age 컬럼 참조가 포함되어야 함");
        assertTrue(code.contains("F.lit(65)"), "65 리터럴이 포함되어야 함");
        assertTrue(code.contains("F.col(\"gender\")"), "gender 컬럼 참조가 포함되어야 함");
        assertTrue(code.contains("F.lit(\"F\")"), "문자열 'F' 리터럴이 포함되어야 함");

        assertTrue(code.contains(".groupBy("), "groupBy 호출이 포함되어야 함");
        assertTrue(code.contains("F.count("), "count 집계가 포함되어야 함");
        assertTrue(code.contains(".alias(\"cnt\")"), "agg alias가 포함되어야 함");
        assertTrue(code.contains(".orderBy("), "orderBy 호출이 포함되어야 함");
        assertTrue(code.contains(".limit(100)"), "limit(100) 포함되어야 함");
    }

    @Test
    @DisplayName("withColumn, withColumns 테스트")
    public void testWithColumn_and_withColumns_containsExpectedFragments() throws Exception {
        String json = toNodeJson("{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"input\": \"df\",\n"
                + "      \"node\": \"withColumn\",\n"
                + "      \"name\": \"age_plus_one\",\n"
                + "      \"expr\": {\n"
                + "        \"type\": \"op\",\n"
                + "        \"op\": \"+\",\n"
                + "        \"left\": {\"type\": \"col\", \"name\": \"age\"},\n"
                + "        \"right\": {\"type\": \"lit\", \"value\": 1}\n"
                + "      },\n"
                + "      \"output\": \"with_age\"\n"
                + "    },\n"
                + "    {\n"
                + "      \"input\": \"with_age\",\n"
                + "      \"node\": \"withColumns\",\n"
                + "      \"cols\": {\n"
                + "        \"is_senior\": { \"type\": \"op\", \"op\": \">=\", \"left\": {\"type\":\"col\",\"name\":\"age\"}, \"right\": {\"type\":\"lit\",\"value\":65} },\n"
                + "        \"gender_flag\": {\n"
                + "          \"type\": \"case\",\n"
                + "          \"when\": [\n"
                + "            { \"if\": { \"type\": \"op\", \"op\": \"=\", \"left\": {\"type\":\"col\",\"name\":\"gender\"}, \"right\": {\"type\":\"lit\",\"value\":\"F\"} }, \"then\": {\"type\":\"lit\",\"value\":1} }\n"
                + "          ],\n"
                + "          \"else\": {\"type\":\"lit\",\"value\":0}\n"
                + "        }\n"
                + "      },\n"
                + "      \"output\": \"with_flags\"\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String code = generate(json);

        assertTrue(code.contains(".withColumn(\"age_plus_one\""), "withColumn 이름이 포함되어야 함");
        assertTrue(code.contains("F.col(\"age\")"), "age 컬럼 참조 포함");
        assertTrue(code.contains("F.lit(1)"), "숫자 리터럴 포함");

        assertTrue(code.contains("\"is_senior\""), "withColumns 키 is_senior 포함");
        assertTrue(code.contains("\"gender_flag\""), "withColumns 키 gender_flag 포함");
        assertTrue(code.contains(".when(") || code.contains("F.when("), "CASE/when 표현 포함 (when 존재 여부 확인)");
    }

    @Test
    @DisplayName("알 수 없는 node는 기본 메서드 체인으로 처리")
    void testUnknownStepFallsBackToDefaultBuilder() throws Exception {
        String json = toNodeJson("{\n"
                + "  \"steps\": [\n"
                + "    { \"input\": \"df\", \"node\": \"cache\", \"output\": \"cached\" },\n"
                + "    { \"input\": \"cached\", \"node\": \"persist\", \"args\": [ { \"type\": \"lit\", \"value\": \"MEMORY_ONLY\" } ], \"output\": \"persisted\" },\n"
                + "    { \"input\": \"persisted\", \"node\": \"count\", \"output\": \"counted\" }\n"
                + "  ]\n"
                + "}";

        String code = generate(json);

        Assertions.assertThat(code).contains(".cache()");
        Assertions.assertThat(code).contains(".persist(F.lit(\"MEMORY_ONLY\"))");
        Assertions.assertThat(code).contains(".count()");
    }

    @Test
    @DisplayName("join 테스트")
    public void testJoin_withAliases_containsExpectedFragments() throws Exception {
        String json = toNodeJson("{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"input\": \"df\",\n"
                + "      \"node\": \"join\",\n"
                + "      \"right\": \"other\",\n"
                + "      \"rightAlias\": \"b\",\n"
                + "      \"leftAlias\": \"a\",\n"
                + "      \"on\": [\n"
                + "        {\n"
                + "          \"type\": \"op\",\n"
                + "          \"op\": \"=\",\n"
                + "          \"left\": {\n"
                + "            \"type\": \"col\",\n"
                + "            \"name\": \"id\",\n"
                + "            \"alias\": \"a\"\n"
                + "          },\n"
                + "          \"right\": {\n"
                + "            \"type\": \"col\",\n"
                + "            \"name\": \"id\",\n"
                + "            \"alias\": \"b\"\n"
                + "          }\n"
                + "        }\n"
                + "      ],\n"
                + "      \"how\": \"left\",\n"
                + "      \"output\": \"joined\"\n"
                + "    },\n"
                + "    {\n"
                + "      \"input\": \"joined\",\n"
                + "      \"node\": \"select\",\n"
                + "      \"columns\": [\n"
                + "        {\n"
                + "          \"as\": \"id\",\n"
                + "          \"expr\": {\n"
                + "            \"type\": \"col\",\n"
                + "            \"name\": \"id\",\n"
                + "            \"table\": \"a\"\n"
                + "          }\n"
                + "        },\n"
                + "        {\n"
                + "          \"as\": \"other_val\",\n"
                + "          \"expr\": {\n"
                + "            \"type\": \"col\",\n"
                + "            \"name\": \"val\",\n"
                + "            \"table\": \"b\"\n"
                + "          }\n"
                + "        }\n"
                + "      ],\n"
                + "      \"output\": \"selected\"\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String code = generate(json);

        Assertions.assertThat(code).contains(".alias(\"a\")");
        Assertions.assertThat(code).contains(".join(other, (F.col(\"a.id\") == F.col(\"b.id\")), \"left\")");
        Assertions.assertThat(code).contains(".select(");
        Assertions.assertThat(code).contains(".alias(\"id\")");
        Assertions.assertThat(code).contains(".alias(\"other_val\")");
    }

    @Test
    @DisplayName("filter - and 조건 테스트")
    public void testPatientInfoFilter_simpleAndCondition_printsAndAsserts() throws Exception {
        String json = toNodeJson("{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"input\": \"spark.table('patientinfo')\",\n"
                + "      \"node\": \"filter\",\n"
                + "      \"condition\": {\n"
                + "        \"type\": \"op\",\n"
                + "        \"op\": \"and\",\n"
                + "        \"left\": {\n"
                + "          \"type\": \"op\",\n"
                + "          \"op\": \">=\",\n"
                + "          \"left\": { \"type\": \"col\", \"name\": \"age\" },\n"
                + "          \"right\": { \"type\": \"lit\", \"value\": 65 }\n"
                + "        },\n"
                + "        \"right\": {\n"
                + "          \"type\": \"op\",\n"
                + "          \"op\": \"=\",\n"
                + "          \"left\": { \"type\": \"col\", \"name\": \"gender\" },\n"
                + "          \"right\": { \"type\": \"lit\", \"value\": \"F\" }\n"
                + "        }\n"
                + "      },\n"
                + "      \"output\": \"filtered\"\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String code = generate(json);

        assertTrue(code.contains("spark.table('patientinfo')") || code.contains("spark.table(\"patientinfo\")"),
                "입력 테이블 patientinfo가 spark.table(...) 형태로 포함되어야 함");
        assertTrue(code.contains(".filter("), "filter 호출이 포함되어야 함");
        assertTrue(code.contains("F.col(\"age\")"), "age 컬럼 참조가 포함되어야 함");
        assertTrue(code.contains("F.lit(65)"), "숫자 리럴 65가 포함되어야 함");
        assertTrue(code.contains("F.col(\"gender\")"), "gender 컬럼 참조가 포함되어야 함");
        assertTrue(code.contains("F.lit(\"F\")"), "문자열 'F' 리터럴이 포함되어야 함");
        assertTrue(code.contains("&") || code.contains(" & "), "AND 연산이 & 로 변환되어 포함되어야 함");
    }

    @Test
    @DisplayName("groupBy - agg 테스트")
    public void testGroupBy_patientinfo_printsAndAsserts() throws Exception {
        String json = toNodeJson("{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"input\": \"spark.table('patientinfo')\",\n"
                + "      \"node\": \"groupBy\",\n"
                + "      \"keys\": [\n"
                + "        { \"type\": \"col\", \"name\": \"disease_code\" }\n"
                + "      ],\n"
                + "      \"output\": \"grouped\"\n"
                + "    },\n"
                + "    {\n"
                + "      \"input\": \"grouped\",\n"
                + "      \"node\": \"agg\",\n"
                + "      \"aggs\": [\n"
                + "        {\n"
                + "          \"expr\": { \"type\": \"func\", \"name\": \"count\", \"args\": [ { \"type\": \"lit\", \"value\": \"*\" } ] },\n"
                + "          \"alias\": \"cnt\"\n"
                + "        },\n"
                + "        {\n"
                + "          \"expr\": { \"type\": \"func\", \"name\": \"avg\", \"args\": [ { \"type\": \"col\", \"name\": \"age\" } ] },\n"
                + "          \"alias\": \"avg_age\"\n"
                + "        }\n"
                + "      ],\n"
                + "      \"output\": \"aggregated\"\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String code = generate(json);

        assertTrue(code.contains("spark.table('patientinfo')") || code.contains("spark.table(\"patientinfo\")"),
                "입력 테이블 patientinfo가 spark.table(...) 형태로 포함되어야 함");
        assertTrue(code.contains(".groupBy("), "groupBy 호출이 포함되어야 함");
        assertTrue(code.contains("F.count("), "count 집계 함수가 포함되어야 함");
        assertTrue(code.contains(".alias(\"cnt\")"), "count alias 'cnt' 포함되어야 함");
        assertTrue(code.contains("F.avg(") && code.contains("F.col(\"age\")"), "avg(age) 표현이 포함되어야 함");
        assertTrue(code.contains(".alias(\"avg_age\")"), "avg alias 'avg_age' 포함되어야 함");

        Assertions.assertThat(FormatUtil.normalizeWhitespace(code)).isEqualTo(
                "df = spark.table('patientinfo').groupBy(F.col(\"disease_code\")) df = df.agg( F.count(F.lit(\"*\")).alias(\"cnt\"), F.avg(F.col(\"age\")).alias(\"avg_age\") )"
        );
    }

    @Test
    @DisplayName("filter - between 테스트")
    public void testFilter_betweenNode_printsAndAsserts() throws Exception {
        String json = toNodeJson("{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"input\": \"spark.table('patientinfo')\",\n"
                + "      \"node\": \"filter\",\n"
                + "      \"condition\": {\n"
                + "        \"type\": \"between\",\n"
                + "        \"expr\": { \"type\": \"col\", \"name\": \"age\" },\n"
                + "        \"low\": { \"type\": \"lit\", \"value\": 30 },\n"
                + "        \"high\": { \"type\": \"lit\", \"value\": 40 }\n"
                + "      },\n"
                + "      \"output\": \"filtered\"\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String code = generate(json);

        assertTrue(code.contains("spark.table('patientinfo')") || code.contains("spark.table(\"patientinfo\")"),
                "patientinfo 테이블 참조(spark.table(...))가 포함되어야 함");
        assertTrue(code.contains(".filter("), "filter 호출이 포함되어야 함");
        assertTrue(code.contains(".between("), "between 메서드 호출이 포함되어야 함");
        assertTrue(code.contains("F.col(\"age\")"), "age 컬럼 참조(F.col(\"age\"))가 포함되어야 함");
        assertTrue(code.contains("F.lit(30)") || code.contains("F.lit(30.0)"), "하한 30 리터럴이 포함되어야 함");
        assertTrue(code.contains("F.lit(40)") || code.contains("F.lit(40.0)"), "상한 40 리터럴이 포함되어야 함");
    }

    @Test
    @DisplayName("filter - not between 테스트")
    public void testFilter_notBetweenNode_printsAndAsserts() throws Exception {
        String json = toNodeJson("{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"input\": \"spark.table('patientinfo')\",\n"
                + "      \"node\": \"filter\",\n"
                + "      \"condition\": {\n"
                + "        \"type\": \"between\",\n"
                + "        \"expr\": {\n"
                + "          \"type\": \"between\",\n"
                + "          \"expr\": { \"type\": \"col\", \"name\": \"age\" },\n"
                + "          \"low\": { \"type\": \"lit\", \"value\": 30 },\n"
                + "          \"high\": { \"type\": \"lit\", \"value\": 40 },\n"
                + "          \"not\": true\n"
                + "        },\n"
                + "        \"low\": { \"type\": \"lit\", \"value\": 20 },\n"
                + "        \"high\": { \"type\": \"lit\", \"value\": 50 }\n"
                + "      },\n"
                + "      \"output\": \"filtered\"\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String code = generate(json);

        assertTrue(code.contains("spark.table('patientinfo')") || code.contains("spark.table(\"patientinfo\")"),
                "patientinfo 테이블 참조(spark.table(...))가 포함되어야 함");
        assertTrue(code.contains(".filter("), "filter 호출이 포함되어야 함");
        assertTrue(code.contains(".between("), "between 메서드 호출이 포함되어야 함");
        assertTrue(code.contains("F.col(\"age\")"), "age 컬럼 참조(F.col(\"age\"))가 포함되어야 함");
        assertTrue(code.contains("F.lit(30)") || code.contains("F.lit(30.0)"), "하한 30 리터럴이 포함되어야 함");
        assertTrue(code.contains("F.lit(40)") || code.contains("F.lit(40.0)"), "상한 40 리터럴이 포함되어야 함");
        assertTrue(code.contains("~"), "not 연산자(~)가 포함되어야 함");
    }

    @Test
    @DisplayName("select - case when 테스트")
    public void testSelect_caseWhen_printsAndAsserts() throws Exception {
        String json = toNodeJson("{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"input\": \"spark.table('patientinfo')\",\n"
                + "      \"node\": \"select\",\n"
                + "      \"columns\": [\n"
                + "        {\n"
                + "          \"expr\": {\n"
                + "            \"type\": \"case\",\n"
                + "            \"when\": [\n"
                + "              {\n"
                + "                \"if\": {\n"
                + "                  \"type\": \"op\",\n"
                + "                  \"op\": \"<\",\n"
                + "                  \"left\": {\n"
                + "                    \"type\": \"col\",\n"
                + "                    \"name\": \"age\"\n"
                + "                  },\n"
                + "                  \"right\": {\n"
                + "                    \"type\": \"lit\",\n"
                + "                    \"value\": 30\n"
                + "                  }\n"
                + "                },\n"
                + "                \"then\": {\n"
                + "                  \"type\": \"lit\",\n"
                + "                  \"value\": \"YOUNG\"\n"
                + "                }\n"
                + "              },\n"
                + "              {\n"
                + "                \"if\": {\n"
                + "                  \"type\": \"op\",\n"
                + "                  \"op\": \"<\",\n"
                + "                  \"left\": {\n"
                + "                    \"type\": \"col\",\n"
                + "                    \"name\": \"age\"\n"
                + "                  },\n"
                + "                  \"right\": {\n"
                + "                    \"type\": \"lit\",\n"
                + "                    \"value\": 60\n"
                + "                  }\n"
                + "                },\n"
                + "                \"then\": {\n"
                + "                  \"type\": \"lit\",\n"
                + "                  \"value\": \"MIDDLE\"\n"
                + "                }\n"
                + "              }\n"
                + "            ],\n"
                + "            \"else\": {\n"
                + "              \"type\": \"lit\",\n"
                + "              \"value\": \"OLD\"\n"
                + "            }\n"
                + "          },\n"
                + "          \"alias\": \"age_group\"\n"
                + "        }\n"
                + "      ],\n"
                + "      \"output\": \"selected\"\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String code = generate(json);

        assertTrue(code.contains("spark.table('patientinfo')") || code.contains("spark.table(\"patientinfo\")"),
                "patientinfo 테이블 참조(spark.table(...))가 포함되어야 함");
        assertTrue(code.contains(".select("), "select 호출이 포함되어야 함");
        assertTrue(code.contains("F.when("), "CASE WHEN 구문 변환(F.when(...))이 포함되어야 함");
        assertTrue(code.contains(".otherwise("), "CASE WHEN 구문에 otherwise(...)가 포함되어야 함");
        assertTrue(code.contains(".alias(\"age_group\")") || code.contains(".alias('age_group')"), "별칭 .alias(\"age_group\")가 포함되어야 함");
        assertTrue(code.contains("F.col(\"age\")") || code.contains("F.col('age')"), "age 컬럼 참조(F.col(\"age\"))가 포함되어야 함");
        assertTrue(code.contains("F.lit(30)") || code.contains("F.lit(30.0)"), "then 분기 조건의 리터럴 30이 포함되어야 함");
        assertTrue(code.contains("F.lit(60)") || code.contains("F.lit(60.0)"), "두번째 분기 조건의 리터럴 60이 포함되어야 함");
    }

    @Test
    @DisplayName("테이블 추출: 단순 input + join")
    void testSimpleTableExtraction() throws Exception {
        String json = toNodeJson("{\n"
                + "  \"input\": \"main_df\",\n"
                + "  \"steps\": [\n"
                + "    { \"input\": \"main_df\", \"node\": \"join\", \"right\": \"orders_df\", \"output\": \"joined\" }\n"
                + "  ]\n"
                + "}";

        Set<String> expected = new HashSet<>(Arrays.asList("main_df", "orders_df", "joined"));
        Set<String> actual = PySparkChainGenerator.extractTables(json);

        TestUtil.printTestInfo("testSimpleTableExtraction", json, actual.toString());
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("테이블 추출: 재귀적 join (중첩 sub-JSON)")
    void testRecursiveTableExtraction() throws Exception {
        String json = toNodeJson("{\n"
                + "  \"input\": \"users_df\",\n"
                + "  \"steps\": [\n"
                + "    { \"input\": \"users_df\", \"node\": \"join\", \"right\": { \"input\": \"profiles_df\", \"steps\": [ { \"input\": \"profiles_df\", \"node\": \"join\", \"right\": \"addresses_df\", \"output\": \"profiles_joined\" } ], \"output\": \"profiles_with_addresses\" }, \"output\": \"users_joined\" }\n"
                + "  ]\n"
                + "}";

        Set<String> expected = new HashSet<>(Arrays.asList("users_df", "profiles_df", "addresses_df", "profiles_joined", "profiles_with_addresses", "users_joined"));
        Set<String> actual = PySparkChainGenerator.extractTables(json);

        TestUtil.printTestInfo("testRecursiveTableExtraction", json, actual.toString());
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("테이블 추출: 기본 input만 (join 없음)")
    void testInputOnlyExtraction() throws Exception {
        String json = toNodeJson("{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [ { \"input\": \"df\", \"node\": \"filter\", \"condition\": { \"type\": \"op\", \"op\": \">\", \"left\": { \"type\": \"col\", \"name\": \"age\" }, \"right\": { \"type\": \"lit\", \"value\": 18 } }, \"output\": \"filtered\" } ]\n"
                + "}";

        Set<String> expected = new HashSet<>(Arrays.asList("df", "filtered"));
        Set<String> actual = PySparkChainGenerator.extractTables(json);

        TestUtil.printTestInfo("testInputOnlyExtraction", json, actual.toString());
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("extractTables - load(iceberg) 파이프라인")
    public void testExtractTables_loadIceberg_containsFullyQualifiedTable() throws Exception {
        String json = toNodeJson("{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"load\",\n"
                + "      \"source\": \"iceberg\",\n"
                + "      \"catalog\": \"hadoop\",\n"
                + "      \"database\": \"curated\",\n"
                + "      \"table\": \"patients\",\n"
                + "      \"output\": \"loaded\"\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        Set<String> tables = PySparkChainGenerator.extractTables(json);

        TestUtil.printTestInfo("extractTables - load(iceberg) 파이프라인", json, String.join(", ", tables));
        Assertions.assertThat(tables)
                .contains("df", "hadoop.curated.patients", "loaded");
    }

    @Test
    @DisplayName("extractTables - load(postgres) 파이프라인")
    public void testExtractTables_loadPostgres_containsDeclaredTables() throws Exception {
        String json = toNodeJson("{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"load\",\n"
                + "      \"source\": \"postgres\",\n"
                + "      \"table\": \"public.patient\",\n"
                + "      \"options\": {\n"
                + "        \"dbtable\": \"staging.patient_snapshot\"\n"
                + "      },\n"
                + "      \"output\": \"loaded\"\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        Set<String> tables = PySparkChainGenerator.extractTables(json);

        TestUtil.printTestInfo("extractTables - load(postgres) 파이프라인", json, String.join(", ", tables));
        Assertions.assertThat(tables)
                .contains("df", "public.patient", "staging.patient_snapshot", "loaded");
    }

}
