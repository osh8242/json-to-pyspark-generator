package com.douzone.platform.recipe;

import com.douzone.platform.recipe.util.TestUtil;
import com.douzone.platform.util.FormatUtil;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * description    :
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2025. 9. 16.        osh8242       최초 생성
 */
public class PySparkChainGeneratorTest {

    @Test
    @DisplayName("load - iceberg source")
    public void testLoadIceberg_setsBaseExpression() throws Exception {
        String json = "{\n" +
                "  \"input\": \"df\",\n" +
                "  \"steps\": [\n" +
                "    {\n" +
                "      \"step\": \"load\",\n" +
                "      \"source\": \"iceberg\",\n" +
                "      \"catalog\": \"dev\",\n" +
                "      \"database\": \"sftp-60106\",\n" +
                "      \"table\": \"orders\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"step\": \"select\",\n" +
                "      \"columns\": [\n" +
                "        { \"expr\": { \"type\": \"col\", \"name\": \"order_id\" } }\n" +
                "      ]\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        String out = PySparkChainGenerator.generate(json);

        Assertions.assertThat(out).contains("spark.read.table(\"dev.sftp-60106.orders\")");
        Assertions.assertThat(out).contains(".select(");
    }

    @Test
    @DisplayName("load - postgres source")
    public void testLoadPostgres_setsJdbcOptions() throws Exception {
        String json = "{\n" +
                "  \"input\": \"df\",\n" +
                "  \"steps\": [\n" +
                "    {\n" +
                "      \"step\": \"load\",\n" +
                "      \"source\": \"postgres\",\n" +
                "      \"host\": \"localhost\",\n" +
                "      \"port\": \"5432\",\n" +
                "      \"database\": \"sample\",\n" +
                "      \"table\": \"public.orders\",\n" +
                "      \"user\": \"app\",\n" +
                "      \"password\": \"secret\",\n" +
                "      \"driver\": \"org.postgresql.Driver\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"step\": \"limit\",\n" +
                "      \"n\": 10\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        String out = PySparkChainGenerator.generate(json);

        Assertions.assertThat(out).contains("spark.read.format(\"jdbc\")");
        Assertions.assertThat(out).contains(".option(\"url\", \"jdbc:postgresql://localhost:5432/sample\")");
        Assertions.assertThat(out).contains(".option(\"dbtable\", \"public.orders\")");
        Assertions.assertThat(out).contains(".option(\"user\", \"app\")");
        Assertions.assertThat(out).contains(".option(\"password\", \"secret\")");
        Assertions.assertThat(out).contains(".option(\"driver\", \"org.postgresql.Driver\")");
        Assertions.assertThat(out).contains(".limit(10)");
    }

    @Test
    public void testBasicPipeline_containsExpectedFragments() throws Exception {
        String json = "{\n" +
                "  \"input\": \"df\",\n" +
                "  \"steps\": [\n" +
                "    {\n" +
                "      \"step\": \"filter\",\n" +
                "      \"condition\": {\n" +
                "        \"type\": \"op\",\n" +
                "        \"op\": \"and\",\n" +
                "        \"left\": {\n" +
                "          \"type\": \"op\",\n" +
                "          \"op\": \">=\",\n" +
                "          \"left\": {\"type\": \"col\", \"name\": \"age\"},\n" +
                "          \"right\": {\"type\": \"lit\", \"value\": 65}\n" +
                "        },\n" +
                "        \"right\": {\n" +
                "          \"type\": \"op\",\n" +
                "          \"op\": \"=\",\n" +
                "          \"left\": {\"type\": \"col\", \"name\": \"gender\"},\n" +
                "          \"right\": {\"type\": \"lit\", \"value\": \"F\"}\n" +
                "        }\n" +
                "      }\n" +
                "    },\n" +
                "    { \"step\": \"groupBy\", \"keys\": [ { \"type\": \"col\", \"name\": \"disease_code\" } ] },\n" +
                "    { \"step\": \"agg\", \"aggs\": [ { \"expr\": { \"type\": \"func\", \"name\": \"count\", \"args\": [ { \"type\": \"col\", \"name\": \"*\" } ] }, \"alias\": \"cnt\" } ] },\n" +
                "    { \"step\": \"orderBy\", \"keys\": [ { \"expr\": { \"type\": \"col\", \"name\": \"cnt\" }, \"asc\": false } ] },\n" +
                "    { \"step\": \"limit\", \"n\": 100 }\n" +
                "  ]\n" +
                "}";
        System.out.println("json = " + json);
        System.out.println();
        String out = PySparkChainGenerator.generate(json);
        System.out.println("result_df = " + out);

        // header 확인
        assertTrue(out.contains("from pyspark.sql import functions as F"), "헤더가 포함되어야 함");

        // filter 표현 확인 (컬럼/리터럴)
        assertTrue(out.contains("F.col(\"age\")"), "age 컬럼 참조가 포함되어야 함");
        assertTrue(out.contains("F.lit(65)"), "65 리터럴이 포함되어야 함");
        assertTrue(out.contains("F.col(\"gender\")"), "gender 컬럼 참조가 포함되어야 함");
        assertTrue(out.contains("F.lit(\"F\")"), "문자열 'F' 리터럴이 포함되어야 함");

        // groupBy / agg / alias / orderBy / limit
        assertTrue(out.contains(".groupBy("), "groupBy 호출이 포함되어야 함");
        assertTrue(out.contains("F.count("), "count 집계가 포함되어야 함");
        assertTrue(out.contains(".alias(\"cnt\")"), "agg alias가 포함되어야 함");
        assertTrue(out.contains(".orderBy("), "orderBy 호출이 포함되어야 함");
        assertTrue(out.contains(".limit(100)"), "limit(100) 포함되어야 함");
    }

    @Test
    @DisplayName("withColumn, withColumns 테스트")
    public void testWithColumn_and_withColumns_containsExpectedFragments() throws Exception {
        String json = "{\n" +
                "  \"input\": \"df\",\n" +
                "  \"steps\": [\n" +
                "    {\n" +
                "      \"step\": \"withColumn\",\n" +
                "      \"name\": \"age_plus_one\",\n" +
                "      \"expr\": {\n" +
                "        \"type\": \"op\",\n" +
                "        \"op\": \"+\",\n" +
                "        \"left\": {\"type\": \"col\", \"name\": \"age\"},\n" +
                "        \"right\": {\"type\": \"lit\", \"value\": 1}\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"step\": \"withColumns\",\n" +
                "      \"cols\": {\n" +
                "        \"is_senior\": { \"type\": \"op\", \"op\": \">=\", \"left\": {\"type\":\"col\",\"name\":\"age\"}, \"right\": {\"type\":\"lit\",\"value\":65} },\n" +
                "        \"gender_flag\": {\n" +
                "          \"type\": \"case\",\n" +
                "          \"when\": [\n" +
                "            { \"if\": { \"type\": \"op\", \"op\": \"=\", \"left\": {\"type\":\"col\",\"name\":\"gender\"}, \"right\": {\"type\":\"lit\",\"value\":\"F\"} }, \"then\": {\"type\":\"lit\",\"value\":1} }\n" +
                "          ],\n" +
                "          \"else\": {\"type\":\"lit\",\"value\":0}\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  ]\n" +
                "}";
        System.out.println("json = " + json);
        System.out.println();
        String out = PySparkChainGenerator.generate(json);
        System.out.println("result_df = " + out);

        // withColumn 확인
        assertTrue(out.contains(".withColumn(\"age_plus_one\""), "withColumn 이름이 포함되어야 함");
        assertTrue(out.contains("F.col(\"age\")"), "age 컬럼 참조 포함");
        assertTrue(out.contains("F.lit(1)"), "숫자 리터럴 포함");

        // withColumns map 확인 (키와 값 표현)
        assertTrue(out.contains("\"is_senior\""), "withColumns 키 is_senior 포함");
        assertTrue(out.contains("\"gender_flag\""), "withColumns 키 gender_flag 포함");
        assertTrue(out.contains(".when(") || out.contains("F.when("), "CASE/when 표현 포함 (when 존재 여부 확인)");
    }

    @Test
    @DisplayName("알 수 없는 step 은 기본 메서드 체인으로 처리")
    void testUnknownStepFallsBackToDefaultBuilder() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    { \"step\": \"cache\" },\n"
                + "    { \"step\": \"persist\", \"args\": [ { \"type\": \"lit\", \"value\": \"MEMORY_ONLY\" } ] },\n"
                + "    { \"step\": \"count\" }\n"
                + "  ]\n"
                + "}";

        String out = PySparkChainGenerator.generate(json);

        Assertions.assertThat(out).contains(".cache()");
        Assertions.assertThat(out).contains(".persist(F.lit(\"MEMORY_ONLY\"))");
        Assertions.assertThat(out).contains(".count()");
    }

    @Test
    @DisplayName("join 테스트")
    public void testJoin_withAliases_containsExpectedFragments() throws Exception {
        String json = "{\n" +
                "  \"input\": \"df\",\n" +
                "  \"steps\": [\n" +
                "    {\n" +
                "      \"step\": \"join\",\n" +
                "      \"right\": \"other\",\n" +
                "      \"rightAlias\": \"b\",\n" +
                "      \"leftAlias\": \"a\",\n" +
                "      \"on\": [\n" +
                "        {\n" +
                "          \"type\": \"op\",\n" +
                "          \"op\": \"=\",\n" +
                "          \"left\": {\n" +
                "            \"type\": \"col\",\n" +
                "            \"name\": \"id\",\n" +
                "            \"alias\": \"a\"\n" +
                "          },\n" +
                "          \"right\": {\n" +
                "            \"type\": \"col\",\n" +
                "            \"name\": \"id\",\n" +
                "            \"alias\": \"b\"\n" +
                "          }\n" +
                "        }\n" +
                "      ],\n" +
                "      \"how\": \"left\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"step\": \"select\",\n" +
                "      \"columns\": [\n" +
                "        {\n" +
                "          \"as\": \"id\",\n" +
                "          \"expr\": {\n" +
                "            \"type\": \"col\",\n" +
                "            \"name\": \"id\",\n" +
                "            \"table\": \"a\"\n" +
                "          }\n" +
                "        },\n" +
                "        {\n" +
                "          \"as\": \"other_val\",\n" +
                "          \"expr\": {\n" +
                "            \"type\": \"col\",\n" +
                "            \"name\": \"val\",\n" +
                "            \"table\": \"b\"\n" +
                "          }\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        System.out.println("json = " + json);
        System.out.println();

        String out = PySparkChainGenerator.generate(json);
        System.out.println("result_df = " + out);

    }

    @Test
    @DisplayName("filter - and 조건 테스트")
    public void testPatientInfoFilter_simpleAndCondition_printsAndAsserts() throws Exception {
        String json = "{\n" +
                "  \"input\": \"spark.table('patientinfo')\",\n" +
                "  \"steps\": [\n" +
                "    {\n" +
                "      \"step\": \"filter\",\n" +
                "      \"condition\": {\n" +
                "        \"type\": \"op\",\n" +
                "        \"op\": \"and\",\n" +
                "        \"left\": {\n" +
                "          \"type\": \"op\",\n" +
                "          \"op\": \">=\",\n" +
                "          \"left\": { \"type\": \"col\", \"name\": \"age\" },\n" +
                "          \"right\": { \"type\": \"lit\", \"value\": 65 }\n" +
                "        },\n" +
                "        \"right\": {\n" +
                "          \"type\": \"op\",\n" +
                "          \"op\": \"=\",\n" +
                "          \"left\": { \"type\": \"col\", \"name\": \"gender\" },\n" +
                "          \"right\": { \"type\": \"lit\", \"value\": \"F\" }\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        // 입력 JSON 출력 (디버그용)
        System.out.println("json = " + json);
        System.out.println();

        // 생성된 PySpark 코드 출력
        String out = PySparkChainGenerator.generate(json);
        System.out.println("result_df = " + out);

        // 핵심 포함 여부 검증
        assertTrue(out.contains("spark.table('patientinfo')") || out.contains("spark.table(\"patientinfo\")"),
                "입력 테이블 patientinfo가 spark.table(...) 형태로 포함되어야 함");
        assertTrue(out.contains(".filter("), "filter 호출이 포함되어야 함");
        assertTrue(out.contains("F.col(\"age\")"), "age 컬럼 참조가 포함되어야 함");
        assertTrue(out.contains("F.lit(65)"), "숫자 리터럴 65가 포함되어야 함");
        assertTrue(out.contains("F.col(\"gender\")"), "gender 컬럼 참조가 포함되어야 함");
        assertTrue(out.contains("F.lit(\"F\")"), "문자열 'F' 리터럴이 포함되어야 함");
        assertTrue(out.contains("&") || out.contains(" & "), "AND 연산이 & 로 변환되어 포함되어야 함");
    }

    @Test
    @DisplayName("groupBy - agg 테스트")
    public void testGroupBy_patientinfo_printsAndAsserts() throws Exception {
        String json = "{\n" +
                "  \"input\": \"spark.table('patientinfo')\",\n" +
                "  \"steps\": [\n" +
                "    {\n" +
                "      \"step\": \"groupBy\",\n" +
                "      \"keys\": [\n" +
                "        { \"type\": \"col\", \"name\": \"disease_code\" }\n" +
                "      ]\n" +
                "    },\n" +
                "    {\n" +
                "      \"step\": \"agg\",\n" +
                "      \"aggs\": [\n" +
                "        {\n" +
                "          \"expr\": { \"type\": \"func\", \"name\": \"count\", \"args\": [ { \"type\": \"lit\", \"value\": \"*\" } ] },\n" +
                "          \"alias\": \"cnt\"\n" +
                "        },\n" +
                "        {\n" +
                "          \"expr\": { \"type\": \"func\", \"name\": \"avg\", \"args\": [ { \"type\": \"col\", \"name\": \"age\" } ] },\n" +
                "          \"alias\": \"avg_age\"\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        // 입력 JSON 출력 (디버그용)
        System.out.println("json = " + json);
        System.out.println();

        // PySpark 코드 생성 및 출력
        String out = PySparkChainGenerator.generate(json);
        System.out.println("***********************************************************************");
        System.out.println(out);

        // 핵심 포함 여부 검증
        assertTrue(out.contains("spark.table('patientinfo')") || out.contains("spark.table(\"patientinfo\")"),
                "입력 테이블 patientinfo가 spark.table(...) 형태로 포함되어야 함");
        assertTrue(out.contains(".groupBy("), "groupBy 호출이 포함되어야 함");
        assertTrue(out.contains("F.count("), "count 집계 함수가 포함되어야 함");
        assertTrue(out.contains(".alias(\"cnt\")"), "count alias 'cnt' 포함되어야 함");
        assertTrue(out.contains("F.avg(") && out.contains("F.col(\"age\")"), "avg(age) 표현이 포함되어야 함");
        assertTrue(out.contains(".alias(\"avg_age\")"), "avg alias 'avg_age' 포함되어야 함");
        Assertions.assertThat(FormatUtil.normalizeWhitespace(out)).isEqualTo(
                "from pyspark.sql import functions as F result_df = ( spark.table('patientinfo') .groupBy(F.col(\"disease_code\")) .agg( F.count(F.lit(\"*\")).alias(\"cnt\"), F.avg(F.col(\"age\")).alias(\"avg_age\") ) )"
        );
    }

    @Test
    @DisplayName("filter - between 테스트")
    public void testFilter_betweenNode_printsAndAsserts() throws Exception {
        String json = "{\n" +
                "  \"input\": \"spark.table('patientinfo')\",\n" +
                "  \"steps\": [\n" +
                "    {\n" +
                "      \"step\": \"filter\",\n" +
                "      \"condition\": {\n" +
                "        \"type\": \"between\",\n" +
                "        \"expr\": { \"type\": \"col\", \"name\": \"age\" },\n" +
                "        \"low\": { \"type\": \"lit\", \"value\": 30 },\n" +
                "        \"high\": { \"type\": \"lit\", \"value\": 40 }\n" +
                "      }\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        // 입력 JSON 출력 (디버그)
        System.out.println("json = " + json);
        System.out.println();

        // PySpark 코드 생성 및 출력
        String out = PySparkChainGenerator.generate(json);
        System.out.println("result_df = " + out);

        // 포함 검증
        assertTrue(out.contains("spark.table('patientinfo')") || out.contains("spark.table(\"patientinfo\")"),
                "patientinfo 테이블 참조(spark.table(...))가 포함되어야 함");
        assertTrue(out.contains(".filter("), "filter 호출이 포함되어야 함");
        assertTrue(out.contains(".between(") || out.contains(".between ("), "between 메서드 호출이 포함되어야 함");
        assertTrue(out.contains("F.col(\"age\")"), "age 컬럼 참조(F.col(\"age\"))가 포함되어야 함");
        assertTrue(out.contains("F.lit(30)") || out.contains("F.lit(30.0)"), "하한 30 리터럴이 포함되어야 함");
        assertTrue(out.contains("F.lit(40)") || out.contains("F.lit(40.0)"), "상한 40 리터럴이 포함되어야 함");
    }

    @Test
    @DisplayName("filter - not between 테스트")
    public void testFilter_notBetweenNode_printsAndAsserts() throws Exception {
        String json = "{\n" +
                "  \"input\": \"spark.table('patientinfo')\",\n" +
                "  \"steps\": [\n" +
                "    {\n" +
                "      \"step\": \"filter\",\n" +
                "      \"condition\": {\n" +
                "        \"type\": \"between\",\n" +
                "        \"expr\": {\n" +
                "          \"type\": \"between\",\n" +
                "          \"expr\": { \"type\": \"col\", \"name\": \"age\" },\n" +
                "          \"low\": { \"type\": \"lit\", \"value\": 30 },\n" +
                "          \"high\": { \"type\": \"lit\", \"value\": 40 },\n" +
                "          \"not\": true\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        // 입력 JSON 출력 (디버그)
        System.out.println("json = " + json);
        System.out.println();

        // PySpark 코드 생성 및 출력
        String out = PySparkChainGenerator.generate(json);
        System.out.println("result_df = " + out);

        // 포함 검증
        assertTrue(out.contains("spark.table('patientinfo')") || out.contains("spark.table(\"patientinfo\")"),
                "patientinfo 테이블 참조(spark.table(...))가 포함되어야 함");
        assertTrue(out.contains(".filter("), "filter 호출이 포함되어야 함");
        assertTrue(out.contains(".between("), "between 메서드 호출이 포함되어야 함");
        assertTrue(out.contains("F.col(\"age\")"), "age 컬럼 참조(F.col(\"age\"))가 포함되어야 함");
        assertTrue(out.contains("F.lit(30)") || out.contains("F.lit(30.0)"), "하한 30 리터럴이 포함되어야 함");
        assertTrue(out.contains("F.lit(40)") || out.contains("F.lit(40.0)"), "상한 40 리터럴이 포함되어야 함");
        assertTrue(out.contains("~"), "not 연산자(~)가 포함되어야 함");
    }

    @Test
    @DisplayName("select - case when 테스트")
    public void testSelect_caseWhen_printsAndAsserts() throws Exception {
        String json = "{\n" +
                "  \"input\": \"spark.table('patientinfo')\",\n" +
                "  \"steps\": [\n" +
                "    {\n" +
                "      \"step\": \"select\",\n" +
                "      \"columns\": [\n" +
                "        {\n" +
                "          \"expr\": {\n" +
                "            \"type\": \"case\",\n" +
                "            \"when\": [\n" +
                "              {\n" +
                "                \"if\": {\n" +
                "                  \"type\": \"op\",\n" +
                "                  \"op\": \"<\",\n" +
                "                  \"left\": {\n" +
                "                    \"type\": \"col\",\n" +
                "                    \"name\": \"age\"\n" +
                "                  },\n" +
                "                  \"right\": {\n" +
                "                    \"type\": \"lit\",\n" +
                "                    \"value\": 30\n" +
                "                  }\n" +
                "                },\n" +
                "                \"then\": {\n" +
                "                  \"type\": \"lit\",\n" +
                "                  \"value\": \"YOUNG\"\n" +
                "                }\n" +
                "              },\n" +
                "              {\n" +
                "                \"if\": {\n" +
                "                  \"type\": \"op\",\n" +
                "                  \"op\": \"<\",\n" +
                "                  \"left\": {\n" +
                "                    \"type\": \"col\",\n" +
                "                    \"name\": \"age\"\n" +
                "                  },\n" +
                "                  \"right\": {\n" +
                "                    \"type\": \"lit\",\n" +
                "                    \"value\": 60\n" +
                "                  }\n" +
                "                },\n" +
                "                \"then\": {\n" +
                "                  \"type\": \"lit\",\n" +
                "                  \"value\": \"MIDDLE\"\n" +
                "                }\n" +
                "              }\n" +
                "            ],\n" +
                "            \"else\": {\n" +
                "              \"type\": \"lit\",\n" +
                "              \"value\": \"OLD\"\n" +
                "            }\n" +
                "          },\n" +
                "          \"alias\": \"age_group\"\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        // 입력 JSON 출력 (디버그)
        System.out.println("json = " + json);
        System.out.println();

        // PySpark 코드 생성 및 출력
        String out = PySparkChainGenerator.generate(json);
        System.out.println("result_df = " + out);

        // 포함 검증
        assertTrue(out.contains("spark.table('patientinfo')") || out.contains("spark.table(\"patientinfo\")"),
                "patientinfo 테이블 참조(spark.table(...))가 포함되어야 함");
        assertTrue(out.contains(".select("), "select 호출이 포함되어야 함");
        assertTrue(out.contains("F.when("), "CASE WHEN 구문 변환(F.when(...))이 포함되어야 함");
        assertTrue(out.contains(".otherwise("), "CASE WHEN 구문에 otherwise(...)가 포함되어야 함");
        // alias 확인: buildSelect은 .alias(...) 형태로 생성
        assertTrue(out.contains(".alias(\"age_group\")") || out.contains(".alias('age_group')"), "별칭 .alias(\"age_group\")가 포함되어야 함");

        // 비교 조건 확인: age 비교 표현 포함
        assertTrue(out.contains("F.col(\"age\")") || out.contains("F.col('age')"), "age 컬럼 참조(F.col(\"age\"))가 포함되어야 함");
        assertTrue(out.contains("F.lit(30)") || out.contains("F.lit(30.0)"), "then 분기 조건의 리터럴 30이 포함되어야 함");
        assertTrue(out.contains("F.lit(60)") || out.contains("F.lit(60.0)"), "두번째 분기 조건의 리터럴 60이 포함되어야 함");
    }

    @Test
    @DisplayName("테이블 추출: 단순 input + join")
    void testSimpleTableExtraction() throws Exception {
        String json = "{\n"
                + "  \"input\": \"main_df\",\n"
                + "  \"steps\": [\n"
                + "    { \"step\": \"join\", \"right\": \"orders_df\" }\n"
                + "  ]\n"
                + "}";

        Set<String> expected = new HashSet<>(Arrays.asList("main_df", "orders_df"));
        Set<String> actual = PySparkChainGenerator.extractTables(json);

        TestUtil.printTestInfo("testSimpleTableExtraction", json, actual.toString());
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("테이블 추출: 재귀적 join (중첩 sub-JSON)")
    void testRecursiveTableExtraction() throws Exception {
        String json = "{\n"
                + "  \"input\": \"users_df\",\n"
                + "  \"steps\": [\n"
                + "    { \"step\": \"join\", \"right\": { \"input\": \"profiles_df\", \"steps\": [ { \"step\": \"join\", \"right\": \"addresses_df\" } ] } }\n"
                + "  ]\n"
                + "}";

        Set<String> expected = new HashSet<>(Arrays.asList("users_df", "profiles_df", "addresses_df"));
        Set<String> actual = PySparkChainGenerator.extractTables(json);

        TestUtil.printTestInfo("testRecursiveTableExtraction", json, actual.toString());
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("테이블 추출: 기본 input만 (join 없음)")
    void testInputOnlyExtraction() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [ { \"step\": \"filter\", \"condition\": { \"type\": \"op\", \"op\": \">\", \"left\": { \"type\": \"col\", \"name\": \"age\" }, \"right\": { \"type\": \"lit\", \"value\": 18 } } } ]\n"
                + "}";

        Set<String> expected = new HashSet<>(Collections.singletonList("df"));
        Set<String> actual = PySparkChainGenerator.extractTables(json);

        TestUtil.printTestInfo("testInputOnlyExtraction", json, actual.toString());
        assertEquals(expected, actual);
    }

}

