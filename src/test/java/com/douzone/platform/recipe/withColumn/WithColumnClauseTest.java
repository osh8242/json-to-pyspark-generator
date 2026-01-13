package com.douzone.platform.recipe.withcolumn;

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
public class WithColumnClauseTest {

    @Test
    @DisplayName("withColumn: ptno 문자열 자르기(substring) 컬럼 추가")
    void testWithColumnSubstringPtno() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"withColumn\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df_out\",\n"
                + "      \"params\": {\n"
                + "        \"name\": \"ptno_sub\",\n"
                + "        \"expr\": {\n"
                + "          \"type\": \"func\",\n"
                + "          \"name\": \"substring\",\n"
                + "          \"args\": [\n"
                + "            { \"type\": \"col\", \"name\": \"ptno\" },\n"
                + "            { \"type\": \"lit\", \"value\": 1 },\n"
                + "            { \"type\": \"lit\", \"value\": 6 }\n"
                + "          ]\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected = "df_out = df.withColumn(\"ptno_sub\", F.substring(F.col(\"ptno\"), 1, 6))\n";
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testWithColumnSubstringPtno", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("withColumn: btdt 기준 일자차이(datediff) 컬럼 추가")
    void testWithColumnDateDiff() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"withColumn\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df_out\",\n"
                + "      \"params\": {\n"
                + "        \"name\": \"days_from_birth\",\n"
                + "        \"expr\": {\n"
                + "          \"type\": \"func\",\n"
                + "          \"name\": \"datediff\",\n"
                + "          \"args\": [\n"
                + "            { \"type\": \"func\", \"name\": \"current_date\" },\n"
                + "            { \"type\": \"col\", \"name\": \"btdt\" }\n"
                + "          ]\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected = "df_out = df.withColumn(\"days_from_birth\", F.datediff(F.current_date(), F.col(\"btdt\")))\n";
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testWithColumnDateDiff", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("withColumn: btdt 기준 월차이(months_between) 컬럼 추가")
    void testWithColumnMonthsDiff() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"withColumn\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df_out\",\n"
                + "      \"params\": {\n"
                + "        \"name\": \"months_from_birth\",\n"
                + "        \"expr\": {\n"
                + "          \"type\": \"func\",\n"
                + "          \"name\": \"months_between\",\n"
                + "          \"args\": [\n"
                + "            { \"type\": \"func\", \"name\": \"current_date\" },\n"
                + "            { \"type\": \"col\", \"name\": \"btdt\" }\n"
                + "          ]\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected = "df_out = df.withColumn(\"months_from_birth\", F.months_between(F.current_date(), F.col(\"btdt\")))\n";
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testWithColumnMonthsDiff", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("withColumn: btdt 기준 연도차이(year(current_date) - year(btdt)) 컬럼 추가")
    void testWithColumnYearsDiff() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"withColumn\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df_out\",\n"
                + "      \"params\": {\n"
                + "        \"name\": \"years_from_birth\",\n"
                + "        \"expr\": {\n"
                + "          \"type\": \"op\",\n"
                + "          \"op\": \"-\",\n"
                + "          \"left\": {\n"
                + "            \"type\": \"func\",\n"
                + "            \"name\": \"year\",\n"
                + "            \"args\": [ { \"type\": \"func\", \"name\": \"current_date\" } ]\n"
                + "          },\n"
                + "          \"right\": {\n"
                + "            \"type\": \"func\",\n"
                + "            \"name\": \"year\",\n"
                + "            \"args\": [ { \"type\": \"col\", \"name\": \"btdt\" } ]\n"
                + "          }\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected = "df_out = df.withColumn(\"years_from_birth\", (F.year(F.current_date()) - F.year(F.col(\"btdt\"))))\n";
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testWithColumnYearsDiff", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("withColumn: ptno + gend_cd 컬럼값 합치기(concat_ws, 구분자 포함) 컬럼 추가")
    void testWithColumnConcatWithDelimiter() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"withColumn\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df_out\",\n"
                + "      \"params\": {\n"
                + "        \"name\": \"ptno_gend\",\n"
                + "        \"expr\": {\n"
                + "          \"type\": \"func\",\n"
                + "          \"name\": \"concat_ws\",\n"
                + "          \"args\": [\n"
                + "            { \"type\": \"lit\", \"value\": \"-\" },\n"
                + "            { \"type\": \"col\", \"name\": \"ptno\" },\n"
                + "            { \"type\": \"col\", \"name\": \"gend_cd\" }\n"
                + "          ]\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected = "df_out = df.withColumn(\"ptno_gend\", F.concat_ws(\"-\", F.col(\"ptno\"), F.col(\"gend_cd\")))\n";
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testWithColumnConcatWithDelimiter", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("withColumn: 컬럼값에 문자열 추가(concat) - ptno에 접두어 추가")
    void testWithColumnAddStringPrefix() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"withColumn\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df_out\",\n"
                + "      \"params\": {\n"
                + "        \"name\": \"ptno_labeled\",\n"
                + "        \"expr\": {\n"
                + "          \"type\": \"func\",\n"
                + "          \"name\": \"concat\",\n"
                + "          \"args\": [\n"
                + "            { \"type\": \"lit\", \"value\": \"PTNO:\" },\n"
                + "            { \"type\": \"col\", \"name\": \"ptno\" }\n"
                + "          ]\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected = "df_out = df.withColumn(\"ptno_labeled\", F.concat(F.lit(\"PTNO:\"), F.col(\"ptno\")))\n";
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testWithColumnAddStringPrefix", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("withColumn: 사칙연산(+) - age_plus_2 = age + 2")
    void testWithColumnArithmeticAdd() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"withColumn\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df_out\",\n"
                + "      \"params\": {\n"
                + "        \"name\": \"age_plus_2\",\n"
                + "        \"expr\": {\n"
                + "          \"type\": \"op\",\n"
                + "          \"op\": \"+\",\n"
                + "          \"left\": { \"type\": \"col\", \"name\": \"age\" },\n"
                + "          \"right\": { \"type\": \"lit\", \"value\": 2 }\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected = "df_out = df.withColumn(\"age_plus_2\", (F.col(\"age\") + F.lit(2)))\n";
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testWithColumnArithmeticAdd", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("withColumn: 사칙연산(-) - age_minus_1 = age - 1")
    void testWithColumnArithmeticSubtract() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"withColumn\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df_out\",\n"
                + "      \"params\": {\n"
                + "        \"name\": \"age_minus_1\",\n"
                + "        \"expr\": {\n"
                + "          \"type\": \"op\",\n"
                + "          \"op\": \"-\",\n"
                + "          \"left\": { \"type\": \"col\", \"name\": \"age\" },\n"
                + "          \"right\": { \"type\": \"lit\", \"value\": 1 }\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected = "df_out = df.withColumn(\"age_minus_1\", (F.col(\"age\") - F.lit(1)))\n";
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testWithColumnArithmeticSubtract", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("withColumn: 사칙연산(*) - wt_x2 = wt * 2")
    void testWithColumnArithmeticMultiply() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"withColumn\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df_out\",\n"
                + "      \"params\": {\n"
                + "        \"name\": \"wt_x2\",\n"
                + "        \"expr\": {\n"
                + "          \"type\": \"op\",\n"
                + "          \"op\": \"*\",\n"
                + "          \"left\": { \"type\": \"col\", \"name\": \"wt\" },\n"
                + "          \"right\": { \"type\": \"lit\", \"value\": 2 }\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected = "df_out = df.withColumn(\"wt_x2\", (F.col(\"wt\") * F.lit(2)))\n";
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testWithColumnArithmeticMultiply", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("withColumn: 사칙연산(/) - wt_div_2 = wt / 2")
    void testWithColumnArithmeticDivide() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"withColumn\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df_out\",\n"
                + "      \"params\": {\n"
                + "        \"name\": \"wt_div_2\",\n"
                + "        \"expr\": {\n"
                + "          \"type\": \"op\",\n"
                + "          \"op\": \"/\",\n"
                + "          \"left\": { \"type\": \"col\", \"name\": \"wt\" },\n"
                + "          \"right\": { \"type\": \"lit\", \"value\": 2 }\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected = "df_out = df.withColumn(\"wt_div_2\", (F.col(\"wt\") / F.lit(2)))\n";
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testWithColumnArithmeticDivide", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("withColumn: 사칙연산(%) - age_mod_10 = age % 10")
    void testWithColumnArithmeticModulo() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"withColumn\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df_out\",\n"
                + "      \"params\": {\n"
                + "        \"name\": \"age_mod_10\",\n"
                + "        \"expr\": {\n"
                + "          \"type\": \"op\",\n"
                + "          \"op\": \"%\",\n"
                + "          \"left\": { \"type\": \"col\", \"name\": \"age\" },\n"
                + "          \"right\": { \"type\": \"lit\", \"value\": 10 }\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected = "df_out = df.withColumn(\"age_mod_10\", (F.col(\"age\") % F.lit(10)))\n";
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testWithColumnArithmeticModulo", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("withColumn: 사칙연산(혼합) - bmi_like = (wt / (ht*ht)) * 10000")
    void testWithColumnArithmeticMixed() throws Exception {
        // bmi 비슷한 계산: (wt / (ht*ht)) * 10000
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"withColumn\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df_out\",\n"
                + "      \"params\": {\n"
                + "        \"name\": \"bmi_like\",\n"
                + "        \"expr\": {\n"
                + "          \"type\": \"op\",\n"
                + "          \"op\": \"*\",\n"
                + "          \"left\": {\n"
                + "            \"type\": \"op\",\n"
                + "            \"op\": \"/\",\n"
                + "            \"left\": { \"type\": \"col\", \"name\": \"wt\" },\n"
                + "            \"right\": {\n"
                + "              \"type\": \"op\",\n"
                + "              \"op\": \"*\",\n"
                + "              \"left\": { \"type\": \"col\", \"name\": \"ht\" },\n"
                + "              \"right\": { \"type\": \"col\", \"name\": \"ht\" }\n"
                + "            }\n"
                + "          },\n"
                + "          \"right\": { \"type\": \"lit\", \"value\": 10000 }\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected = "df_out = df.withColumn(\"bmi_like\", ((F.col(\"wt\") / (F.col(\"ht\") * F.col(\"ht\"))) * F.lit(10000)))\n";
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testWithColumnArithmeticMixed", json, actual);
        assertEquals(expected, actual);
    }

    // ---------------------------
    // try_* 산술 함수 테스트들
    // (주의: 실제 PySpark 런타임에 F.try_divide 등이 존재해야 실행 가능.
    // 여기서는 "코드 생성 결과"만 검증)
    // ---------------------------

    @Test
    @DisplayName("withColumn: try_divide(col, lit) - safe_div = try_divide(wt, 2)")
    void testWithColumnTryDivideLiteral() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"withColumn\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df_out\",\n"
                + "      \"params\": {\n"
                + "        \"name\": \"safe_div\",\n"
                + "        \"expr\": {\n"
                + "          \"type\": \"func\",\n"
                + "          \"name\": \"try_divide\",\n"
                + "          \"args\": [\n"
                + "            { \"type\": \"col\", \"name\": \"wt\" },\n"
                + "            { \"type\": \"lit\", \"value\": 2 }\n"
                + "          ]\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected = "df_out = df.withColumn(\"safe_div\", F.try_divide(F.col(\"wt\"), F.lit(2)))\n";
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testWithColumnTryDivideLiteral", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("withColumn: try_multiply(col, lit) - safe_mul = try_multiply(wt, 2)")
    void testWithColumnTryMultiplyLiteral() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"withColumn\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df_out\",\n"
                + "      \"params\": {\n"
                + "        \"name\": \"safe_mul\",\n"
                + "        \"expr\": {\n"
                + "          \"type\": \"func\",\n"
                + "          \"name\": \"try_multiply\",\n"
                + "          \"args\": [\n"
                + "            { \"type\": \"col\", \"name\": \"wt\" },\n"
                + "            { \"type\": \"lit\", \"value\": 2 }\n"
                + "          ]\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected = "df_out = df.withColumn(\"safe_mul\", F.try_multiply(F.col(\"wt\"), F.lit(2)))\n";
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testWithColumnTryMultiplyLiteral", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("withColumn: try_add(col, lit) - safe_add = try_add(age, 10)")
    void testWithColumnTryAddLiteral() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"withColumn\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df_out\",\n"
                + "      \"params\": {\n"
                + "        \"name\": \"safe_add\",\n"
                + "        \"expr\": {\n"
                + "          \"type\": \"func\",\n"
                + "          \"name\": \"try_add\",\n"
                + "          \"args\": [\n"
                + "            { \"type\": \"col\", \"name\": \"age\" },\n"
                + "            { \"type\": \"lit\", \"value\": 10 }\n"
                + "          ]\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected = "df_out = df.withColumn(\"safe_add\", F.try_add(F.col(\"age\"), F.lit(10)))\n";
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testWithColumnTryAddLiteral", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("withColumn: try_subtract(col, lit) - safe_sub = try_subtract(age, 10)")
    void testWithColumnTrySubtractLiteral() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"withColumn\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df_out\",\n"
                + "      \"params\": {\n"
                + "        \"name\": \"safe_sub\",\n"
                + "        \"expr\": {\n"
                + "          \"type\": \"func\",\n"
                + "          \"name\": \"try_subtract\",\n"
                + "          \"args\": [\n"
                + "            { \"type\": \"col\", \"name\": \"age\" },\n"
                + "            { \"type\": \"lit\", \"value\": 10 }\n"
                + "          ]\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected = "df_out = df.withColumn(\"safe_sub\", F.try_subtract(F.col(\"age\"), F.lit(10)))\n";
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testWithColumnTrySubtractLiteral", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("withColumn: try_divide - 문자열 숫자 리터럴(\"2\")도 숫자로 취급(강제)되는지")
    void testWithColumnTryDivideStringNumericLiteralCoercion() throws Exception {
        // 이 테스트는 'COLUMN_COERCE_NUMERIC' 같은 패치를 적용했을 때만 통과하도록 의도한 케이스.
        // 패치 전에는 F.lit(\"2\")가 되어 기대값이 달라짐.
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"withColumn\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df_out\",\n"
                + "      \"params\": {\n"
                + "        \"name\": \"safe_div2\",\n"
                + "        \"expr\": {\n"
                + "          \"type\": \"func\",\n"
                + "          \"name\": \"try_divide\",\n"
                + "          \"args\": [\n"
                + "            { \"type\": \"col\", \"name\": \"wt\" },\n"
                + "            { \"type\": \"lit\", \"value\": \"2\" }\n"
                + "          ]\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected = "df_out = df.withColumn(\"safe_div2\", F.try_divide(F.col(\"wt\"), F.lit(2)))\n";
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testWithColumnTryDivideStringNumericLiteralCoercion", json, actual);
        assertEquals(expected, actual);
    }

}
