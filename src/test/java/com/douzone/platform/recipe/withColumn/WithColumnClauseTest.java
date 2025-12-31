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
}
