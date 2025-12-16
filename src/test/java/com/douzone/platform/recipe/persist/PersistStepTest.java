package com.douzone.platform.recipe.persist;

import com.douzone.platform.recipe.PySparkChainGenerator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.douzone.platform.recipe.util.TestUtil.printTestInfo;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * description    : persist 플래그가 true인 step의 코드 라인 끝에 .persist()가 붙는지 검증
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2025. 12. 16.       osh8242       persist 테스트 추가
 */
public class PersistStepTest {

    @Test
    @DisplayName("Persist: filter step에 persist=true면 라인 끝에 .persist() 추가")
    void testPersistOnFilterStep() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"filter\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df_filtered\",\n"
                + "      \"persist\": true,\n"
                + "      \"params\": {\n"
                + "        \"condition\": {\n"
                + "          \"type\": \"op\",\n"
                + "          \"op\": \">=\",\n"
                + "          \"left\": { \"type\": \"col\", \"name\": \"age\" },\n"
                + "          \"right\": { \"type\": \"lit\", \"value\": 20 }\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected = "df_filtered = df.filter((F.col(\"age\") >= F.lit(20))).persist()\n";
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testPersistOnFilterStep", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Persist: persist 미지정이면 기존과 동일(= .persist()가 붙지 않음)")
    void testPersistAbsentKeepsOriginal() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"filter\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df_filtered\",\n"
                + "      \"params\": {\n"
                + "        \"condition\": {\n"
                + "          \"type\": \"op\",\n"
                + "          \"op\": \">=\",\n"
                + "          \"left\": { \"type\": \"col\", \"name\": \"age\" },\n"
                + "          \"right\": { \"type\": \"lit\", \"value\": 20 }\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected = "df_filtered = df.filter((F.col(\"age\") >= F.lit(20)))\n";
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testPersistAbsentKeepsOriginal", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Persist: distinct step에 persist=true면 .distinct().persist() 형태로 생성")
    void testPersistOnDistinctStep() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"distinct\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df_distinct\",\n"
                + "      \"persist\": true\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected = "df_distinct = df.distinct().persist()\n";
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testPersistOnDistinctStep", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Persist: output이 없는 변환 step(in-place)에도 persist 적용(df = df.xxx().persist())")
    void testPersistInPlaceUpdate() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"distinct\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"persist\": true\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected = "df = df.distinct().persist()\n";
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testPersistInPlaceUpdate", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Persist: 여러 step 중 persist=true인 라인에만 .persist()가 붙는지 검증")
    void testPersistMixedSteps() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"filter\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df1\",\n"
                + "      \"persist\": true,\n"
                + "      \"params\": {\n"
                + "        \"condition\": {\n"
                + "          \"type\": \"op\",\n"
                + "          \"op\": \">=\",\n"
                + "          \"left\": { \"type\": \"col\", \"name\": \"age\" },\n"
                + "          \"right\": { \"type\": \"lit\", \"value\": 20 }\n"
                + "        }\n"
                + "      }\n"
                + "    },\n"
                + "    {\n"
                + "      \"node\": \"distinct\",\n"
                + "      \"input\": \"df1\",\n"
                + "      \"output\": \"df2\"\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected = "df1 = df.filter((F.col(\"age\") >= F.lit(20))).persist()\n"
                + "df2 = df1.distinct()\n";

        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testPersistMixedSteps", json, actual);
        assertEquals(expected, actual);
    }
}
