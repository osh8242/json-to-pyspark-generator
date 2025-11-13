package com.douzone.platform.recipe.join;

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
public class JoinClauseTest {

    @Test
    @DisplayName("Join: 가장 기본적인 Inner Join (별칭 사용)")
    void testBasicInnerJoinWithAliases() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"join\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df_joined\",\n"
                + "      \"params\": {\n"
                + "        \"right\": \"departments\",\n"
                + "        \"how\": \"inner\",\n"
                + "        \"leftAlias\": \"emp\",\n"
                + "        \"rightAlias\": \"dept\",\n"
                + "        \"on\": {\n"
                + "          \"type\": \"op\", \"op\": \"=\",\n"
                + "          \"left\": { \"type\": \"col\", \"name\": \"department_id\", \"table\": \"emp\" },\n"
                + "          \"right\": { \"type\": \"col\", \"name\": \"id\", \"table\": \"dept\" }\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected =
                "df_joined = df.alias(\"emp\").join(\n" +
                        "  (departments).alias(\"dept\"),\n" +
                        "  (F.col(\"emp.department_id\") == F.col(\"dept.id\")),\n" +
                        "  \"inner\"\n" +
                        ")\n";

        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testBasicInnerJoinWithAliases", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Join: Left Outer Join 테스트")
    void testLeftOuterJoin() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"join\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df_joined\",\n"
                + "      \"params\": {\n"
                + "        \"right\": \"salaries\",\n"
                + "        \"how\": \"left_outer\",\n"
                + "        \"on\": {\n"
                + "          \"type\": \"op\", \"op\": \"=\",\n"
                + "          \"left\": { \"type\": \"col\", \"name\": \"id\" },\n"
                + "          \"right\": { \"type\": \"col\", \"name\": \"emp_id\", \"table\": \"salaries\" }\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected =
                "df_joined = df.join(\n" +
                        "  salaries,\n" +
                        "  (F.col(\"id\") == F.col(\"salaries.emp_id\")),\n" +
                        "  \"left_outer\"\n" +
                        ")\n";

        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testLeftOuterJoin", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Join: 여러 조건(Array on)으로 조인")
    void testJoinWithMultipleConditions() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"join\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df_joined\",\n"
                + "      \"params\": {\n"
                + "        \"right\": \"orders\",\n"
                + "        \"leftAlias\": \"c\",\n"
                + "        \"rightAlias\": \"o\",\n"
                + "        \"on\": [\n"
                + "          { \"type\": \"op\", \"op\": \"=\", \"left\": {\"type\":\"col\", \"name\":\"id\", \"table\":\"c\"}, \"right\": {\"type\":\"col\", \"name\":\"customer_id\", \"table\":\"o\"} },\n"
                + "          { \"type\": \"op\", \"op\": \"=\", \"left\": {\"type\":\"col\", \"name\":\"year\", \"table\":\"c\"}, \"right\": {\"type\":\"col\", \"name\":\"order_year\", \"table\":\"o\"} }\n"
                + "        ]\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected =
                "df_joined = df.alias(\"c\").join(\n" +
                        "  (orders).alias(\"o\"),\n" +
                        "  ((F.col(\"c.id\") == F.col(\"o.customer_id\"))) & ((F.col(\"c.year\") == F.col(\"o.order_year\"))),\n" +
                        "  \"inner\"\n" +
                        ")\n";

        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testJoinWithMultipleConditions", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Join: Cross Join (on 조건 없음)")
    void testCrossJoin() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"join\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df_joined\",\n"
                + "      \"params\": {\n"
                + "        \"right\": \"products\",\n"
                + "        \"how\": \"cross\"\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected =
                "df_joined = df.join(\n" +
                        "  products,\n" +
                        "  None,\n" +
                        "  \"cross\"\n" +
                        ")\n";

        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testCrossJoin", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Join: Self Join (동일 DataFrame 조인)")
    void testSelfJoin() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"join\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df_joined\",\n"
                + "      \"params\": {\n"
                + "        \"right\": \"df\",\n"
                + "        \"how\": \"left\",\n"
                + "        \"leftAlias\": \"emp\",\n"
                + "        \"rightAlias\": \"mgr\",\n"
                + "        \"on\": {\n"
                + "          \"type\": \"op\", \"op\": \"=\",\n"
                + "          \"left\": { \"type\": \"col\", \"name\": \"manager_id\", \"table\": \"emp\" },\n"
                + "          \"right\": { \"type\": \"col\", \"name\": \"id\", \"table\": \"mgr\" }\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected =
                "df_joined = df.alias(\"emp\").join(\n" +
                        "  (df).alias(\"mgr\"),\n" +
                        "  (F.col(\"emp.manager_id\") == F.col(\"mgr.id\")),\n" +
                        "  \"left\"\n" +
                        ")\n";

        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testSelfJoin", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Join: 복잡한 on 조건 (비교 연산자 포함)")
    void testJoinWithComplexOnCondition() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"join\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df_joined\",\n"
                + "      \"params\": {\n"
                + "        \"right\": \"events\",\n"
                + "        \"leftAlias\": \"u\",\n"
                + "        \"rightAlias\": \"e\",\n"
                + "        \"on\": {\n"
                + "          \"type\": \"op\", \"op\": \"and\",\n"
                + "          \"left\": { \"type\": \"op\", \"op\": \"=\", \"left\": {\"type\":\"col\", \"name\":\"id\", \"table\":\"u\"}, \"right\": {\"type\":\"col\", \"name\":\"user_id\", \"table\":\"e\"} },\n"
                + "          \"right\": { \"type\": \"op\", \"op\": \">\", \"left\": {\"type\":\"col\", \"name\":\"event_timestamp\", \"table\":\"e\"}, \"right\": {\"type\":\"col\", \"name\":\"signup_date\", \"table\":\"u\"} }\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected =
                "df_joined = df.alias(\"u\").join(\n" +
                        "  (events).alias(\"e\"),\n" +
                        "  ((F.col(\"u.id\") == F.col(\"e.user_id\")) & (F.col(\"e.event_timestamp\") > F.col(\"u.signup_date\"))),\n" +
                        "  \"inner\"\n" +
                        ")\n";

        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testJoinWithComplexOnCondition", json, actual);
        assertEquals(expected, actual);
    }

}
