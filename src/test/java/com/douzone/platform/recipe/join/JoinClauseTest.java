package com.douzone.platform.recipe.join;

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
 * 2025. 9. 17.        osh8242       최초 생성
 */
public class JoinClauseTest {
    @Test
    @DisplayName("Join: 가장 기본적인 Inner Join (별칭 사용)")
    void testBasicInnerJoinWithAliases() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"step\": \"join\",\n"
                + "      \"right\": \"departments\",\n"
                + "      \"how\": \"inner\",\n"
                + "      \"leftAlias\": \"emp\",\n"
                + "      \"rightAlias\": \"dept\",\n"
                + "      \"on\": {\n"
                + "        \"type\": \"op\", \"op\": \"=\",\n"
                + "        \"left\": { \"type\": \"col\", \"name\": \"department_id\", \"table\": \"emp\" },\n"
                + "        \"right\": { \"type\": \"col\", \"name\": \"id\", \"table\": \"dept\" }\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expectedStep = "  .alias(\"emp\")\n"
                + "  .join((departments).alias(\"dept\"), (F.col(\"emp.department_id\") == F.col(\"dept.id\")), \"inner\")\n";
        String expected = buildFullScript(expectedStep);
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testBasicInnerJoinWithAliases", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Join: Left Outer Join 테스트")
    void testLeftOuterJoin() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"step\": \"join\",\n"
                + "      \"right\": \"salaries\",\n"
                + "      \"how\": \"left_outer\",\n"
                + "      \"on\": {\n"
                + "        \"type\": \"op\", \"op\": \"=\",\n"
                + "        \"left\": { \"type\": \"col\", \"name\": \"id\" },\n"
                + "        \"right\": { \"type\": \"col\", \"name\": \"emp_id\", \"table\": \"salaries\" }\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expectedStep = "  .join(salaries, (F.col(\"id\") == F.col(\"salaries.emp_id\")), \"left_outer\")\n";
        String expected = buildFullScript(expectedStep);
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testLeftOuterJoin", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Join: 여러 조건(Array on)으로 조인")
    void testJoinWithMultipleConditions() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"step\": \"join\",\n"
                + "      \"right\": \"orders\",\n"
                + "      \"leftAlias\": \"c\",\n"
                + "      \"rightAlias\": \"o\",\n"
                + "      \"on\": [\n"
                + "        { \"type\": \"op\", \"op\": \"=\", \"left\": {\"type\":\"col\", \"name\":\"id\", \"table\":\"c\"}, \"right\": {\"type\":\"col\", \"name\":\"customer_id\", \"table\":\"o\"} },\n"
                + "        { \"type\": \"op\", \"op\": \"=\", \"left\": {\"type\":\"col\", \"name\":\"year\", \"table\":\"c\"}, \"right\": {\"type\":\"col\", \"name\":\"order_year\", \"table\":\"o\"} }\n"
                + "      ]\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expectedStep = "  .alias(\"c\")\n"
                + "  .join((orders).alias(\"o\"), ((F.col(\"c.id\") == F.col(\"o.customer_id\"))) & ((F.col(\"c.year\") == F.col(\"o.order_year\"))), \"inner\")\n";
        String expected = buildFullScript(expectedStep);
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testJoinWithMultipleConditions", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Join: Cross Join (on 조건 없음)")
    void testCrossJoin() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"step\": \"join\",\n"
                + "      \"right\": \"products\",\n"
                + "      \"how\": \"cross\"\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expectedStep = "  .join(products, None, \"cross\")\n";
        String expected = buildFullScript(expectedStep);
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testCrossJoin", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Join: Self Join (동일 DataFrame 조인)")
    void testSelfJoin() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"step\": \"join\",\n"
                + "      \"right\": \"df\",\n"
                + "      \"how\": \"left\",\n"
                + "      \"leftAlias\": \"emp\",\n"
                + "      \"rightAlias\": \"mgr\",\n"
                + "      \"on\": {\n"
                + "        \"type\": \"op\", \"op\": \"=\",\n"
                + "        \"left\": { \"type\": \"col\", \"name\": \"manager_id\", \"table\": \"emp\" },\n"
                + "        \"right\": { \"type\": \"col\", \"name\": \"id\", \"table\": \"mgr\" }\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expectedStep = "  .alias(\"emp\")\n"
                + "  .join((df).alias(\"mgr\"), (F.col(\"emp.manager_id\") == F.col(\"mgr.id\")), \"left\")\n";
        String expected = buildFullScript(expectedStep);
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testSelfJoin", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Join: 복잡한 on 조건 (비교 연산자 포함)")
    void testJoinWithComplexOnCondition() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"step\": \"join\",\n"
                + "      \"right\": \"events\",\n"
                + "      \"leftAlias\": \"u\",\n"
                + "      \"rightAlias\": \"e\",\n"
                + "      \"on\": {\n"
                + "        \"type\": \"op\", \"op\": \"and\",\n"
                + "        \"left\": { \"type\": \"op\", \"op\": \"=\", \"left\": {\"type\":\"col\", \"name\":\"id\", \"table\":\"u\"}, \"right\": {\"type\":\"col\", \"name\":\"user_id\", \"table\":\"e\"} },\n"
                + "        \"right\": { \"type\": \"op\", \"op\": \">\", \"left\": {\"type\":\"col\", \"name\":\"event_timestamp\", \"table\":\"e\"}, \"right\": {\"type\":\"col\", \"name\":\"signup_date\", \"table\":\"u\"} }\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expectedStep = "  .alias(\"u\")\n"
                + "  .join((events).alias(\"e\"), ((F.col(\"u.id\") == F.col(\"e.user_id\")) & (F.col(\"e.event_timestamp\") > F.col(\"u.signup_date\"))), \"inner\")\n";
        String expected = buildFullScript(expectedStep);
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testJoinWithComplexOnCondition", json, actual);
        assertEquals(expected, actual);
    }

}
