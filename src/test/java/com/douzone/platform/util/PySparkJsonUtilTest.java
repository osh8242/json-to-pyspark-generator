package com.douzone.platform.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * description    :
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2025. 11. 13.        osh8242       최초 생성
 */
class PySparkJsonUtilTest {

    @Test
    @DisplayName("getLoadTables: 최상위 load 스텝에서 테이블 목록 추출")
    void getLoadTablesTest() throws JsonProcessingException {
        String json = "{\n" +
                "  \"steps\": [\n" +
                "    {\n" +
                "      \"node\": \"load\",\n" +
                "      \"output\": \"df\",\n" +
                "      \"params\": {\n" +
                "        \"table\": \"F_AP_PTNT_BSIS_INFM\",\n" +
                "        \"access_key\": \"key1\",\n" +
                "        \"type\": \"basic\"\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"node\": \"load\",\n" +
                "      \"output\": \"df2\",\n" +
                "      \"params\": {\n" +
                "        \"table\": \"F_OTPT_INFM\",\n" +
                "        \"access_key\": \"key2\",\n" +
                "        \"type\": \"basic\"\n" +
                "      }\n" +
                "    }\n" +
                "  ]\n" +
                "}";
        JsonNode root = FormatUtil.getMapper().readTree(json);

        List<String> tables = PySparkJsonUtil.getLoadTables(root);

        System.out.println("==== getLoadTablesTest ====");
        System.out.println("Input JSON:");
        System.out.println(json);
        System.out.println("Extracted tables: " + tables);
        System.out.println("===========================");

        assertNotNull(tables);
        assertEquals(2, tables.size());
        assertTrue(tables.contains("F_AP_PTNT_BSIS_INFM"));
        assertTrue(tables.contains("F_OTPT_INFM"));
    }

    @Test
    @DisplayName("getLoadTables: join.right 내부 서브 파이프라인의 load 테이블까지 추출")
    void getLoadTablesWithNestedRightTest() throws JsonProcessingException {
        String json = "{\n" +
                "  \"steps\": [\n" +
                "    {\n" +
                "      \"node\": \"join\",\n" +
                "      \"input\": \"df\",\n" +
                "      \"output\": \"df_joined\",\n" +
                "      \"params\": {\n" +
                "        \"right\": {\n" +
                "          \"input\": \"logs\",\n" +
                "          \"steps\": [\n" +
                "            {\n" +
                "              \"node\": \"load\",\n" +
                "              \"output\": \"logs_df\",\n" +
                "              \"params\": {\n" +
                "                \"table\": \"LOG_TABLE\",\n" +
                "                \"access_key\": \"key-log\",\n" +
                "                \"type\": \"basic\"\n" +
                "              }\n" +
                "            }\n" +
                "          ]\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  ]\n" +
                "}";
        JsonNode root = FormatUtil.getMapper().readTree(json);

        List<String> tables = PySparkJsonUtil.getLoadTables(root);

        System.out.println("==== getLoadTablesWithNestedRightTest ====");
        System.out.println("Input JSON:");
        System.out.println(json);
        System.out.println("Extracted tables: " + tables);
        System.out.println("==========================================");

        assertNotNull(tables);
        assertEquals(1, tables.size());
        assertEquals("LOG_TABLE", tables.get(0));
    }

    @Test
    @DisplayName("enrichLoadSteps: 단일 load 스텝에 DbInfo 주입")
    void enrichLoadStepsSingleLoadTest() throws JsonProcessingException {
        String json = "{\n" +
                "  \"steps\": [\n" +
                "    {\n" +
                "      \"node\": \"load\",\n" +
                "      \"output\": \"df\",\n" +
                "      \"params\": {\n" +
                "        \"table\": \"F_AP_PTNT_BSIS_INFM\",\n" +
                "        \"access_key\": \"key1\",\n" +
                "        \"type\": \"basic\"\n" +
                "      }\n" +
                "    }\n" +
                "  ]\n" +
                "}";
        JsonNode root = FormatUtil.getMapper().readTree(json);

        Map<String, String> options = new HashMap<>();
        options.put("fetchsize", "1000");
        options.put("stringtype", "unspecified");

        Map<String, PySparkJsonUtil.DbInfo> dbInfoByTable = new HashMap<>();
        dbInfoByTable.put("F_AP_PTNT_BSIS_INFM",
                new PySparkJsonUtil.DbInfo(
                        "postgres",
                        "jdbc:postgresql://localhost:5432/sample",
                        "public.F_AP_PTNT_BSIS_INFM",
                        "app",
                        "secret",
                        "org.postgresql.Driver",
                        options
                ));

        System.out.println("==== enrichLoadStepsSingleLoadTest - BEFORE ====");
        System.out.println(FormatUtil.getMapper()
                .writerWithDefaultPrettyPrinter()
                .writeValueAsString(root));

        PySparkJsonUtil.enrichLoadSteps(root, dbInfoByTable);

        System.out.println("==== enrichLoadStepsSingleLoadTest - AFTER ====");
        System.out.println(FormatUtil.getMapper()
                .writerWithDefaultPrettyPrinter()
                .writeValueAsString(root));
        System.out.println("===============================================");

        JsonNode params = root.get("steps").get(0).get("params");
        assertEquals("postgres", params.get("source").asText());
        assertEquals("jdbc:postgresql://localhost:5432/sample", params.get("url").asText());
        assertEquals("public.F_AP_PTNT_BSIS_INFM", params.get("table").asText());
        assertEquals("app", params.get("user").asText());
        assertEquals("secret", params.get("password").asText());
        assertEquals("org.postgresql.Driver", params.get("driver").asText());

        JsonNode opts = params.get("options");
        assertNotNull(opts);
        assertEquals("1000", opts.get("fetchsize").asText());
        assertEquals("unspecified", opts.get("stringtype").asText());
    }

    @Test
    @DisplayName("enrichLoadSteps: join.right 서브 파이프라인의 load 스텝에도 DbInfo 주입")
    void enrichLoadStepsNestedRightTest() throws JsonProcessingException {
        String json = "{\n" +
                "  \"steps\": [\n" +
                "    {\n" +
                "      \"node\": \"join\",\n" +
                "      \"input\": \"df\",\n" +
                "      \"output\": \"df_joined\",\n" +
                "      \"params\": {\n" +
                "        \"right\": {\n" +
                "          \"input\": \"logs\",\n" +
                "          \"steps\": [\n" +
                "            {\n" +
                "              \"node\": \"load\",\n" +
                "              \"output\": \"logs_df\",\n" +
                "              \"params\": {\n" +
                "                \"table\": \"LOG_TABLE\",\n" +
                "                \"access_key\": \"key-log\",\n" +
                "                \"type\": \"basic\"\n" +
                "              }\n" +
                "            }\n" +
                "          ]\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  ]\n" +
                "}";
        JsonNode root = FormatUtil.getMapper().readTree(json);

        Map<String, PySparkJsonUtil.DbInfo> dbInfoByTable = new HashMap<>();
        Map<String, String> logOptions = new HashMap<>();
        logOptions.put("fetchsize", "2000");
        dbInfoByTable.put("LOG_TABLE",
                new PySparkJsonUtil.DbInfo(
                        "postgres",
                        "jdbc:postgresql://localhost:5432/logdb",
                        "public.LOG_TABLE",
                        "log_user",
                        "log_pw",
                        "org.postgresql.Driver",
                        logOptions
                ));

        System.out.println("==== enrichLoadStepsNestedRightTest - BEFORE ====");
        System.out.println(FormatUtil.getMapper()
                .writerWithDefaultPrettyPrinter()
                .writeValueAsString(root));

        PySparkJsonUtil.enrichLoadSteps(root, dbInfoByTable);

        System.out.println("==== enrichLoadStepsNestedRightTest - AFTER ====");
        System.out.println(FormatUtil.getMapper()
                .writerWithDefaultPrettyPrinter()
                .writeValueAsString(root));
        System.out.println("================================================");

        JsonNode right = root.get("steps").get(0).get("params").get("right");
        JsonNode loadParams = right.get("steps").get(0).get("params");

        assertEquals("postgres", loadParams.get("source").asText());
        assertEquals("jdbc:postgresql://localhost:5432/logdb", loadParams.get("url").asText());
        assertEquals("public.LOG_TABLE", loadParams.get("table").asText());
        assertEquals("log_user", loadParams.get("user").asText());
        assertEquals("log_pw", loadParams.get("password").asText());
        assertEquals("org.postgresql.Driver", loadParams.get("driver").asText());

        JsonNode opts = loadParams.get("options");
        assertNotNull(opts);
        assertEquals("2000", opts.get("fetchsize").asText());
    }
}
