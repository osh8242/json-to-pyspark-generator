package com.douzone.platform.util;

import com.douzone.platform.recipe.util.StringUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.*;

/**
 * description    :
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2025. 11. 13.        osh8242       최초 생성
 */
public class PySparkJsonUtil {

    private PySparkJsonUtil() {}

    @Getter
    @AllArgsConstructor
    public static class DbInfo {
        // DB 종류(예: "postgres", "iceberg" 등)
        private final String source;

        // JDBC/커넥션 정보
        private final String url;          // jdbc url (postgres 계열이면 이 값만으로도 충분)
        private final String table;        // 실제 로드할 테이블명 (schema 포함까지 반영 가능)
        private final String user;
        private final String password;
        private final String driver;

        // 추가 옵션 (fetchsize, stringtype 등)
        private final Map<String, String> options;
    }

    /**
     * json 파라미터에서 load 작업의 테이블 목록을 반환한다.
     * - 최상위 steps 와 join 의 right 내부 steps 까지 재귀적으로 탐색한다.
     */
    public static List<String> getLoadTables(JsonNode root) {
        Set<String> tables = new LinkedHashSet<>();
        collectLoadTables(root, tables);
        return new ArrayList<>(tables);
    }

    private static void collectLoadTables(JsonNode node, Set<String> tables) {
        if (node == null || !node.isObject()) {
            return;
        }

        ArrayNode steps = (ArrayNode) node.get("steps");
        if (steps == null) {
            return;
        }

        for (JsonNode step : steps) {
            if (step == null || !step.isObject()) {
                continue;
            }

            String nodeType = StringUtil.getText(step, "node", null);
            if ("load".equals(nodeType)) {
                JsonNode params = getStepParams(step);
                String table = StringUtil.getText(params, "table", null);
                if (table != null && !table.isEmpty()) {
                    tables.add(table);
                }
            }

            // join 의 right 에 서브 파이프라인이 들어있는 경우까지 재귀 처리
            JsonNode params = getStepParams(step);
            JsonNode rightNode = (params != null) ? params.get("right") : null;
            if (rightNode != null && rightNode.isObject()) {
                collectLoadTables(rightNode, tables);
            }
        }
    }

    /**
     * DB 메타 정보(Map<table, DbInfo>)를 load 스텝에 주입한다.
     * - getLoadTables 로 테이블 목록을 먼저 구한 뒤,
     *   서비스 레이어에서 getCheckRole 등으로 DbInfo 를 구성해서 넘겨주면 된다.
     */
    public static void enrichLoadSteps(JsonNode root, Map<String, DbInfo> dbInfoByTable) {
        if (root == null || !root.isObject() || dbInfoByTable == null || dbInfoByTable.isEmpty()) {
            return;
        }
        enrichLoadStepsInternal(root, dbInfoByTable);
    }

    private static void enrichLoadStepsInternal(JsonNode root, Map<String, DbInfo> dbInfoByTable) {
        if (root == null || !root.isObject()) {
            return;
        }

        ArrayNode steps = (ArrayNode) root.get("steps");
        if (steps == null) {
            return;
        }

        for (JsonNode step : steps) {
            if (step == null || !step.isObject()) {
                continue;
            }

            String nodeType = StringUtil.getText(step, "node", null);
            if ("load".equals(nodeType)) {
                JsonNode params = getStepParams(step);
                if (params != null && params.isObject()) {
                    ObjectNode paramsObj = (ObjectNode) params;

                    String table = StringUtil.getText(paramsObj, "table", null);
                    if (table != null && !table.isEmpty()) {
                        DbInfo dbInfo = dbInfoByTable.get(table);
                        if (dbInfo != null) {
                            applyDbInfoToLoadParams(paramsObj, dbInfo);
                        }
                    }
                }
            }

            // join 의 right 에 서브 파이프라인이 들어있는 경우까지 재귀 처리
            JsonNode params = getStepParams(step);
            JsonNode rightNode = (params != null) ? params.get("right") : null;
            if (rightNode != null && rightNode.isObject()) {
                enrichLoadStepsInternal(rightNode, dbInfoByTable);
            }
        }
    }

    /**
     * 하나의 load 스텝(params)에 DbInfo 를 주입한다.
     * - source, url, table, user, password, driver, options 를 단순히 세팅한다.
     * - table 은 DB 실테이블명(스키마 포함)으로 overwrite 하는 용도로도 사용할 수 있다.
     * - DB 종류가 늘어나더라도 DbInfo 에 필드만 추가하면 유틸은 그대로 재사용 가능하다.
     */
    private static void applyDbInfoToLoadParams(ObjectNode paramsObj, DbInfo dbInfo) {
        // source (spark.read.jdbc, spark.read.table 등에서 분기할 때 사용)
        putIfNotNull(paramsObj, "source", dbInfo.getSource());

        // 공통 JDBC 정보
        putIfNotNull(paramsObj, "url", dbInfo.getUrl());
        putIfNotNull(paramsObj, "table", dbInfo.getTable());
        putIfNotNull(paramsObj, "user", dbInfo.getUser());
        putIfNotNull(paramsObj, "password", dbInfo.getPassword());
        putIfNotNull(paramsObj, "driver", dbInfo.getDriver());

        // options 병합
        Map<String, String> options = dbInfo.getOptions();
        if (options != null && !options.isEmpty()) {
            ObjectNode optionsNode;
            JsonNode existing = paramsObj.get("options");
            if (existing != null && existing.isObject()) {
                optionsNode = (ObjectNode) existing;
            } else {
                optionsNode = paramsObj.putObject("options");
            }
            options.forEach((k, v) -> {
                if (v != null) {
                    // 동일 키가 이미 있다면, 여기서 overwrite 할지 유지할지 정책 결정 가능
                    optionsNode.put(k, v);
                }
            });
        }
    }

    private static void putIfNotNull(ObjectNode node, String field, String value) {
        if (value != null) {
            node.put(field, value);
        }
    }

    private static JsonNode getStepParams(JsonNode step) {
        if (step != null && step.has("params")) {
            JsonNode params = step.get("params");
            if (params != null && params.isObject()) {
                return params;
            }
        }
        return step;
    }
}
