package com.douzone.platform.recipe;

import com.douzone.platform.recipe.builder.StepBuilder;
import com.douzone.platform.recipe.util.StringUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import lombok.Getter;

import java.util.HashSet;
import java.util.Set;

public class PySparkChainGenerator {

    private final StepBuilder stepBuilder;
    private final ObjectMapper om = new ObjectMapper();

    // 생성자에서 StepBuilder를 초기화하며, 재귀 호출을 위해 자기 자신의 참조를 넘겨줍니다.
    public PySparkChainGenerator() {
        this.stepBuilder = new StepBuilder(this);
    }

    /**
     * 외부에서 호출하는 정적 진입점 메서드입니다.
     */
    public static String generate(String json) throws Exception {
        if (json == null || json.trim().isEmpty()) {
            throw new RecipeStepException("Recipe JSON must not be empty.");
        }
        PySparkChainGenerator generator = new PySparkChainGenerator();
        return generator.generatePySparkCode(json);
    }

    public static String generate(JsonNode json) throws Exception {
        if (json == null) {
            throw new RecipeStepException("Recipe JSON must not be null.");
        }
        PySparkChainGenerator generator = new PySparkChainGenerator();
        return generator.generatePySparkCode(json);
    }

    /**
     * JSON 파라미터에서 테이블 목록을 추출합니다.
     * - input 필드와 join step의 right 테이블(문자열 또는 sub-JSON input)을 재귀적으로 수집.
     * - 중복 제거된 Set으로 반환.
     *
     * @param json JSON 문자열
     * @return 테이블 이름들의 Set (예: {"df", "orders_df", "users_df"})
     * @throws Exception JSON 파싱 또는 처리 오류 시
     */
    public static Set<String> extractTables(String json) throws Exception {
        PySparkChainGenerator generator = new PySparkChainGenerator();
        JsonNode root = generator.om.readTree(json);
        Set<String> tables = new HashSet<>();
        generator.collectTables(root, tables);
        return tables;
    }

    /**
     * 실제 코드 생성을 담당하는 인스턴스 메서드입니다.
     * PySpark 스크립트의 전체적인 틀(import, out = ...)을 생성합니다.
     */
    public String generatePySparkCode(String json) throws Exception {
        JsonNode root = om.readTree(json);
        return generatePySparkCode(root);
    }

    public String generatePySparkCode(JsonNode root) throws Exception {
        if (root == null || root.isNull()) {
            throw new RecipeStepException("Recipe JSON must not be null.");
        }

        JsonNode stepsNode = root.get("steps");
        if (stepsNode == null || !stepsNode.isArray()) {
            throw new RecipeStepException("Recipe JSON must contain a 'steps' array.");
        }

        ArrayNode steps = (ArrayNode) stepsNode;

        StringBuilder script = new StringBuilder();

        for (JsonNode node : steps) {
            String opName = StringUtil.getText(node, "node", null);
            if (opName == null) {
                continue;
            }

            String inputDf = StringUtil.getText(node, "input", "");
            String outputDf = StringUtil.getText(node, "output", null);

            // 1) show 는 action 이라 대입문 없이 한 줄짜리 statement 로 생성
            switch (opName) {
                case "show":
                    script.append(inputDf);
                    script.append(stepBuilder.buildShowAction(node));  // ".show(...)\n"
                    continue;
                case "print":
                    // show 와 마찬가지로 action 이라 대입문 없이 한 줄짜리 블록 생성
                    script.append(stepBuilder.buildPrint(node));
                    continue;
                case "save":
                    script.append(stepBuilder.buildSave(node));
                    continue;
            }


            // 2) 나머지 node 들은 변환이므로 "out = in.xxx()" 형태로 생성
            if (outputDf == null || outputDf.isEmpty()) {
                // output 이 없으면 in-place 갱신 (df = df.xxx())
                outputDf = inputDf;
            }

            // 대입문 헤더
            script.append(outputDf)
                    .append(" = ")
                    .append(inputDf);

            // 뒤에 체인 메서드 붙이기
            switch (opName) {
                case "load":
                    script.append(stepBuilder.buildLoad(node));
                    break;
                case "select":
                    script.append(stepBuilder.buildSelect(node));
                    break;
                case "withColumn":
                    script.append(stepBuilder.buildWithColumn(node));
                    break;
                case "withColumns":
                    script.append(stepBuilder.buildWithColumns(node));
                    break;
                case "filter":
                case "where":
                    script.append(stepBuilder.buildFilter(node));
                    break;
                case "join":
                    script.append(stepBuilder.buildJoin(node));
                    break;
                case "groupBy":
                    script.append(stepBuilder.buildGroupBy(node));
                    break;
                case "agg":
                    script.append(stepBuilder.buildAgg(node));
                    break;
                case "orderBy":
                case "sort":
                    script.append(stepBuilder.buildOrderBy(node));
                    break;
                case "limit":
                    script.append(stepBuilder.buildLimit(node));
                    break;
                case "distinct":
                    script.append(".distinct()\n");
                    break;
                case "dropDuplicates":
                    script.append(stepBuilder.buildDropDuplicates(node));
                    break;
                case "drop":
                    script.append(stepBuilder.buildDrop(node));
                    break;
                case "withColumnRenamed":
                    script.append(stepBuilder.buildWithColumnRenamed(node));
                    break;
                default:
                    script.append(stepBuilder.buildDefaultStep(opName, node));
                    break;
            }
        }
        return script.toString();
    }

    /**
     * steps 배열을 받아 체인 메서드 문자열을 생성하는 재귀적 핵심 로직입니다.
     * 이 메서드는 순수하게 메서드 체인(.filter(...).join(...)) 부분만 생성합니다.
     */
    public ChainBuildResult buildChain(String inputDf, ArrayNode steps) throws Exception {
        String baseExpression = inputDf;
        StringBuilder sb = new StringBuilder();

        if (steps == null) {
            return new ChainBuildResult(baseExpression, "");
        }

        for (JsonNode step : steps) {
            String opName = StringUtil.getText(step, "node", null);
            if (opName == null) continue;

            switch (opName) {
                case "load":
                    String loadExpr = stepBuilder.buildLoad(step);
                    if (loadExpr != null && !loadExpr.isEmpty()) {
                        baseExpression = loadExpr;
                    }
                    break;
                case "select":
                    sb.append(stepBuilder.buildSelect(step));
                    break;
                case "withColumn":
                    sb.append(stepBuilder.buildWithColumn(step));
                    break;
                case "withColumns":
                    sb.append(stepBuilder.buildWithColumns(step));
                    break;
                case "filter":
                case "where":
                    sb.append(stepBuilder.buildFilter(step));
                    break;
                case "join":
                    sb.append(stepBuilder.buildJoin(step));
                    break;
                case "groupBy":
                    sb.append(stepBuilder.buildGroupBy(step));
                    break;
                case "agg":
                    sb.append(stepBuilder.buildAgg(step));
                    break;
                case "orderBy":
                case "sort":
                    sb.append(stepBuilder.buildOrderBy(step));
                    break;
                case "limit":
                    sb.append(stepBuilder.buildLimit(step));
                    break;
                case "show":
                    // show는 action이므로 체인에 포함하지 않음
                    break;
                case "distinct":
                    sb.append(".distinct()\n");
                    break;
                case "dropDuplicates":
                    sb.append(stepBuilder.buildDropDuplicates(step));
                    break;
                case "drop":
                    sb.append(stepBuilder.buildDrop(step));
                    break;
                case "withColumnRenamed":
                    sb.append(stepBuilder.buildWithColumnRenamed(step));
                    break;
                default:
                    sb.append(stepBuilder.buildDefaultStep(opName, step));
                    break;
            }
        }

        return new ChainBuildResult(baseExpression, sb.toString());
    }

    /**
     * 인스턴스 메서드로 JSON 노드에서 테이블을 재귀적으로 수집합니다.
     * - 외부 호출은 static extractTables 사용 권장.
     */
    private void collectTables(JsonNode node, Set<String> tables) {
        if (node == null || !node.isObject()) {
            return;
        }

        // input 필드 추가 (기본 "df")
        String input = StringUtil.getText(node, "input", "df");
        tables.add(input);

        // steps 배열 검사
        ArrayNode steps = (ArrayNode) node.get("steps");
        if (steps != null) {
            for (JsonNode step : steps) {
                String opName = StringUtil.getText(step, "node", null);
                if ("load".equals(opName)) {
                    collectLoadTables(step, tables);
                } else if ("join".equals(opName)) {                    // join step: right 처리
                    JsonNode rightNode = getStepField(step, "right");
                    if (rightNode != null && !rightNode.isNull()) {
                        if (rightNode.isTextual()) {
                            // right가 문자열: 테이블 이름 추가
                            tables.add(rightNode.asText());
                        } else if (rightNode.isObject()) {
                            // right가 객체: input 추가 + 재귀 steps 검사
                            String subInput = StringUtil.getText(rightNode, "input", "df");
                            tables.add(subInput);
                            collectTables(rightNode, tables);  // sub-JSON 재귀
                        }
                    }
                }
                // 다른 step은 테이블 생성 안 하므로 무시
            }
        }
    }

    private void collectLoadTables(JsonNode step, Set<String> tables) {
        JsonNode params = getStepParams(step);
        String source = StringUtil.getText(params, "source", null);
        if (source == null) {
            String table = StringUtil.getText(params, "table", null);
            if (table != null && !table.isEmpty()) {
                tables.add(table);
            }
            return;
        }

        switch (source.toLowerCase()) {
            case "iceberg":
                String catalog = StringUtil.getText(params, "catalog", null);
                String database = StringUtil.getText(params, "database", null);
                String table = StringUtil.getText(params, "table", null);
                if (table != null && !table.isEmpty()) {
                    StringBuilder identifier = new StringBuilder();
                    if (catalog != null && !catalog.isEmpty()) {
                        identifier.append(catalog).append('.');
                    }
                    if (database != null && !database.isEmpty()) {
                        identifier.append(database).append('.');
                    }
                    identifier.append(table);
                    tables.add(identifier.toString());
                }
                break;
            case "postgres":
            case "postgresql":
                String jdbcTable = StringUtil.getText(params, "table", null);
                if (jdbcTable != null && !jdbcTable.isEmpty()) {
                    tables.add(jdbcTable);
                }
                JsonNode options = params.get("options");
                if (options != null && options.isObject()) {
                    if (options.hasNonNull("dbtable")) {
                        tables.add(options.get("dbtable").asText());
                    }
                    if (options.hasNonNull("table")) {
                        tables.add(options.get("table").asText());
                    }
                    if (options.hasNonNull("query")) {
                        tables.add(options.get("query").asText());
                    }
                }
                break;
            default:
                String generic = StringUtil.getText(params, "table", null);
                if (generic != null && !generic.isEmpty()) {
                    tables.add(generic);
                }
                break;
        }
    }

    private JsonNode getStepParams(JsonNode step) {
        if (step != null && step.has("params")) {
            JsonNode params = step.get("params");
            if (params != null && params.isObject()) {
                return params;
            }
        }
        return step;
    }

    private JsonNode getStepField(JsonNode step, String field) {
        JsonNode params = getStepParams(step);
        return params != null ? params.get(field) : null;
    }

    @Getter
    public static class ChainBuildResult {
        private final String baseExpression;
        private final String chain;

        public ChainBuildResult(String baseExpression, String chain) {
            this.baseExpression = baseExpression;
            this.chain = chain;
        }

    }

}
