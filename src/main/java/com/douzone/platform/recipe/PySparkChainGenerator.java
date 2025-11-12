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
        PySparkChainGenerator generator = new PySparkChainGenerator();
        return generator.generatePySparkCode(json);
    }

    public static String generate(JsonNode json) throws Exception {
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
        ArrayNode steps = root != null && root.has("steps") && root.get("steps").isArray()
                ? (ArrayNode) root.get("steps")
                : null;

        if (steps == null) {
            return "";
        }

        if (isNewSchema(root, steps)) {
            return generateLinePerStepScript(steps);
        }

        return generateLegacyScript(root, steps);
    }

    private String generateLegacyScript(JsonNode root, ArrayNode steps) throws Exception {
        String inputDf = StringUtil.getText(root, "input", "df");
        String outDf = StringUtil.getText(root, "output", "result_df");

        StringBuilder script = new StringBuilder();
        String currentBase = inputDf;
        StringBuilder chainBuilder = new StringBuilder();
        boolean materialized = false;

        for (JsonNode step : steps) {
            String opName = resolveNodeName(step);
            if (opName == null) {
                continue;
            }

            switch (opName) {
                case "load":
                    String loadExpr = stepBuilder.buildLoad(step);
                    if (loadExpr != null && !loadExpr.isEmpty()) {
                        currentBase = loadExpr;
                        chainBuilder.setLength(0);
                        materialized = false;
                    }
                    break;
                case "show":
                    if (!materialized || chainBuilder.length() > 0 || !outDf.equals(currentBase)) {
                        appendAssignment(script, outDf, currentBase, chainBuilder);
                        currentBase = outDf;
                        chainBuilder.setLength(0);
                        materialized = true;
                    }
                    script.append(stepBuilder.buildShowAction(step, outDf));
                    break;
                case "select":
                    chainBuilder.append(stepBuilder.buildSelect(step));
                    materialized = false;
                    break;
                case "withColumn":
                    chainBuilder.append(stepBuilder.buildWithColumn(step));
                    materialized = false;
                    break;
                case "withColumns":
                    chainBuilder.append(stepBuilder.buildWithColumns(step));
                    materialized = false;
                    break;
                case "filter":
                case "where":
                    chainBuilder.append(stepBuilder.buildFilter(step));
                    materialized = false;
                    break;
                case "join":
                    chainBuilder.append(stepBuilder.buildJoin(step));
                    materialized = false;
                    break;
                case "groupBy":
                    chainBuilder.append(stepBuilder.buildGroupBy(step));
                    materialized = false;
                    break;
                case "agg":
                    chainBuilder.append(stepBuilder.buildAgg(step));
                    materialized = false;
                    break;
                case "orderBy":
                case "sort":
                    chainBuilder.append(stepBuilder.buildOrderBy(step));
                    materialized = false;
                    break;
                case "toJSON":
                    chainBuilder.append(stepBuilder.buildToJson(step));
                    materialized = false;
                    break;
                case "limit":
                    chainBuilder.append(stepBuilder.buildLimit(step));
                    materialized = false;
                    break;
                case "distinct":
                    chainBuilder.append("  .distinct()\n");
                    materialized = false;
                    break;
                case "dropDuplicates":
                    chainBuilder.append(stepBuilder.buildDropDuplicates(step));
                    materialized = false;
                    break;
                case "repartition":
                    chainBuilder.append(stepBuilder.buildRepartition(step));
                    materialized = false;
                    break;
                case "coalesce":
                    chainBuilder.append(stepBuilder.buildCoalesce(step));
                    materialized = false;
                    break;
                case "sample":
                    chainBuilder.append(stepBuilder.buildSample(step));
                    materialized = false;
                    break;
                case "drop":
                    chainBuilder.append(stepBuilder.buildDrop(step));
                    materialized = false;
                    break;
                case "withColumnRenamed":
                    chainBuilder.append(stepBuilder.buildWithColumnRenamed(step));
                    materialized = false;
                    break;
                default:
                    chainBuilder.append(stepBuilder.buildDefaultStep(opName, step));
                    materialized = false;
                    break;
            }
        }

        if (!materialized || chainBuilder.length() > 0 || !outDf.equals(currentBase)) {
            appendAssignment(script, outDf, currentBase, chainBuilder);
        }

        return script.toString();
    }

    private String generateLinePerStepScript(ArrayNode steps) throws Exception {
        StringBuilder script = new StringBuilder();

        for (JsonNode step : steps) {
            if (step == null || step.isNull()) {
                continue;
            }

            String opName = resolveNodeName(step);
            if (opName == null) {
                continue;
            }

            String inputDf = StringUtil.getText(step, "input", "df");
            String outputDf = StringUtil.getText(step, "output", inputDf);

            switch (opName) {
                case "load":
                    appendLoadStatement(script, outputDf, step);
                    break;
                case "show":
                    script.append(stepBuilder.buildShowAction(step, inputDf));
                    break;
                case "distinct":
                    appendTransformation(script, outputDf, inputDf, "  .distinct()\n");
                    break;
                default:
                    appendTransformation(script, outputDf, inputDf, buildTransformationSuffix(opName, step));
                    break;
            }
        }

        return script.toString();
    }

    private boolean isNewSchema(JsonNode root, ArrayNode steps) {
        if (steps == null) {
            return false;
        }

        for (JsonNode step : steps) {
            if (step != null && step.has("output")) {
                return true;
            }
        }

        boolean hasTopLevelInput = root != null && (root.has("input") || root.has("output"));
        if (!hasTopLevelInput) {
            for (JsonNode step : steps) {
                if (step != null && step.has("node") && !step.has("step")) {
                    return true;
                }
            }
        }

        return false;
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
            String opName = resolveNodeName(step);
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
                case "toJSON":
                    sb.append(stepBuilder.buildToJson(step));
                    break;
                case "limit":
                    sb.append(stepBuilder.buildLimit(step));
                    break;
                case "show":
                    // show는 action이므로 체인에 포함하지 않음
                    break;
                case "distinct":
                    sb.append("  .distinct()\n");
                    break;
                case "dropDuplicates":
                    sb.append(stepBuilder.buildDropDuplicates(step));
                    break;
                case "repartition":
                    sb.append(stepBuilder.buildRepartition(step));
                    break;
                case "coalesce":
                    sb.append(stepBuilder.buildCoalesce(step));
                    break;
                case "sample":
                    sb.append(stepBuilder.buildSample(step));
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

        if (node.has("input")) {
            tables.add(StringUtil.getText(node, "input", "df"));
        }

        ArrayNode steps = (ArrayNode) node.get("steps");
        if (steps == null) {
            return;
        }

        boolean hasExplicitInput = node.has("input");
        if (!hasExplicitInput) {
            for (JsonNode step : steps) {
                if (step != null && step.has("input")) {
                    hasExplicitInput = true;
                    tables.add(StringUtil.getText(step, "input", "df"));
                }
            }
        }

        if (!hasExplicitInput) {
            tables.add("df");
        }

        for (JsonNode step : steps) {
            if (step == null || step.isNull()) {
                continue;
            }

            if (step.has("input")) {
                tables.add(StringUtil.getText(step, "input", "df"));
            }

            String opName = resolveNodeName(step);
            if ("load".equals(opName)) {
                collectLoadTables(step, tables);
                continue;
            }

            if ("join".equals(opName)) {
                JsonNode rightNode = getStepField(step, "right");
                if (rightNode == null || rightNode.isNull()) {
                    continue;
                }

                if (rightNode.isTextual()) {
                    tables.add(rightNode.asText());
                } else if (rightNode.isObject()) {
                    if (rightNode.has("input")) {
                        tables.add(StringUtil.getText(rightNode, "input", "df"));
                    }
                    collectTables(rightNode, tables);
                }
            }
        }
    }

    private void appendAssignment(StringBuilder script, String outDf, String baseExpression, StringBuilder chainBuilder) {
        script.append(outDf).append(" = (\n");
        script.append(formatBaseExpression(baseExpression));
        script.append(chainBuilder);
        script.append(")\n");
    }

    private String formatBaseExpression(String baseExpression) {
        if (baseExpression == null) {
            baseExpression = "df";
        }
        String[] lines = baseExpression.split("\\r?\\n", -1);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < lines.length; i++) {
            String line = lines[i];
            if (i == lines.length - 1 && line.isEmpty()) {
                continue;
            }
            sb.append("  ").append(line).append("\n");
        }
        return sb.toString();
    }

    private void appendLoadStatement(StringBuilder script, String outputDf, JsonNode step) throws Exception {
        String loadExpr = stepBuilder.buildLoad(step);
        if (loadExpr == null || loadExpr.isEmpty()) {
            return;
        }

        script.append(outputDf).append(" = ").append(loadExpr).append("\n");
    }

    private void appendTransformation(StringBuilder script, String outputDf, String inputDf, String suffix) {
        script.append(outputDf).append(" = ").append(inputDf);

        if (suffix != null) {
            String trimmed = suffix.trim();
            if (!trimmed.isEmpty()) {
                script.append(trimmed);
            }
        }

        script.append("\n");
    }

    private String buildTransformationSuffix(String opName, JsonNode step) throws Exception {
        switch (opName) {
            case "select":
                return stepBuilder.buildSelect(step);
            case "withColumn":
                return stepBuilder.buildWithColumn(step);
            case "withColumns":
                return stepBuilder.buildWithColumns(step);
            case "filter":
            case "where":
                return stepBuilder.buildFilter(step);
            case "join":
                return stepBuilder.buildJoin(step);
            case "groupBy":
                return stepBuilder.buildGroupBy(step);
            case "agg":
                return stepBuilder.buildAgg(step);
            case "orderBy":
            case "sort":
                return stepBuilder.buildOrderBy(step);
            case "toJSON":
                return stepBuilder.buildToJson(step);
            case "limit":
                return stepBuilder.buildLimit(step);
            case "dropDuplicates":
                return stepBuilder.buildDropDuplicates(step);
            case "repartition":
                return stepBuilder.buildRepartition(step);
            case "coalesce":
                return stepBuilder.buildCoalesce(step);
            case "sample":
                return stepBuilder.buildSample(step);
            case "drop":
                return stepBuilder.buildDrop(step);
            case "withColumnRenamed":
                return stepBuilder.buildWithColumnRenamed(step);
            default:
                return stepBuilder.buildDefaultStep(opName, step);
        }
    }

    private String resolveNodeName(JsonNode step) {
        if (step == null) {
            return null;
        }

        String opName = StringUtil.getText(step, "node", null);
        if (opName == null) {
            opName = StringUtil.getText(step, "step", null);
        }
        return opName;
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
