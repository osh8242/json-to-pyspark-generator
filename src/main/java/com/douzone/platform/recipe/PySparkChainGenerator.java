package com.douzone.platform.recipe;

import com.douzone.platform.recipe.builder.StepBuilder;
import com.douzone.platform.recipe.exception.RecipeStepException;
import com.douzone.platform.recipe.util.StringUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import lombok.Getter;

import java.util.HashSet;
import java.util.Set;

/**
 * JSON 기반 레시피를 PySpark 코드로 변환하는 메인 엔진.
 * <p>
 * - 외부 진입점: static generate(String / JsonNode)
 * - steps 배열을 순차적으로 실행 가능한 PySpark 코드로 변환
 * - join 서브쿼리, load/save, show 등 특수 스텝 지원
 */
public class PySparkChainGenerator {

    private final StepBuilder stepBuilder;
    private final ObjectMapper om = new ObjectMapper();

    // 생성자에서 StepBuilder를 초기화하며, 재귀 호출을 위해 자기 자신의 참조를 넘겨줍니다.
    public PySparkChainGenerator() {
        this.stepBuilder = new StepBuilder(this);
    }

    /**
     * 외부에서 호출하는 정적 진입점 메서드입니다. (문자열 JSON)
     */
    public static String generate(String json) throws Exception {
        if (json == null || json.trim().isEmpty()) {
            throw new RecipeStepException("Recipe JSON must not be empty.");
        }
        PySparkChainGenerator generator = new PySparkChainGenerator();
        return generator.generatePySparkCode(json);
    }

    /**
     * 외부에서 호출하는 정적 진입점 메서드입니다. (JsonNode)
     */
    public static String generate(JsonNode json) throws Exception {
        if (json == null) {
            throw new RecipeStepException("Recipe JSON must not be null.");
        }
        PySparkChainGenerator generator = new PySparkChainGenerator();
        return generator.generatePySparkCode(json);
    }

    /**
     * JSON 파라미터에서 DataFrame/테이블/쿼리 식별자를 추출합니다.
     * <p>
     * - input 필드 (기본 df)
     * - load step의 table/dbtable/query 등
     * - join step의 right (문자열 또는 sub-JSON input)
     * <p>
     * 을 재귀적으로 수집하여 중복 없는 Set 으로 반환합니다.
     *
     * @param json JSON 문자열
     * @return 식별자 이름들의 Set (예: {"df", "orders_df", "users_df", "catalog.db.table", "public.orders"})
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
     * JSON 문자열을 받아 PySpark 코드로 변환합니다.
     */
    public String generatePySparkCode(String json) throws Exception {
        JsonNode root = om.readTree(json);
        return generatePySparkCode(root);
    }

    /**
     * JsonNode를 받아 PySpark 코드로 변환합니다.
     * - 상단에 import 헤더를 포함합니다.
     * - steps 배열을 순차적으로 처리하여 body를 생성합니다.
     */
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

        // 1) 각 step 을 순차적으로 코드로 변환
        for (JsonNode node : steps) {
            String opName = StringUtil.getText(node, "node", null);
            if (opName == null) continue;

            boolean persist = getPersistFlag(node);

            String inputDf = StringUtil.getText(node, "input", "df");
            String outputDf = StringUtil.getText(node, "output", null);

            // 1-1) Action 류: show / print / count / save / load
            switch (opName) {
                case "show":
                    appendWithOptionalPersist(script, stepBuilder.buildShowAction(node), false);
                    continue;
                case "print":
                    appendWithOptionalPersist(script, stepBuilder.buildPrint(node), false);
                    continue;
                case "count":
                    appendWithOptionalPersist(script, stepBuilder.buildCount(node), false);
                    continue;
                case "save":
                    appendWithOptionalPersist(script, stepBuilder.buildSave(node), false);
                    continue;
                case "load": {
                    if (!StringUtil.hasText(outputDf)) {
                        throw new RecipeStepException("load step requires non-empty 'output'.");
                    }
                    String loadExpr = stepBuilder.buildLoad(node);
                    String line = outputDf + " = " + loadExpr;
                    appendWithOptionalPersist(script, line, persist); // ★ load에도 persist 지원
                    continue;
                }
                default:
                    // 나머지는 아래에서 일반 변환 스텝으로 처리
            }

            // 2) 나머지 node 들은 변환이므로 "out = in.xxx()" 형태로 생성
            if (!StringUtil.hasText(outputDf)) {
                outputDf = inputDf; // output 이 없으면 in-place 갱신
            }

            StringBuilder line = new StringBuilder();
            line.append(outputDf).append(" = ").append(inputDf);

            // 뒤에 체인 메서드 붙이기
            switch (opName) {
                case "select":
                    line.append(stepBuilder.buildSelect(node));
                    break;
                case "withColumn":
                    line.append(stepBuilder.buildWithColumn(node));
                    break;
                case "withColumns":
                    line.append(stepBuilder.buildWithColumns(node));
                    break;
                case "filter":
                case "where":
                    line.append(stepBuilder.buildFilter(node));
                    break;
                case "fileFilter":
                    line.append(stepBuilder.buildFileFilter(node));
                    break;
                case "join":
                    line.append(stepBuilder.buildJoin(node));
                    break;
                case "groupBy":
                    line.append(stepBuilder.buildGroupBy(node));
                    break;
                case "agg":
                    line.append(stepBuilder.buildAgg(node));
                    break;
                case "orderBy":
                case "sort":
                    line.append(stepBuilder.buildOrderBy(node));
                    break;
                case "limit":
                    line.append(stepBuilder.buildLimit(node));
                    break;
                case "distinct":
                    line.append(".distinct()\n");
                    break;
                case "dropDuplicates":
                    line.append(stepBuilder.buildDropDuplicates(node));
                    break;
                case "drop":
                    line.append(stepBuilder.buildDrop(node));
                    break;
                case "withColumnRenamed":
                    line.append(stepBuilder.buildWithColumnRenamed(node));
                    break;
                default:
                    line.append(stepBuilder.buildDefaultStep(opName, node));
                    break;
            }

            // ★ 최종 라인 끝에 persist 적용
            appendWithOptionalPersist(script, line.toString(), persist);
        }
        return script.toString();
    }

    /**
     * steps 배열을 받아 체인 메서드 문자열을 생성하는 재귀적 핵심 로직입니다.
     * 이 메서드는 순수하게 메서드 체인(.filter(...).join(...)) 부분만 생성합니다.
     * <p>
     * join 서브쿼리(right JSON) 등의 상황에서 사용됩니다.
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
                    // 서브 체인의 소스 DF 지정
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
                case "fileFilter":
                    sb.append(stepBuilder.buildFileFilter(step));
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
                case "print":
                    // print도 action이므로 체인에 포함하지 않음
                    break;
                case "count":
                    throw new RecipeStepException("count는 action(df.count())이므로 sub-chain 내부에서 사용할 수 없습니다.");
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
     * steps JSON 노드에서 DataFrame/테이블 식별자를 재귀적으로 수집합니다.
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
                } else if ("join".equals(opName)) {
                    // join step: right 처리
                    JsonNode rightNode = getStepField(step, "right");
                    if (rightNode != null && !rightNode.isNull()) {
                        if (rightNode.isTextual()) {
                            // right가 문자열: 식별자 추가
                            tables.add(rightNode.asText());
                        } else if (rightNode.isObject()) {
                            // right가 객체: input 추가 + 재귀 steps 검사
                            String subInput = StringUtil.getText(rightNode, "input", "df");
                            tables.add(subInput);
                            collectTables(rightNode, tables);  // sub-JSON 재귀
                        }
                    }
                }
                // 다른 step은 별도 식별자 생성 안 하므로 무시
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

    private boolean getPersistFlag(JsonNode step) {
        if (step == null || step.isNull()) return false;

        JsonNode direct = step.get("persist");
        if (direct != null && direct.isBoolean()) {
            return direct.asBoolean(false);
        }

        return false;
    }

    /**
     * code 문자열의 "마지막 개행(\n/\r\n) 직전"에 .persist()를 삽입하고,
     * 개행이 없으면 마지막에 \n을 보장합니다.
     */
    private void appendWithOptionalPersist(StringBuilder out, String code, boolean persist) {
        if (code == null || code.isEmpty()) return;

        int cut = code.length();
        while (cut > 0) {
            char c = code.charAt(cut - 1);
            if (c == '\n' || c == '\r') cut--;
            else break;
        }

        String core = code.substring(0, cut);
        String tail = code.substring(cut); // 원래 달려있던 개행들

        if (persist) {
            String trimmed = core.trim();
            if (!trimmed.endsWith(".persist()")) {
                core = core + ".persist()";
            }
        }

        if (tail.isEmpty()) tail = "\n";
        out.append(core).append(tail);
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
