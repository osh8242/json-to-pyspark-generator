package com.douzone.platform.recipe;

import com.douzone.platform.recipe.builder.StepBuilder;
import com.douzone.platform.recipe.util.StringUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PySparkChainGenerator {

    private static final Set<String> SIMPLE_NO_ARG_ACTIONS = Set.of(
            "count",
            "collect",
            "first",
            "head",
            "tail",
            "toPandas",
            "toLocalIterator",
            "printSchema",
            "explain"
    );

    public static class ChainBuildResult {
        private final String baseExpression;
        private final String chain;

        public ChainBuildResult(String baseExpression, String chain) {
            this.baseExpression = baseExpression;
            this.chain = chain;
        }

        public String getBaseExpression() {
            return baseExpression;
        }

        public String getChain() {
            return chain;
        }
    }

    private static class ActionStep {
        private final String name;
        private final JsonNode node;

        private ActionStep(String name, JsonNode node) {
            this.name = name;
            this.node = node;
        }
    }

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

    /**
     * 실제 코드 생성을 담당하는 인스턴스 메서드입니다.
     * PySpark 스크립트의 전체적인 틀(import, out = ...)을 생성합니다.
     */
    public String generatePySparkCode(String json) throws Exception {
        JsonNode root = om.readTree(json);
        return generatePySparkCode(root);
    }

    public String generatePySparkCode(JsonNode root) throws Exception {
        String inputDf = StringUtil.getText(root, "input", "df");
        String outDf = StringUtil.getText(root, "output", "result_df");
        ArrayNode steps = (ArrayNode) root.get("steps");
        List<ActionStep> actionSteps = new ArrayList<>();
        ArrayNode transformSteps = steps;
        if (steps != null) {
            ArrayNode filtered = om.createArrayNode();
            for (JsonNode step : steps) {
                String opName = StringUtil.getText(step, "step", null);
                if (isShowStep(opName) || isNoArgAction(opName)) {
                    actionSteps.add(new ActionStep(opName, step));
                } else {
                    filtered.add(step);
                }
            }
            transformSteps = filtered;
        }

        ChainBuildResult chainResult = buildChain(inputDf, transformSteps);

        StringBuilder sb = new StringBuilder();
        sb.append("from pyspark.sql import functions as F\n\n");
        sb.append(outDf).append(" = (\n");
        sb.append(formatBaseExpression(chainResult.getBaseExpression()));
        sb.append(chainResult.getChain());
        sb.append(")\n");

        for (ActionStep actionStep : actionSteps) {
            if (isShowStep(actionStep.name)) {
                sb.append(stepBuilder.buildShowAction(actionStep.node, outDf));
            } else {
                sb.append(stepBuilder.buildNoArgAction(actionStep.name, outDf));
            }
        }
        return sb.toString();
    }

    private boolean isShowStep(String opName) {
        return "show".equals(opName);
    }

    private boolean isNoArgAction(String opName) {
        return SIMPLE_NO_ARG_ACTIONS.contains(opName);
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
            String opName = StringUtil.getText(step, "step", null);
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
                    break;
            }
        }

        return new ChainBuildResult(baseExpression, sb.toString());
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
                String opName = StringUtil.getText(step, "step", null);
                if ("join".equals(opName)) {
                    // join step: right 처리
                    JsonNode rightNode = step.get("right");
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

}
