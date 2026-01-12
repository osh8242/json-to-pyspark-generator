// 파일: src/main/java/com/douzone/platform/recipe/PySparkChainGenerator.java
package com.douzone.platform.recipe;

import com.douzone.platform.recipe.builder.StepBuilder;
import com.douzone.platform.recipe.codegen.CodeWriter;
import com.douzone.platform.recipe.codegen.CodegenContext;
import com.douzone.platform.recipe.exception.RecipeStepException;
import com.douzone.platform.recipe.step.*;
import com.douzone.platform.recipe.util.StringUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.util.Set;

/**
 * JSON 기반 레시피를 PySpark 코드로 변환하는 메인 엔진.
 * <p>
 * 핵심 변경점:
 * - node별 switch 제거
 * - StepRegistry + StepHandler(전략) 기반으로 확장 가능 구조
 * - 스텝을 SOURCE / DF_TRANSFORM / ACTION_VOID / ACTION_SCALAR 로 분류
 * - first/isEmpty 같은 "output optional" 액션 지원
 */
public class PySparkChainGenerator {

    private final StepBuilder stepBuilder;
    private final ObjectMapper om = new ObjectMapper();

    // 새 구조
    private final StepRegistry registry;
    private final CodegenContext context;

    public PySparkChainGenerator() {
        this.context = new CodegenContext("df");
        this.stepBuilder = new StepBuilder(this);
        this.registry = StepRegistry.defaultRegistry();
    }

    /**
     * 외부에서 호출하는 정적 진입점 (문자열 JSON)
     */
    public static String generate(String json) throws Exception {
        if (json == null || json.trim().isEmpty()) {
            throw new RecipeStepException("Recipe JSON must not be empty.");
        }
        PySparkChainGenerator generator = new PySparkChainGenerator();
        return generator.generatePySparkCode(json);
    }

    /**
     * 외부에서 호출하는 정적 진입점 (JsonNode)
     */
    public static String generate(JsonNode json) throws Exception {
        if (json == null) {
            throw new RecipeStepException("Recipe JSON must not be null.");
        }
        PySparkChainGenerator generator = new PySparkChainGenerator();
        return generator.generatePySparkCode(json);
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

        CodeWriter writer = new CodeWriter();

        for (JsonNode step : steps) {
            StepRequest req = StepRequestFactory.fromTopLevelStep(step, context);

            StepHandler handler = registry.get(req.getNode());
            registry.validate(req, handler);

            StepEmit emit = handler.emit(req, stepBuilder, context);

            switch (emit.getKind()) {
                case SOURCE: {
                    String output = req.getOutput();
                    if (!StringUtil.hasText(output)) {
                        throw new RecipeStepException("SOURCE step requires non-empty 'output'. node=" + req.getNode());
                    }
                    writer.appendDfAssignment(output.trim(), emit.getSourceExpr(), req.isPersist());
                    break;
                }
                case DF_TRANSFORM: {
                    // output 없으면 in-place 갱신
                    String outputDf = StringUtil.hasText(req.getOutput()) ? req.getOutput().trim() : req.getInputDf();
                    writer.appendDfChainAssignment(outputDf, req.getInputDf(), emit.getChainFragment(), req.isPersist());
                    break;
                }
                case ACTION_VOID:
                case ACTION_SCALAR: {
                    writer.appendStatement(emit.getStatement());
                    break;
                }
                default:
                    throw new RecipeStepException("Unsupported StepKind: " + emit.getKind());
            }
        }

        return writer.toString();
    }

    /**
     * steps 배열을 받아 체인 메서드 문자열을 생성하는 재귀적 핵심 로직입니다.
     * - join right 서브쿼리에서 사용
     * - CHAIN 모드에서는 ACTION 스텝을 금지(안전장치)
     */
    public ChainBuildResult buildChain(String inputDf, ArrayNode steps) throws Exception {
        String baseExpression = inputDf;
        StringBuilder chain = new StringBuilder();

        if (steps == null) {
            return new ChainBuildResult(baseExpression, "");
        }

        for (JsonNode step : steps) {
            StepRequest req = StepRequestFactory.fromChainStep(step, inputDf, context);

            StepHandler handler = registry.get(req.getNode());
            registry.validate(req, handler);

            StepEmit emit = handler.emit(req, stepBuilder, context);

            if (emit.getKind() == StepKind.SOURCE) {
                // 서브체인의 base DF 표현식을 교체
                baseExpression = CodeWriter.stripTrailingNewlines(emit.getSourceExpr()).trim();
            } else if (emit.getKind() == StepKind.DF_TRANSFORM) {
                chain.append(emit.getChainFragment());
            } else {
                // show/save/count/first/isEmpty 같은 액션이 join subchain에 끼는 것을 방지
                throw new RecipeStepException("Action step is not allowed in CHAIN mode. node=" + req.getNode());
            }
        }

        return new ChainBuildResult(baseExpression, chain.toString());
    }

    /**
     * JSON 노드에서 DataFrame/테이블 식별자를 재귀적으로 수집합니다.
     * - input 필드(기본 df)
     * - load step: table/dbtable/query/namespace 등을 발견하면 추가
     * - join step: right가 문자열이면 추가, 객체면 재귀
     */
    private void collectTables(JsonNode node, Set<String> tables) {
        if (node == null || !node.isObject()) {
            return;
        }

        // input
        String input = StringUtil.getText(node, "input", context.getDefaultInputDf());
        if (StringUtil.hasText(input)) tables.add(input);

        // steps
        JsonNode stepsNode = node.get("steps");
        if (stepsNode != null && stepsNode.isArray()) {
            for (JsonNode step : stepsNode) {
                String op = StringUtil.getText(step, "node", null);
                if (!StringUtil.hasText(op)) continue;

                if ("load".equals(op)) {
                    collectLoadTables(step, tables);
                } else if ("join".equals(op)) {
                    JsonNode params = step.has("params") ? step.get("params") : step;
                    if (params != null) {
                        JsonNode rightNode = params.get("right");
                        if (rightNode != null) {
                            if (rightNode.isTextual()) {
                                tables.add(rightNode.asText());
                            } else if (rightNode.isObject()) {
                                // right가 객체면 input + 재귀
                                String subInput = StringUtil.getText(rightNode, "input", context.getDefaultInputDf());
                                if (StringUtil.hasText(subInput)) tables.add(subInput);
                                collectTables(rightNode, tables);
                            }
                        }
                    }
                }
            }
        }
    }

    private void collectLoadTables(JsonNode step, Set<String> tables) {
        if (step == null) return;
        JsonNode params = step.has("params") ? step.get("params") : step;

        // load에서 흔히 쓰는 식별자 후보들을 넓게 잡아 수집
        String[] keys = {"table", "dbtable", "query", "namespace", "database"};
        for (String k : keys) {
            JsonNode v = params.get(k);
            if (v != null && v.isTextual()) {
                String s = v.asText();
                if (StringUtil.hasText(s)) tables.add(s);
            }
        }
    }

    /**
     * buildChain 결과 반환용 객체
     */
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
}
