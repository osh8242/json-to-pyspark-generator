// 파일: src/main/java/com/douzone/platform/recipe/step/handlers/TransformStepHandler.java
package com.douzone.platform.recipe.step.handlers;

import com.douzone.platform.recipe.builder.StepBuilder;
import com.douzone.platform.recipe.codegen.CodegenContext;
import com.douzone.platform.recipe.exception.RecipeStepException;
import com.douzone.platform.recipe.step.*;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.HashMap;
import java.util.Map;

/**
 * 대부분의 DF 변환 스텝(.select/.filter/.join/...)을 한 핸들러에서 처리.
 * - node별 switch를 "Map<node, lambda>"로 대체
 * - 새 변환 스텝 추가는 map.put만 추가하면 끝
 */
public class TransformStepHandler implements StepHandler {

    @FunctionalInterface
    public interface ChainBuilder {
        String build(StepBuilder sb, JsonNode node) throws Exception;
    }

    private final Map<String, ChainBuilder> map = new HashMap<>();

    public static TransformStepHandler defaultTransformHandler() {
        TransformStepHandler h = new TransformStepHandler();

        // 변환 스텝들
        h.map.put("select", (sb, n) -> sb.buildSelect(n));
        h.map.put("withColumn", (sb, n) -> sb.buildWithColumn(n));
        h.map.put("withColumns", (sb, n) -> sb.buildWithColumns(n));
        h.map.put("filter", (sb, n) -> sb.buildFilter(n));
        h.map.put("fileFilter", (sb, n) -> sb.buildFileFilter(n));
        h.map.put("join", (sb, n) -> sb.buildJoin(n));
        h.map.put("groupBy", (sb, n) -> sb.buildGroupBy(n));
        h.map.put("agg", (sb, n) -> sb.buildAgg(n));
        h.map.put("orderBy", (sb, n) -> sb.buildOrderBy(n));
        h.map.put("limit", (sb, n) -> sb.buildLimit(n));
        h.map.put("distinct", (sb, n) -> ".distinct()\n");
        h.map.put("dropDuplicates", (sb, n) -> sb.buildDropDuplicates(n));
        h.map.put("drop", (sb, n) -> sb.buildDrop(n));
        h.map.put("withColumnRenamed", (sb, n) -> sb.buildWithColumnRenamed(n));

        return h;
    }

    @Override
    public StepKind kind() {
        return StepKind.DF_TRANSFORM;
    }

    @Override
    public OutputPolicy outputPolicy() {
        // 변환은 output OPTIONAL: 없으면 generator에서 in-place 갱신
        return OutputPolicy.OPTIONAL;
    }

    @Override
    public PersistPolicy persistPolicy() {
        return PersistPolicy.ALLOWED;
    }

    @Override
    public boolean supports(BuildMode mode) {
        return true; // TOP_LEVEL/CHAIN 모두 허용
    }

    @Override
    public boolean matches(String node) {
        return map.containsKey(node);
    }

    @Override
    public StepEmit emit(StepRequest req, StepBuilder sb, CodegenContext ctx) throws Exception {
        ChainBuilder b = map.get(req.getNode());
        if (b == null) {
            throw new RecipeStepException("TransformStepHandler cannot handle node=" + req.getNode());
        }
        String fragment = b.build(sb, req.getRawStep());
        return StepEmit.chain(fragment);
    }
}
