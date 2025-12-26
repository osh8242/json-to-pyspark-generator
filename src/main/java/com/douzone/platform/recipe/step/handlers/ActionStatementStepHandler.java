// 파일: src/main/java/com/douzone/platform/recipe/step/handlers/ActionStatementStepHandler.java
package com.douzone.platform.recipe.step.handlers;

import com.douzone.platform.recipe.builder.StepBuilder;
import com.douzone.platform.recipe.codegen.CodegenContext;
import com.douzone.platform.recipe.exception.RecipeStepException;
import com.douzone.platform.recipe.step.*;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.HashMap;
import java.util.Map;

/**
 * 반환 없는 액션 스텝(show/print/save 등)을 처리.
 */
public class ActionStatementStepHandler implements StepHandler {

    @FunctionalInterface
    public interface StmtBuilder {
        String build(StepBuilder sb, JsonNode node) throws Exception;
    }

    private final Map<String, StmtBuilder> map = new HashMap<>();

    public static ActionStatementStepHandler defaultActionHandler() {
        ActionStatementStepHandler h = new ActionStatementStepHandler();
        h.map.put("show", (sb, n) -> sb.buildShowAction(n));
        h.map.put("print", (sb, n) -> sb.buildPrint(n));
        h.map.put("save", (sb, n) -> sb.buildSave(n));
        return h;
    }

    @Override
    public StepKind kind() {
        return StepKind.ACTION_VOID;
    }

    @Override
    public OutputPolicy outputPolicy() {
        return OutputPolicy.FORBIDDEN;
    }

    @Override
    public PersistPolicy persistPolicy() {
        return PersistPolicy.FORBIDDEN;
    }

    @Override
    public boolean supports(BuildMode mode) {
        return mode == BuildMode.TOP_LEVEL; // CHAIN에서 액션 금지
    }

    @Override
    public boolean matches(String node) {
        return map.containsKey(node);
    }

    @Override
    public StepEmit emit(StepRequest req, StepBuilder sb, CodegenContext ctx) throws Exception {
        StmtBuilder b = map.get(req.getNode());
        if (b == null) {
            throw new RecipeStepException("ActionStatementStepHandler cannot handle node=" + req.getNode());
        }
        String stmt = b.build(sb, req.getRawStep());
        return StepEmit.stmt(StepKind.ACTION_VOID, stmt);
    }
}
