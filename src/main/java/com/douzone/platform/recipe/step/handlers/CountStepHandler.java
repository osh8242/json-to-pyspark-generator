// 파일: src/main/java/com/douzone/platform/recipe/step/handlers/CountStepHandler.java
package com.douzone.platform.recipe.step.handlers;

import com.douzone.platform.recipe.builder.StepBuilder;
import com.douzone.platform.recipe.codegen.CodegenContext;
import com.douzone.platform.recipe.step.*;

/**
 * count는 값 반환 액션이며 output이 필수(기존 StepBuilder.buildCount가 강제).
 */
public class CountStepHandler implements StepHandler {

    @Override
    public StepKind kind() {
        return StepKind.ACTION_SCALAR;
    }

    @Override
    public OutputPolicy outputPolicy() {
        return OutputPolicy.REQUIRED;
    }

    @Override
    public PersistPolicy persistPolicy() {
        return PersistPolicy.FORBIDDEN;
    }

    @Override
    public boolean supports(BuildMode mode) {
        return mode == BuildMode.TOP_LEVEL; // 서브체인에서 count 금지
    }

    @Override
    public boolean matches(String node) {
        return "count".equals(node);
    }

    @Override
    public StepEmit emit(StepRequest req, StepBuilder sb, CodegenContext ctx) {
        String stmt = sb.buildCount(req.getRawStep());
        return StepEmit.stmt(StepKind.ACTION_SCALAR, stmt);
    }
}
