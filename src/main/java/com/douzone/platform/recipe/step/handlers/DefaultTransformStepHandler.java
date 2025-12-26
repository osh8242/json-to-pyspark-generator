// 파일: src/main/java/com/douzone/platform/recipe/step/handlers/DefaultTransformStepHandler.java
package com.douzone.platform.recipe.step.handlers;

import com.douzone.platform.recipe.builder.StepBuilder;
import com.douzone.platform.recipe.codegen.CodegenContext;
import com.douzone.platform.recipe.step.*;

/**
 * registry에 매칭되는 handler가 없을 때 fallback.
 * - 기존 PySparkChainGenerator의 default: stepBuilder.buildDefaultStep(opName,node) 유지 목적
 */
public class DefaultTransformStepHandler implements StepHandler {

    @Override
    public StepKind kind() {
        return StepKind.DF_TRANSFORM;
    }

    @Override
    public OutputPolicy outputPolicy() {
        return OutputPolicy.OPTIONAL;
    }

    @Override
    public PersistPolicy persistPolicy() {
        return PersistPolicy.ALLOWED;
    }

    @Override
    public boolean supports(BuildMode mode) {
        return true;
    }

    @Override
    public boolean matches(String node) {
        // 실제 매칭은 registry에서 "없으면 defaultHandler"로 처리
        return false;
    }

    @Override
    public StepEmit emit(StepRequest req, StepBuilder sb, CodegenContext ctx) {
        String frag = sb.buildDefaultStep(req.getNode(), req.getRawStep());
        return StepEmit.chain(frag);
    }
}
