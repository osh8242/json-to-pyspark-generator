// 파일: src/main/java/com/douzone/platform/recipe/step/handlers/LoadStepHandler.java
package com.douzone.platform.recipe.step.handlers;

import com.douzone.platform.recipe.builder.StepBuilder;
import com.douzone.platform.recipe.codegen.CodegenContext;
import com.douzone.platform.recipe.step.*;

public class LoadStepHandler implements StepHandler {

    @Override
    public StepKind kind() {
        return StepKind.SOURCE;
    }

    @Override
    public OutputPolicy outputPolicy() {
        return OutputPolicy.REQUIRED;
    }

    @Override
    public PersistPolicy persistPolicy() {
        return PersistPolicy.ALLOWED;
    }

    @Override
    public boolean supports(BuildMode mode) {
        return true; // TOP_LEVEL, CHAIN 모두 허용(서브체인에서도 load로 baseExpr 지정 가능)
    }

    @Override
    public boolean matches(String node) {
        return "load".equals(node);
    }

    @Override
    public StepEmit emit(StepRequest req, StepBuilder sb, CodegenContext ctx) {
        // StepBuilder.buildLoad는 "spark.read....\n" 같은 표현식 문자열을 반환(기존 로직 유지)
        String expr = sb.buildLoad(req.getRawStep());
        return StepEmit.source(expr);
    }
}
