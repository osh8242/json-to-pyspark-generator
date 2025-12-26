// 파일: src/main/java/com/douzone/platform/recipe/step/handlers/FirstStepHandler.java
package com.douzone.platform.recipe.step.handlers;

import com.douzone.platform.recipe.builder.StepBuilder;
import com.douzone.platform.recipe.codegen.CodegenContext;
import com.douzone.platform.recipe.exception.RecipeStepException;
import com.douzone.platform.recipe.step.*;
import com.douzone.platform.recipe.util.StringUtil;

/**
 * first()는 값 반환 액션.
 * - output 있으면 변수에 저장
 * - output 없으면 "_ = df.first()" 형태로 실행만 수행
 */
public class FirstStepHandler implements StepHandler {

    @Override
    public StepKind kind() {
        return StepKind.ACTION_SCALAR;
    }

    @Override
    public OutputPolicy outputPolicy() {
        return OutputPolicy.OPTIONAL;
    }

    @Override
    public PersistPolicy persistPolicy() {
        return PersistPolicy.FORBIDDEN;
    }

    @Override
    public boolean supports(BuildMode mode) {
        return mode == BuildMode.TOP_LEVEL; // 서브체인에서 금지
    }

    @Override
    public boolean matches(String node) {
        return "first".equals(node);
    }

    @Override
    public StepEmit emit(StepRequest req, StepBuilder sb, CodegenContext ctx) {
        String out = req.getOutput();
        String target;
        if (!StringUtil.hasText(out)) {
            target = "_";
        } else {
            out = out.trim();
            if (!StringUtil.isPyIdent(out)) {
                throw new RecipeStepException("first step 'output' must be a valid Python identifier: " + out);
            }
            target = out;
        }

        String inputDf = req.getInputDf();
        String stmt = target + " = " + inputDf + ".first()\n";
        return StepEmit.stmt(StepKind.ACTION_SCALAR, stmt);
    }
}
