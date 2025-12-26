// 파일: src/main/java/com/douzone/platform/recipe/step/StepRequestFactory.java
package com.douzone.platform.recipe.step;

import com.douzone.platform.recipe.codegen.CodegenContext;
import com.douzone.platform.recipe.exception.RecipeStepException;
import com.douzone.platform.recipe.util.StringUtil;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Json step -> StepRequest 변환을 중앙화.
 */
public class StepRequestFactory {

    private StepRequestFactory() {}

    public static StepRequest fromTopLevelStep(JsonNode step, CodegenContext ctx) {
        return fromStep(step, BuildMode.TOP_LEVEL, ctx.getDefaultInputDf(), ctx);
    }

    public static StepRequest fromChainStep(JsonNode step, String chainInputDf, CodegenContext ctx) {
        // chainInputDf는 "right JSON"의 기본 inputDf로 들어온 값이므로,
        // step에 input이 없으면 chainInputDf를 기본으로 사용
        return fromStep(step, BuildMode.CHAIN, chainInputDf, ctx);
    }

    private static StepRequest fromStep(JsonNode step, BuildMode mode, String defaultInputDf, CodegenContext ctx) {
        if (step == null || step.isNull()) {
            throw new RecipeStepException("Step must not be null.");
        }

        String node = StringUtil.getText(step, "node", null);
        if (!StringUtil.hasText(node)) {
            throw new RecipeStepException("Each step requires non-empty 'node'.");
        }

        String inputDf = StringUtil.getText(step, "input", defaultInputDf);
        String output = StringUtil.getText(step, "output", null);

        boolean persist = false;
        JsonNode p = step.get("persist");
        if (p != null && p.isBoolean()) {
            persist = p.asBoolean(false);
        }

        return new StepRequest(node, inputDf, output, persist, mode, step);
    }
}
