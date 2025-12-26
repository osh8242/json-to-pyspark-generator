// 파일: src/main/java/com/douzone/platform/recipe/step/StepRequest.java
package com.douzone.platform.recipe.step;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Getter;

/**
 * Handler가 필요한 입력 정보를 표준화한 요청 객체.
 */
@Getter
public class StepRequest {

    private final String node;
    private final String inputDf;
    private final String output; // nullable
    private final boolean persist;
    private final BuildMode mode;
    private final JsonNode rawStep;

    public StepRequest(String node, String inputDf, String output, boolean persist, BuildMode mode, JsonNode rawStep) {
        this.node = node;
        this.inputDf = inputDf;
        this.output = output;
        this.persist = persist;
        this.mode = mode;
        this.rawStep = rawStep;
    }

}
