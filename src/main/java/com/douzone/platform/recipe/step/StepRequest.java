// 파일: src/main/java/com/douzone/platform/recipe/step/StepRequest.java
package com.douzone.platform.recipe.step;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Handler가 필요한 입력 정보를 표준화한 요청 객체.
 */
@Getter
@AllArgsConstructor
public class StepRequest {

    private final String node;
    private final String inputDf;
    private final String output; // nullable
    private final boolean persist;
    private final BuildMode mode;
    private final JsonNode rawStep;

}
