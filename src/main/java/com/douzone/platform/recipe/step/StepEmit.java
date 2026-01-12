// 파일: src/main/java/com/douzone/platform/recipe/step/StepEmit.java
package com.douzone.platform.recipe.step;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Handler가 생성한 코드 조각을 표준화한 결과 객체.
 */
@Getter
@AllArgsConstructor
public class StepEmit {

    private final StepKind kind;
    private final String sourceExpr;     // SOURCE
    private final String chainFragment;  // DF_TRANSFORM
    private final String statement;      // ACTION_*

    public static StepEmit source(String expr) {
        return new StepEmit(StepKind.SOURCE, expr, null, null);
    }

    public static StepEmit chain(String fragment) {
        return new StepEmit(StepKind.DF_TRANSFORM, null, fragment, null);
    }

    public static StepEmit stmt(StepKind kind, String statement) {
        return new StepEmit(kind, null, null, statement);
    }

}
