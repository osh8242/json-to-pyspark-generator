// 파일: src/main/java/com/douzone/platform/recipe/step/StepHandler.java
package com.douzone.platform.recipe.step;

import com.douzone.platform.recipe.builder.StepBuilder;
import com.douzone.platform.recipe.codegen.CodegenContext;

/**
 * node별 코드 생성 전략 인터페이스.
 */
public interface StepHandler {

    StepKind kind();

    OutputPolicy outputPolicy();

    PersistPolicy persistPolicy();

    /**
     * TOP_LEVEL/CHAIN 지원 여부
     */
    boolean supports(BuildMode mode);

    /**
     * 처리하는 node 이름
     */
    boolean matches(String node);

    /**
     * node에 맞는 코드 조각 생성
     */
    StepEmit emit(StepRequest req, StepBuilder sb, CodegenContext ctx) throws Exception;
}
