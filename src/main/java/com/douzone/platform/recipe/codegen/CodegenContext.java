// 파일: src/main/java/com/douzone/platform/recipe/codegen/CodegenContext.java
package com.douzone.platform.recipe.codegen;

import lombok.Getter;

/**
 * 코드 생성 공용 컨텍스트.
 * - 기본 input df 이름 등 "전역 기본값"을 담는다.
 * - 향후 F alias 가변화 같은 설정도 여기로 확장 가능.
 */
@Getter
public class CodegenContext {

    private final String defaultInputDf;

    public CodegenContext(String defaultInputDf) {
        this.defaultInputDf = defaultInputDf;
    }

}
