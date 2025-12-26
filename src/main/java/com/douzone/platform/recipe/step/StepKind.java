// 파일: src/main/java/com/douzone/platform/recipe/step/StepKind.java
package com.douzone.platform.recipe.step;

/**
 * 스텝이 생성하는 코드의 성격을 고정된 분류로 모델링.
 */
public enum StepKind {
    SOURCE,         // spark.read... 처럼 "base DF 표현식"
    DF_TRANSFORM,   // .select()/.filter() 처럼 체인 프래그먼트
    ACTION_VOID,    // show/save/print 처럼 반환 없는 statement
    ACTION_SCALAR   // count/first/isEmpty 처럼 값 반환(assignment 또는 단독호출)
}
