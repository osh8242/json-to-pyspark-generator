package com.douzone.platform.recipe.util;

/**
 * description    :
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2025. 9. 17.        osh8242       최초 생성
 */
public class TestUtil {
    public static String buildFullScript(String stepCode, String... actionLines) {
        return stepCode;
    }
    /**
     * 테스트 정보(입력 JSON, 생성된 코드)를 콘솔에 출력합니다.
     * @param testName 현재 실행 중인 테스트 이름
     * @param json 입력으로 사용된 JSON 문자열
     * @param actual 생성된 PySpark 코드 문자열
     */
    public static void printTestInfo(String testName, String json, String actual) {
        System.out.println("========== " + testName + " ==========");
        System.out.println("\n[Input JSON]\n" + json);
        System.out.println("\n[Generated PySpark Code]\n" + actual);
        System.out.println("========================================\n");
    }
}
