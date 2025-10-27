package com.douzone.platform.recipe.util;

/**
 * description    :
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2025. 9. 17.        osh8242       최초 생성
 */
public class TestUtil {
    /**
     * 반복되는 PySpark 스크립트의 헤더와 풋터를 생성하여 테스트 코드의 가독성을 높이는 헬퍼 메서드입니다.
     * @param stepCode 테스트할 step의 PySpark 코드 조각
     * @return 완전한 PySpark 스크립트 문자열
     */
    public static String buildFullScript(String stepCode, String... actionLines) {
        StringBuilder sb = new StringBuilder();
        sb.append("from pyspark.sql import functions as F\n\n");
        sb.append("result_df = (\n");
        sb.append("  df\n");
        if (stepCode != null) {
            sb.append(stepCode);
        }
        sb.append(")\n");

        if (actionLines != null) {
            for (String action : actionLines) {
                if (action != null) {
                    sb.append(action);
                }
            }
        }

        return sb.toString();
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
