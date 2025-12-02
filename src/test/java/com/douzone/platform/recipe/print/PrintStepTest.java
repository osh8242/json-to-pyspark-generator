package com.douzone.platform.recipe.print;

import com.douzone.platform.recipe.PySparkChainGenerator;
import com.douzone.platform.recipe.exception.RecipeStepException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.douzone.platform.recipe.util.TestUtil.printTestInfo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * description    : print 스텝에 대한 코드 생성 테스트
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2025. 9. 18.        osh8242       최초 생성
 */
public class PrintStepTest {

    @Test
    @DisplayName("Print: 단일 변수 - 문자열 arg (\"df_count\")")
    void testPrintWithTextVar() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"print\",\n"
                + "      \"params\": {\n"
                + "        \"args\": [\"df_count\"]\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected = "print(df_count)\n";
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testPrintWithTextVar", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Print: 단일 변수 - {kind: var, name: df_count}")
    void testPrintWithVarObject() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"print\",\n"
                + "      \"params\": {\n"
                + "        \"args\": [\n"
                + "          { \"kind\": \"var\", \"name\": \"df_count\" }\n"
                + "        ]\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected = "print(df_count)\n";
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testPrintWithVarObject", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Print: literal 문자열 출력")
    void testPrintWithLiteral() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"print\",\n"
                + "      \"params\": {\n"
                + "        \"args\": [\n"
                + "          { \"kind\": \"literal\", \"value\": \"hello\" }\n"
                + "        ]\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected = "print(\"hello\")\n";
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testPrintWithLiteral", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Print: 한글 literal 문자열 출력")
    void testPrintWithKoreanLiteral() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"print\",\n"
                + "      \"params\": {\n"
                + "        \"args\": [\n"
                + "          { \"kind\": \"literal\", \"value\": \"작업 시작\" }\n"
                + "        ]\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected = "print(\"작업 시작\")\n";
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testPrintWithKoreanLiteral", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Print: sep, end 옵션 포함")
    void testPrintWithSepAndEnd() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"print\",\n"
                + "      \"params\": {\n"
                + "        \"args\": [\"df1_count\", \"df2_count\"],\n"
                + "        \"sep\": \" | \",\n"
                + "        \"end\": \"!!\"\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected = "print(df1_count, df2_count, sep=\" | \", end=\"!!\")\n";
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testPrintWithSepAndEnd", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Print: params 없이 사용 → print()")
    void testPrintWithoutParams() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"print\"\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected = "print()\n";
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testPrintWithoutParams", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Print: params는 있으나 args 없음 → print()")
    void testPrintWithEmptyParams() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"print\",\n"
                + "      \"params\": {\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected = "print()\n";
        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testPrintWithEmptyParams", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Print: var kind인데 name 누락 시 예외")
    void testPrintVarMissingNameShouldFail() {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"print\",\n"
                + "      \"params\": {\n"
                + "        \"args\": [\n"
                + "          { \"kind\": \"var\" }\n"
                + "        ]\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        assertThrows(RecipeStepException.class,
                () -> PySparkChainGenerator.generate(json));
    }

    @Test
    @DisplayName("Print: 지원하지 않는 kind 사용 시 예외")
    void testPrintWithUnknownKindShouldFail() {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"print\",\n"
                + "      \"params\": {\n"
                + "        \"args\": [\n"
                + "          { \"kind\": \"unknown\", \"name\": \"x\" }\n"
                + "        ]\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        assertThrows(RecipeStepException.class,
                () -> PySparkChainGenerator.generate(json));
    }

    @Test
    @DisplayName("Print: arg 타입이 문자열/객체가 아닌 경우 예외")
    void testPrintWithInvalidArgTypeShouldFail() {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"print\",\n"
                + "      \"params\": {\n"
                + "        \"args\": [ 123 ]\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        assertThrows(RecipeStepException.class,
                () -> PySparkChainGenerator.generate(json));
    }
}
