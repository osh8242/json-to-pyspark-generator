package com.douzone.platform.recipe.fileFilter;

import com.douzone.platform.recipe.PySparkChainGenerator;
import com.douzone.platform.recipe.exception.RecipeStepException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.douzone.platform.recipe.util.TestUtil.printTestInfo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * description    : fileFilter 스텝에 대한 코드 생성 테스트
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2025. 9. 19.        osh8242       최초 생성
 */
public class FileFilterTest {

    @Test
    @DisplayName("FileFilter: 기본 케이스 - input/output 명시")
    void testFileFilterBasic() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"fileFilter\",\n"
                + "      \"input\": \"orders_df\",\n"
                + "      \"output\": \"orders_filtered\",\n"
                + "      \"params\": {\n"
                + "        \"bucket\": \"my-bucket\",\n"
                + "        \"objectKey\": \"tmp/order_ids.csv\",\n"
                + "        \"filterColumn\": \"order_id\"\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected =
                "orders_filtered = orders_df.join(\n" +
                        "  spark.read.format(\"csv\")\n" +
                        "    .option(\"header\", \"true\")\n" +
                        "    .option(\"inferSchema\", \"true\")\n" +
                        "    .load(\"s3a://my-bucket/tmp/order_ids.csv\")\n" +
                        "    .select(\"order_id\")\n" +
                        "    .distinct(),\n" +
                        "  \"order_id\",\n" +
                        "  \"inner\"\n" +
                        ")\n";

        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testFileFilterBasic", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("FileFilter: bucket 이 '/' 로 끝나는 경우에도 경로가 올바르게 생성")
    void testFileFilterBucketWithTrailingSlash() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"fileFilter\",\n"
                + "      \"input\": \"orders_df\",\n"
                + "      \"output\": \"orders_filtered\",\n"
                + "      \"params\": {\n"
                + "        \"bucket\": \"my-bucket/\",\n"
                + "        \"objectKey\": \"tmp/order_ids.csv\",\n"
                + "        \"filterColumn\": \"order_id\"\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        // bucket 이 "my-bucket/" 이어도 s3a://my-bucket/tmp/order_ids.csv 로 한 번만 '/' 가 들어가야 함
        String expected =
                "orders_filtered = orders_df.join(\n" +
                        "  spark.read.format(\"csv\")\n" +
                        "    .option(\"header\", \"true\")\n" +
                        "    .option(\"inferSchema\", \"true\")\n" +
                        "    .load(\"s3a://my-bucket/tmp/order_ids.csv\")\n" +
                        "    .select(\"order_id\")\n" +
                        "    .distinct(),\n" +
                        "  \"order_id\",\n" +
                        "  \"inner\"\n" +
                        ")\n";

        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testFileFilterBucketWithTrailingSlash", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("FileFilter: filterColumn 누락 시 예외")
    void testFileFilterMissingFilterColumnShouldFail() {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"fileFilter\",\n"
                + "      \"input\": \"orders_df\",\n"
                + "      \"output\": \"orders_filtered\",\n"
                + "      \"params\": {\n"
                + "        \"bucket\": \"my-bucket\",\n"
                + "        \"objectKey\": \"tmp/order_ids.csv\"\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        // filterColumn 이 없으면 requireText 에 의해 RecipeStepException 발생
        assertThrows(RecipeStepException.class,
                () -> PySparkChainGenerator.generate(json));
    }
}