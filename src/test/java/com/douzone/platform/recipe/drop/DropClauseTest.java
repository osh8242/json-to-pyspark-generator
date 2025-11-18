package com.douzone.platform.recipe.drop;

import com.douzone.platform.recipe.PySparkChainGenerator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.douzone.platform.recipe.util.TestUtil.printTestInfo;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * description    : drop() step 테스트
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2025. 10. 24.     osh8242       최초 생성
 */
public class DropClauseTest {

    @Test
    @DisplayName("Drop: 단일 컬럼 제거")
    void testSingleColumnDrop() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"drop\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df_dropped\",\n"
                + "      \"params\": {\n"
                + "        \"cols\": [\"temp_col\"]\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected =
                "df_dropped = df.drop(\"temp_col\")\n";

        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testSingleColumnDrop", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Drop: 여러 컬럼 제거")
    void testMultipleColumnsDrop() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"drop\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df_dropped\",\n"
                + "      \"params\": {\n"
                + "        \"cols\": [\"id\", \"timestamp\", \"unused_flag\"]\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected =
                "df_dropped = df.drop(\"id\", \"timestamp\", \"unused_flag\")\n";

        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testMultipleColumnsDrop", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Drop: 다단계 체인에 포함 (filter 후 drop)")
    void testDropInMultiStepPipeline() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"filter\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df_filtered\",\n"
                + "      \"params\": {\n"
                + "        \"condition\": {\n"
                + "          \"type\": \"op\", \"op\": \">\",\n"
                + "          \"left\": { \"type\": \"col\", \"name\": \"age\" },\n"
                + "          \"right\": { \"type\": \"lit\", \"value\": 25 }\n"
                + "        }\n"
                + "      }\n"
                + "    },\n"
                + "    {\n"
                + "      \"node\": \"drop\",\n"
                + "      \"input\": \"df_filtered\",\n"
                + "      \"output\": \"df_dropped\",\n"
                + "      \"params\": {\n"
                + "        \"cols\": [\"birth_date\", \"ssn\"]\n"
                + "      }\n"
                + "    },\n"
                + "    {\n"
                + "      \"node\": \"limit\",\n"
                + "      \"input\": \"df_dropped\",\n"
                + "      \"output\": \"df_limited\",\n"
                + "      \"params\": {\n"
                + "        \"n\": 100\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected =
                "df_filtered = df.filter((F.col(\"age\") > F.lit(25)))\n" +
                        "df_dropped = df_filtered.drop(\"birth_date\", \"ssn\")\n" +
                        "df_limited = df_dropped.limit(100)\n";

        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testDropInMultiStepPipeline", json, actual);
        assertEquals(expected, actual);
    }
}
