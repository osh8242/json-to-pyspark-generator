package com.douzone.platform.recipe;

import org.junit.jupiter.api.Test;

/**
 * description    :
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2025. 11. 12.        osh8242       최초 생성
 */
public class SimpleTest {
    @Test
    public void simpleTest() throws Exception {
        String json = "{\n" +
                "        \"steps\": [\n" +
                "            {\n" +
                "                \"input\": \"df\",\n" +
                "                \"output\": \"result_df\",\n" +
                "                \"node\": \"count\"\n" +
                "            },\n" +
                "            {\n" +
                "                \"input\": \"result_df\",\n" +
                "                \"node\": \"print\"\n" +
                "            }\n" +
                "        ]\n" +
                "    }";
        System.out.println("json = " + json);
        String generate = PySparkChainGenerator.generate(json);
        System.out.println("============ generate ============\n" + generate);
    }
}
