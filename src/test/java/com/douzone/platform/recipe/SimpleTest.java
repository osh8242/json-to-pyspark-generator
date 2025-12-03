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
                "                \"input\": \"df_select\",\n" +
                "                \"node\": \"fileFilter\",\n" +
                "                \"output\": \"df_select_filtered\",\n" +
                "                \"params\": {\n" +
                "                    \"objectKey\": \"tmp/pms/4053ff9a-8ec9-49ce-986c-69885c3de12b\",\n" +
                "                    \"bucket\": \"devicebergrestcatalog\",\n" +
                "                    \"filterColumn\": \"ptno\"\n" +
                "                }\n" +
                "            },\n" +
                "            {\n" +
                "                \"input\": \"df_select_filtered\",\n" +
                "                \"node\": \"show\",\n" +
                "                \"params\": {\n" +
                "                    \"format\": \"csv\",\n" +
                "                    \"n\": 20,\n" +
                "                    \"header\": \"true\"\n" +
                "                }\n" +
                "            }\n" +
                "        ]\n" +
                "    }";
        System.out.println("json = " + json);
        String generate = PySparkChainGenerator.generate(json);
        System.out.println("============ generate ============\n" + generate);
    }
}
