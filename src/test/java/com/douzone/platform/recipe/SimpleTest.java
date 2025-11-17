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
                "                \"node\": \"load\",\n" +
                "                \"output\": \"df\",\n" +
                "                \"params\": {\n" +
                "                    \"table\": \"t_821551_9ea466b673664ef6909550de9ab5375b\",\n" +
                "                    \"type\": \"custom\",\n" +
                "                    \"key\": \"0d4bc8e826c771886784223a2b7fc9326df1fdf9a21239d32bcdfa77d07aa3f3\"\n" +
                "                }\n" +
                "            },\n" +
                "            {\n" +
                "                \"input\": \"df\",\n" +
                "                \"node\": \"select\",\n" +
                "                \"output\": \"df_select\",\n" +
                "                \"params\": {\n" +
                "                    \"columns\": [\n" +
                "                        {\n" +
                "                            \"expr\": {\n" +
                "                                \"type\": \"col\",\n" +
                "                                \"name\": \"t1_ptno\"\n" +
                "                            }\n" +
                "                        },\n" +
                "                        {\n" +
                "                            \"expr\": {\n" +
                "                                \"type\": \"col\",\n" +
                "                                \"name\": \"t1_btdt\"\n" +
                "                            }\n" +
                "                        }\n" +
                "                    ]\n" +
                "                }\n" +
                "            },\n" +
                "            {\n" +
                "                \"input\": \"df_select\",\n" +
                "                \"node\": \"show\",\n" +
                "                \"params\": {\n" +
                "                    \"n\": 50\n" +
                "                }\n" +
                "            }\n" +
                "        ]\n" +
                "    }";
        System.out.println("json = " + json);
        String generate = PySparkChainGenerator.generate(json);
        System.out.println("============ generate ============\n" + generate);
    }
}
