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
                "  \"steps\": [\n" +
                "    {\n" +
                "      \"node\": \"load\",\n" +
                "      \"output\": \"df\",\n" +
                "      \"params\": {\n" +
                "        \"table\": \"custom_60106.t_821551_9ea466b673664ef6909550de9ab5375b\",\n" +
                "        \"type\": \"custom\",\n" +
                "        \"source\": \"postgres\",\n" +
                "        \"url\": \"jdbc:postgresql://10.70.167.15:5432/datamart\",\n" +
                "        \"user\": \"osh8242\",\n" +
                "        \"password\": \"Dhtmdghks1@\",\n" +
                "        \"driver\": \"org.postgresql.Driver\",\n" +
                "        \"predicates\": \"[f'abs(hashtext(ctid::text)) % 24 = {i}' for i in range(24)]\"\n" +
                "      }\n" +
                "    }\n" +
                "  ]\n" +
                "}";
        System.out.println("json = " + json);
        String generate = PySparkChainGenerator.generate(json);
        System.out.println("============ generate ============\n" + generate);
    }
}
