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
                "      \"input\": \"employees_df1\",\n" +
                "      \"node\": \"filter\",\n" +
                "      \"params\": {\n" +
                "        \"condition\": {\n" +
                "          \"type\": \"op\",\n" +
                "          \"op\": \">=\",\n" +
                "          \"left\": {\n" +
                "            \"type\": \"col\",\n" +
                "            \"name\": \"age\"\n" +
                "          },\n" +
                "          \"right\": {\n" +
                "            \"type\": \"lit\",\n" +
                "            \"value\": 25\n" +
                "          }\n" +
                "        }\n" +
                "      },\n" +
                "      \"output\": \"employees_df1_filter1\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"input\": \"employees_df1_filter1\",\n" +
                "      \"node\": \"join\",\n" +
                "      \"params\": {\n" +
                "        \"right\": \"departments\",\n" +
                "        \"leftAlias\": \"emp\",\n" +
                "        \"rightAlias\": \"dept\",\n" +
                "        \"on\": {\n" +
                "          \"type\": \"op\",\n" +
                "          \"op\": \"=\",\n" +
                "          \"left\": {\n" +
                "            \"type\": \"col\",\n" +
                "            \"name\": \"dept_id\",\n" +
                "            \"table\": \"emp\"\n" +
                "          },\n" +
                "          \"right\": {\n" +
                "            \"type\": \"col\",\n" +
                "            \"name\": \"id\",\n" +
                "            \"table\": \"dept\"\n" +
                "          }\n" +
                "        }\n" +
                "      },\n" +
                "      \"output\": \"employees_df1_filter1_join1\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"input\": \"employees_df1_filter1_join1\",\n" +
                "      \"node\": \"select\",\n" +
                "      \"params\": {\n" +
                "        \"columns\": [\n" +
                "          {\n" +
                "            \"expr\": {\n" +
                "              \"type\": \"col\",\n" +
                "              \"name\": \"name\",\n" +
                "              \"table\": \"emp\"\n" +
                "            }\n" +
                "          },\n" +
                "          {\n" +
                "            \"expr\": {\n" +
                "              \"type\": \"col\",\n" +
                "              \"name\": \"dept_name\",\n" +
                "              \"table\": \"dept\"\n" +
                "            }\n" +
                "          },\n" +
                "          {\n" +
                "            \"expr\": {\n" +
                "              \"type\": \"col\",\n" +
                "              \"name\": \"salary\",\n" +
                "              \"table\": \"emp\"\n" +
                "            },\n" +
                "            \"alias\": \"monthly_salary\"\n" +
                "          }\n" +
                "        ]\n" +
                "      },\n" +
                "      \"output\": \"employees_df1_filter1_join1_select1\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"input\": \"employees_df1_filter1_join1_select1\",\n" +
                "      \"node\": \"orderBy\",\n" +
                "      \"params\": {\n" +
                "        \"keys\": [\n" +
                "          {\n" +
                "            \"expr\": {\n" +
                "              \"type\": \"col\",\n" +
                "              \"name\": \"monthly_salary\"\n" +
                "            },\n" +
                "            \"asc\": false\n" +
                "          }\n" +
                "        ]\n" +
                "      },\n" +
                "      \"output\": \"employees_df1_filter1_join1_select1_orderby1\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"input\": \"employees_df1_filter1_join1_select1_orderby1\",\n" +
                "      \"node\": \"limit\",\n" +
                "      \"params\": {\n" +
                "        \"n\": 10\n" +
                "      },\n" +
                "      \"output\": \"employees_df1_filter1_join1_select1_orderby1_limit1\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";
        System.out.println("json = " + json);
        String generate = PySparkChainGenerator.generate(json);
        System.out.println("============ generate ============\n" + generate);
    }
}
