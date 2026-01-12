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
                "      \"output\": \"df1\",\n" +
                "      \"params\": {\n" +
                "        \"source\": \"iceberg\",\n" +
                "        \"catalog\": \"dev\",\n" +
                "        \"namespace\": \"dev\",\n" +
                "        \"table\": \"F_AP_PTNT_BSIS_INFM\"\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"node\": \"select\",\n" +
                "      \"input\": \"df1\",\n" +
                "      \"output\": \"df1_select\",\n" +
                "      \"params\": {\n" +
                "        \"columns\": [\n" +
                "          { \"expr\": { \"type\": \"col\", \"name\": \"ptno\" } },\n" +
                "          { \"expr\": { \"type\": \"col\", \"name\": \"gend_cd\" } },\n" +
                "          { \"expr\": { \"type\": \"col\", \"name\": \"btdt\" } }\n" +
                "        ]\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"node\": \"filter\",\n" +
                "      \"input\": \"df1_select\",\n" +
                "      \"output\": \"df1_select_filter\",\n" +
                "      \"params\": {\n" +
                "        \"condition\": {\n" +
                "          \"type\": \"op\",\n" +
                "          \"op\": \"=\",\n" +
                "          \"left\": { \"type\": \"col\", \"name\": \"gend_cd\" },\n" +
                "          \"right\": { \"type\": \"lit\", \"value\": \"F\" }\n" +
                "        }\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"node\": \"select\",\n" +
                "      \"input\": \"df1_select_filter\",\n" +
                "      \"output\": \"t1\",\n" +
                "      \"params\": {\n" +
                "        \"columns\": [\n" +
                "          { \"expr\": { \"type\": \"col\", \"name\": \"ptno\" }, \"alias\": \"t1_ptno\" },\n" +
                "          { \"expr\": { \"type\": \"col\", \"name\": \"gend_cd\" }, \"alias\": \"t1_gend_cd\" },\n" +
                "          { \"expr\": { \"type\": \"col\", \"name\": \"btdt\" }, \"alias\": \"t1_btdt\" }\n" +
                "        ]\n" +
                "      }\n" +
                "    },\n" +
                "\n" +
                "    {\n" +
                "      \"node\": \"load\",\n" +
                "      \"output\": \"df2\",\n" +
                "      \"params\": {\n" +
                "        \"source\": \"iceberg\",\n" +
                "        \"catalog\": \"dev\",\n" +
                "        \"namespace\": \"dev\",\n" +
                "        \"table\": \"F_MD_PACLNCT\"\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"node\": \"select\",\n" +
                "      \"input\": \"df2\",\n" +
                "      \"output\": \"t2\",\n" +
                "      \"params\": {\n" +
                "        \"columns\": [\n" +
                "          { \"expr\": { \"type\": \"col\", \"name\": \"mdrp_no\" }, \"alias\": \"t2_mdrp_no\" },\n" +
                "          { \"expr\": { \"type\": \"col\", \"name\": \"ptnt_clnc_dx_sno\" }, \"alias\": \"t2_ptnt_clnc_dx_sno\" },\n" +
                "          { \"expr\": { \"type\": \"col\", \"name\": \"ptno\" }, \"alias\": \"t2_ptno\" },\n" +
                "          { \"expr\": { \"type\": \"col\", \"name\": \"mcdp_cd\" }, \"alias\": \"t2_mcdp_cd\" }\n" +
                "        ]\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"node\": \"join\",\n" +
                "      \"input\": \"t1\",\n" +
                "      \"output\": \"t3\",\n" +
                "      \"params\": {\n" +
                "        \"right\": \"t2\",\n" +
                "        \"how\": \"inner\",\n" +
                "        \"on\": {\n" +
                "          \"type\": \"op\",\n" +
                "          \"op\": \"=\",\n" +
                "          \"left\": { \"type\": \"col\", \"name\": \"t1_ptno\" },\n" +
                "          \"right\": { \"type\": \"col\", \"name\": \"t2_ptno\" }\n" +
                "        }\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"node\": \"distinct\",\n" +
                "      \"input\": \"t3\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"node\": \"filter\",\n" +
                "      \"input\": \"t3\",\n" +
                "      \"output\": \"t3\",\n" +
                "      \"params\": {\n" +
                "        \"condition\": {\n" +
                "          \"type\": \"op\",\n" +
                "          \"op\": \">\",\n" +
                "          \"left\": {\n" +
                "            \"type\": \"func\",\n" +
                "            \"name\": \"to_date\",\n" +
                "            \"args\": [\n" +
                "              { \"type\": \"col\", \"name\": \"t1_btdt\" }\n" +
                "            ]\n" +
                "          },\n" +
                "          \"right\": { \"type\": \"lit\", \"value\": \"2000-01-01\" }\n" +
                "        }\n" +
                "      }\n" +
                "    },\n" +
                "\n" +
                "    {\n" +
                "      \"node\": \"load\",\n" +
                "      \"output\": \"df4\",\n" +
                "      \"params\": {\n" +
                "        \"source\": \"iceberg\",\n" +
                "        \"catalog\": \"dev\",\n" +
                "        \"namespace\": \"dev\",\n" +
                "        \"table\": \"F_MD_PADIAGT\"\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"node\": \"select\",\n" +
                "      \"input\": \"df4\",\n" +
                "      \"output\": \"t4\",\n" +
                "      \"params\": {\n" +
                "        \"columns\": [\n" +
                "          { \"expr\": { \"type\": \"col\", \"name\": \"mdrp_no\" }, \"alias\": \"t4_mdrp_no\" },\n" +
                "          { \"expr\": { \"type\": \"col\", \"name\": \"ptno\" }, \"alias\": \"t4_ptno\" },\n" +
                "          { \"expr\": { \"type\": \"col\", \"name\": \"mcdp_cd\" }, \"alias\": \"t4_mcdp_cd\" },\n" +
                "          { \"expr\": { \"type\": \"col\", \"name\": \"mcdp_abrv\" }, \"alias\": \"t4_mcdp_abrv\" },\n" +
                "          { \"expr\": { \"type\": \"col\", \"name\": \"mcdp_nm\" }, \"alias\": \"t4_mcdp_nm\" }\n" +
                "        ]\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"node\": \"join\",\n" +
                "      \"input\": \"t3\",\n" +
                "      \"output\": \"t5\",\n" +
                "      \"params\": {\n" +
                "        \"right\": \"t4\",\n" +
                "        \"how\": \"inner\",\n" +
                "        \"on\": {\n" +
                "          \"type\": \"op\",\n" +
                "          \"op\": \"=\",\n" +
                "          \"left\": { \"type\": \"col\", \"name\": \"t1_ptno\" },\n" +
                "          \"right\": { \"type\": \"col\", \"name\": \"t4_ptno\" }\n" +
                "        }\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"node\": \"distinct\",\n" +
                "      \"input\": \"t5\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"node\": \"load\",\n" +
                "      \"output\": \"df6\",\n" +
                "      \"params\": {\n" +
                "        \"source\": \"iceberg\",\n" +
                "        \"catalog\": \"dev\",\n" +
                "        \"namespace\": \"dev\",\n" +
                "        \"table\": \"F_AP_PTNT_BSIS_INFM\"\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"node\": \"select\",\n" +
                "      \"input\": \"df6\",\n" +
                "      \"output\": \"df6_select\",\n" +
                "      \"params\": {\n" +
                "        \"columns\": [\n" +
                "          { \"expr\": { \"type\": \"col\", \"name\": \"ptno\" } },\n" +
                "          { \"expr\": { \"type\": \"col\", \"name\": \"gend_cd\" } },\n" +
                "          { \"expr\": { \"type\": \"col\", \"name\": \"btdt\" } }\n" +
                "        ]\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"node\": \"filter\",\n" +
                "      \"input\": \"df6_select\",\n" +
                "      \"output\": \"df6_select_filter\",\n" +
                "      \"params\": {\n" +
                "        \"condition\": {\n" +
                "          \"type\": \"op\",\n" +
                "          \"op\": \"=\",\n" +
                "          \"left\": { \"type\": \"col\", \"name\": \"gend_cd\" },\n" +
                "          \"right\": { \"type\": \"lit\", \"value\": \"F\" }\n" +
                "        }\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"node\": \"select\",\n" +
                "      \"input\": \"df6_select_filter\",\n" +
                "      \"output\": \"t6\",\n" +
                "      \"params\": {\n" +
                "        \"columns\": [\n" +
                "          { \"expr\": { \"type\": \"col\", \"name\": \"ptno\" }, \"alias\": \"t6_ptno\" },\n" +
                "          { \"expr\": { \"type\": \"col\", \"name\": \"gend_cd\" }, \"alias\": \"t6_gend_cd\" },\n" +
                "          { \"expr\": { \"type\": \"col\", \"name\": \"btdt\" }, \"alias\": \"t6_btdt\" }\n" +
                "        ]\n" +
                "      }\n" +
                "    },\n" +
                "\n" +
                "    {\n" +
                "      \"node\": \"load\",\n" +
                "      \"output\": \"t7\",\n" +
                "      \"params\": {\n" +
                "        \"source\": \"iceberg\",\n" +
                "        \"catalog\": \"dev\",\n" +
                "        \"namespace\": \"dev\",\n" +
                "        \"table\": \"F_ME_PTNT_BODY_BMMT\"\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"node\": \"join\",\n" +
                "      \"input\": \"t6\",\n" +
                "      \"output\": \"t8\",\n" +
                "      \"params\": {\n" +
                "        \"right\": \"t7\",\n" +
                "        \"how\": \"inner\",\n" +
                "        \"on\": {\n" +
                "          \"type\": \"op\",\n" +
                "          \"op\": \"=\",\n" +
                "          \"left\": { \"type\": \"col\", \"name\": \"t6_ptno\" },\n" +
                "          \"right\": { \"type\": \"col\", \"name\": \"ptno\" }\n" +
                "        }\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"node\": \"distinct\",\n" +
                "      \"input\": \"t8\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"node\": \"join\",\n" +
                "      \"input\": \"t5\",\n" +
                "      \"output\": \"t9\",\n" +
                "      \"params\": {\n" +
                "        \"right\": \"t8\",\n" +
                "        \"how\": \"inner\",\n" +
                "        \"on\": {\n" +
                "          \"type\": \"op\",\n" +
                "          \"op\": \"=\",\n" +
                "          \"left\": { \"type\": \"col\", \"name\": \"t1_ptno\" },\n" +
                "          \"right\": { \"type\": \"col\", \"name\": \"t6_ptno\" }\n" +
                "        }\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"node\": \"distinct\",\n" +
                "      \"input\": \"t9\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"node\": \"save\",\n" +
                "      \"input\": \"t9\",\n" +
                "      \"params\": {\n" +
                "        \"source\": \"postgres\",\n" +
                "        \"mode\": \"overwrite\",\n" +
                "        \"options\": {\n" +
                "          \"url\": \"jdbc:postgresql://10.70.167.15:5432/datamart\",\n" +
                "          \"user\": \"test\",\n" +
                "          \"password\": \"1234\",\n" +
                "          \"driver\": \"org.postgresql.Driver\",\n" +
                "          \"dbtable\": \"user_custom.aa_teetasdfasyzv\",\n" +
                "          \"batchsize\": \"100\",\n" +
                "          \"isolationLevel\": \"NONE\",\n" +
                "          \"reWriteBatchedInserts\": \"true\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  ]\n" +
                "}\n";
        System.out.println("json = " + json);
        String generate = PySparkChainGenerator.generate(json);
        System.out.println("============ generate ============\n" + generate);
    }
}
