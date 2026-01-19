package com.douzone.platform.recipe;

import com.fasterxml.jackson.databind.ObjectMapper;
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
        String json = "{\"steps\":[{\"node\":\"select\",\"input\":\"df_278157_a726747f7fc54b99bdc50b77de9ac1b3\",\"output\":\"df_278157_f20c2a62c1da4ce1abea9774ddc37f26\",\"persist\":true,\"params\":{\"columns\":[{\"expr\":{\"type\":\"func\",\"name\":\"substring\",\"args\":[{\"type\":\"col\",\"name\":\"PTNO\"},{\"type\":\"lit\",\"value\":1},{\"type\":\"lit\",\"value\":4}]},\"alias\":\"aa\"},{\"expr\":{\"type\":\"col\",\"name\":\"ptno\"}},{\"expr\":{\"type\":\"col\",\"name\":\"mdrp_no\"}},{\"expr\":{\"type\":\"col\",\"name\":\"mdcr_ymd\"}}]}}]})";
        System.out.println("json = " + json);
        String generate = PySparkChainGenerator.generate(json);
        System.out.println("============ generate ============\n" + generate);
    }
}
