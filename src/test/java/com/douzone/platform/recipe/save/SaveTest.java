package com.douzone.platform.recipe.save;

import com.douzone.platform.recipe.PySparkChainGenerator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.douzone.platform.recipe.util.TestUtil.printTestInfo;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * description    :
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2025. 11. 17.        osh8242       최초 생성
 */
public class SaveTest {

    @Test
    @DisplayName("Save(Postgres 기본): options 에서 url/user/password/driver/dbtable 을 읽어 JDBC 저장 코드 생성")
    void testSavePostgresBasic() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"save\",\n"
                + "      \"input\": \"result_df\",\n"
                + "      \"params\": {\n"
                + "        \"source\": \"postgres\",\n"
                + "        \"mode\": \"append\",\n"
                + "        \"options\": {\n"
                + "          \"url\": \"jdbc:postgresql://10.70.167.15:5432/datamart\",\n"
                + "          \"user\": \"service_app\",\n"
                + "          \"password\": \"1234\",\n"
                + "          \"driver\": \"org.postgresql.Driver\",\n"
                + "          \"dbtable\": \"custom_60106.t_778413_64c24a4dba744dec8a8e7eb9273efa84\"\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected =
                "result_df.write.format(\"jdbc\")"
                        + ".option(\"url\", \"jdbc:postgresql://10.70.167.15:5432/datamart\")"
                        + ".option(\"user\", \"service_app\")"
                        + ".option(\"password\", \"1234\")"
                        + ".option(\"driver\", \"org.postgresql.Driver\")"
                        + ".option(\"dbtable\", \"custom_60106.t_778413_64c24a4dba744dec8a8e7eb9273efa84\")"
                        + ".mode(\"append\").save()\n";

        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testSavePostgresBasic", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Save(Postgres + options): batchsize, reWriteBatchedInserts, isolationLevel 포함")
    void testSavePostgresWithOptions() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"save\",\n"
                + "      \"input\": \"result_df\",\n"
                + "      \"params\": {\n"
                + "        \"source\": \"postgres\",\n"
                + "        \"mode\": \"append\",\n"
                + "        \"options\": {\n"
                + "          \"url\": \"jdbc:postgresql://10.70.167.15:5432/datamart\",\n"
                + "          \"user\": \"service_app\",\n"
                + "          \"password\": \"1234\",\n"
                + "          \"driver\": \"org.postgresql.Driver\",\n"
                + "          \"dbtable\": \"custom_60106.t_778413_64c24a4dba744dec8a8e7eb9273efa84\",\n"
                + "          \"batchsize\": \"10000\",\n"
                + "          \"reWriteBatchedInserts\": \"true\",\n"
                + "          \"isolationLevel\": \"NONE\"\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected =
                "result_df.write.format(\"jdbc\")"
                        + ".option(\"url\", \"jdbc:postgresql://10.70.167.15:5432/datamart\")"
                        + ".option(\"user\", \"service_app\")"
                        + ".option(\"password\", \"1234\")"
                        + ".option(\"driver\", \"org.postgresql.Driver\")"
                        + ".option(\"dbtable\", \"custom_60106.t_778413_64c24a4dba744dec8a8e7eb9273efa84\")"
                        + ".option(\"batchsize\", \"10000\")"
                        + ".option(\"reWriteBatchedInserts\", \"true\")"
                        + ".option(\"isolationLevel\", \"NONE\")"
                        + ".mode(\"append\").save()\n";

        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testSavePostgresWithOptions", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Save(Postgres + mode): mode=overwrite 로 저장")
    void testSavePostgresWithCustomMode() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"save\",\n"
                + "      \"input\": \"result_df\",\n"
                + "      \"params\": {\n"
                + "        \"source\": \"postgres\",\n"
                + "        \"mode\": \"overwrite\",\n"
                + "        \"options\": {\n"
                + "          \"url\": \"jdbc:postgresql://10.70.167.15:5432/datamart\",\n"
                + "          \"user\": \"service_app\",\n"
                + "          \"password\": \"1234\",\n"
                + "          \"driver\": \"org.postgresql.Driver\",\n"
                + "          \"dbtable\": \"custom_60106.t_778413_64c24a4dba744dec8a8e7eb9273efa84\",\n"
                + "          \"batchsize\": \"10000\"\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected =
                "result_df.write.format(\"jdbc\")"
                        + ".option(\"url\", \"jdbc:postgresql://10.70.167.15:5432/datamart\")"
                        + ".option(\"user\", \"service_app\")"
                        + ".option(\"password\", \"1234\")"
                        + ".option(\"driver\", \"org.postgresql.Driver\")"
                        + ".option(\"dbtable\", \"custom_60106.t_778413_64c24a4dba744dec8a8e7eb9273efa84\")"
                        + ".option(\"batchsize\", \"10000\")"
                        + ".mode(\"overwrite\").save()\n";

        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testSavePostgresWithCustomMode", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Select 이후 Save(Postgres): 변환 + 저장 체인 테스트 (options 에 url/user/... 포함)")
    void testSelectThenSavePostgres() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"select\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"result_df\",\n"
                + "      \"params\": {\n"
                + "        \"columns\": [\n"
                + "          { \"expr\": { \"type\": \"col\", \"name\": \"ptno\" } }\n"
                + "        ]\n"
                + "      }\n"
                + "    },\n"
                + "    {\n"
                + "      \"node\": \"save\",\n"
                + "      \"input\": \"result_df\",\n"
                + "      \"params\": {\n"
                + "        \"source\": \"postgres\",\n"
                + "        \"mode\": \"append\",\n"
                + "        \"options\": {\n"
                + "          \"url\": \"jdbc:postgresql://10.70.167.15:5432/datamart\",\n"
                + "          \"user\": \"service_app\",\n"
                + "          \"password\": \"1234\",\n"
                + "          \"driver\": \"org.postgresql.Driver\",\n"
                + "          \"dbtable\": \"custom_60106.t_821551_oshtest1234\",\n"
                + "          \"batchsize\": \"10000\",\n"
                + "          \"reWriteBatchedInserts\": \"true\"\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected =
                "result_df = df.select(F.col(\"ptno\"))\n"
                        + "result_df.write.format(\"jdbc\")"
                        + ".option(\"url\", \"jdbc:postgresql://10.70.167.15:5432/datamart\")"
                        + ".option(\"user\", \"service_app\")"
                        + ".option(\"password\", \"1234\")"
                        + ".option(\"driver\", \"org.postgresql.Driver\")"
                        + ".option(\"dbtable\", \"custom_60106.t_821551_oshtest1234\")"
                        + ".option(\"batchsize\", \"10000\")"
                        + ".option(\"reWriteBatchedInserts\", \"true\")"
                        + ".mode(\"append\").save()\n";

        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testSelectThenSavePostgres", json, actual);
        assertEquals(expected, actual);
    }
}
