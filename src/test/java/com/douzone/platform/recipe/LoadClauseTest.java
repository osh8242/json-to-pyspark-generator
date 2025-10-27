package com.douzone.platform.recipe;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class LoadClauseTest {

    private String generateAndPrint(String json) throws Exception {
        System.out.println("json = " + json);
        System.out.println();
        String code = PySparkChainGenerator.generate(json);
        System.out.println("generated_code = " + code);
        return code;
    }

    @Test
    @DisplayName("load - iceberg source")
    void testLoadIceberg_setsBaseExpression() throws Exception {
        String json = "{\n" +
                "  \"input\": \"df\",\n" +
                "  \"steps\": [\n" +
                "    {\n" +
                "      \"step\": \"load\",\n" +
                "      \"source\": \"iceberg\",\n" +
                "      \"catalog\": \"dev\",\n" +
                "      \"database\": \"sftp-60106\",\n" +
                "      \"table\": \"orders\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"step\": \"select\",\n" +
                "      \"columns\": [\n" +
                "        { \"expr\": { \"type\": \"col\", \"name\": \"order_id\" } }\n" +
                "      ]\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        String code = generateAndPrint(json);

        Assertions.assertThat(code).contains("spark.read.table(\"dev.sftp-60106.orders\")");
        Assertions.assertThat(code).contains(".select(");
        Assertions.assertThat(code).startsWith("from pyspark.sql import functions as F\n\nresult_df = (");
    }

    @Test
    @DisplayName("load - postgres source")
    void testLoadPostgres_setsJdbcOptions() throws Exception {
        String json = "{\n" +
                "  \"input\": \"df\",\n" +
                "  \"steps\": [\n" +
                "    {\n" +
                "      \"step\": \"load\",\n" +
                "      \"source\": \"postgres\",\n" +
                "      \"host\": \"localhost\",\n" +
                "      \"port\": \"5432\",\n" +
                "      \"database\": \"sample\",\n" +
                "      \"table\": \"public.orders\",\n" +
                "      \"user\": \"app\",\n" +
                "      \"password\": \"secret\",\n" +
                "      \"driver\": \"org.postgresql.Driver\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"step\": \"limit\",\n" +
                "      \"n\": 10\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        String code = generateAndPrint(json);

        Assertions.assertThat(code).contains("spark.read.format(\"jdbc\")");
        Assertions.assertThat(code).contains(".option(\"url\", \"jdbc:postgresql://localhost:5432/sample\")");
        Assertions.assertThat(code).contains(".option(\"dbtable\", \"public.orders\")");
        Assertions.assertThat(code).contains(".option(\"user\", \"app\")");
        Assertions.assertThat(code).contains(".option(\"password\", \"secret\")");
        Assertions.assertThat(code).contains(".option(\"driver\", \"org.postgresql.Driver\")");
        Assertions.assertThat(code).contains(".limit(10)");
    }
}
