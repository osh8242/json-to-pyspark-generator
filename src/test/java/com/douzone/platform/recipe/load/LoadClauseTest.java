package com.douzone.platform.recipe.load;

import com.douzone.platform.recipe.PySparkChainGenerator;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.douzone.platform.recipe.util.TestUtil.printTestInfo;

class LoadClauseTest {

    @Test
    @DisplayName("load - iceberg source")
    void testLoadIceberg_setsBaseExpression() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"step\": \"load\",\n"
                + "      \"source\": \"iceberg\",\n"
                + "      \"catalog\": \"dev\",\n"
                + "      \"database\": \"sftp-60106\",\n"
                + "      \"table\": \"orders\"\n"
                + "    },\n"
                + "    {\n"
                + "      \"step\": \"select\",\n"
                + "      \"columns\": [\n"
                + "        { \"expr\": { \"type\": \"col\", \"name\": \"order_id\" } }\n"
                + "      ]\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String actual = PySparkChainGenerator.generate(json);
        String expected = String.join("",
                "result_df = (\n",
                "  spark.read.table(\"dev.sftp-60106.orders\")\n",
                "  .select(F.col(\"order_id\"))\n",
                ")\n");

        printTestInfo("testLoadIceberg_setsBaseExpression", json, actual);
        Assertions.assertThat(actual).isEqualTo(expected);
    }

    @Test
    @DisplayName("load - postgres source (host/port -> url 변환)")
    void testLoadPostgres_buildsJdbcOptionsFromHostPort() throws Exception {
        String json = "{\n"
                + "  \"input\": \"df\",\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"step\": \"load\",\n"
                + "      \"source\": \"postgres\",\n"
                + "      \"host\": \"localhost\",\n"
                + "      \"port\": \"5432\",\n"
                + "      \"database\": \"sample\",\n"
                + "      \"table\": \"public.orders\",\n"
                + "      \"user\": \"app\",\n"
                + "      \"password\": \"secret\",\n"
                + "      \"driver\": \"org.postgresql.Driver\"\n"
                + "    },\n"
                + "    { \"step\": \"limit\", \"n\": 10 }\n"
                + "  ]\n"
                + "}";

        String actual = PySparkChainGenerator.generate(json);
        String expected = String.join("",
                "result_df = (\n",
                "  spark.read.format(\"jdbc\")\n",
                "    .option(\"url\", \"jdbc:postgresql://localhost:5432/sample\")\n",
                "    .option(\"dbtable\", \"public.orders\")\n",
                "    .option(\"user\", \"app\")\n",
                "    .option(\"password\", \"secret\")\n",
                "    .option(\"driver\", \"org.postgresql.Driver\")\n",
                "    .load()\n",
                "  .limit(10)\n",
                ")\n");

        printTestInfo("testLoadPostgres_buildsJdbcOptionsFromHostPort", json, actual);
        Assertions.assertThat(actual).isEqualTo(expected);
    }

    @Test
    @DisplayName("load - postgres source (명시적 URL 우선)")
    void testLoadPostgres_respectsExplicitUrl() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"step\": \"load\",\n"
                + "      \"source\": \"postgres\",\n"
                + "      \"url\": \"jdbc:postgresql://external-host:9999/custom\",\n"
                + "      \"host\": \"should-not-appear\",\n"
                + "      \"port\": \"4444\",\n"
                + "      \"database\": \"ignored\",\n"
                + "      \"table\": \"sales\"\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testLoadPostgres_respectsExplicitUrl", json, actual);
        Assertions.assertThat(actual)
                .contains(".option(\"url\", \"jdbc:postgresql://external-host:9999/custom\")")
                .doesNotContain("should-not-appear")
                .doesNotContain("4444")
                .doesNotContain("ignored");
    }

    @Test
    @DisplayName("load - postgres source (추가 options 직렬화)")
    void testLoadPostgres_includesCustomOptions() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"step\": \"load\",\n"
                + "      \"source\": \"postgres\",\n"
                + "      \"url\": \"jdbc:postgresql://localhost:5432/demo\",\n"
                + "      \"table\": \"t\",\n"
                + "      \"options\": {\n"
                + "        \"stringtype\": \"unspecified\",\n"
                + "        \"fetchsize\": \"1000\"\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testLoadPostgres_includesCustomOptions", json, actual);
        Assertions.assertThat(actual)
                .contains(".option(\"stringtype\", \"unspecified\")")
                .contains(".option(\"fetchsize\", \"1000\")");
    }

    @Test
    @DisplayName("load - postgres source (predicate 옵션 적용)")
    void testLoadPostgres_includesPredicateOption() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"step\": \"load\",\n"
                + "      \"source\": \"postgres\",\n"
                + "      \"url\": \"jdbc:postgresql://localhost:5432/demo\",\n"
                + "      \"table\": \"public.orders\",\n"
                + "      \"predicate\": \"created_at >= '2024-01-01'\"\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String actual = PySparkChainGenerator.generate(json);
        String expected = String.join("",
                "result_df = (\n",
                "  spark.read.jdbc(\n",
                "    url=\"jdbc:postgresql://localhost:5432/demo\",\n",
                "    table=\"public.orders\",\n",
                "    predicates=[\"created_at >= '2024-01-01'\"]\n",
                "  )\n",
                ")\n");

        printTestInfo("testLoadPostgres_includesPredicateOption", json, actual);
        Assertions.assertThat(actual).isEqualTo(expected);
    }
}
