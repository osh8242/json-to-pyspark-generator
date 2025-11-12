package com.douzone.platform.recipe.load;

import com.douzone.platform.recipe.PySparkChainGenerator;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.douzone.platform.recipe.util.TestUtil.printTestInfo;
import static com.douzone.platform.recipe.util.TestUtil.toNodeJson;

class LoadClauseTest {

    @Test
    @DisplayName("load - iceberg source")
    void testLoadIceberg_setsBaseExpression() throws Exception {
        String json = toNodeJson("{\n"
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
        String expected = String.join("\n",
                "df = spark.read.table(\"dev.sftp-60106.orders\")",
                "df = df.select(F.col(\"order_id\"))",
                "");

        printTestInfo("testLoadIceberg_setsBaseExpression", json, actual);
        Assertions.assertThat(actual).isEqualTo(expected);
    }

    @Test
    @DisplayName("load - postgres source (host/port -> url 변환)")
    void testLoadPostgres_buildsJdbcOptionsFromHostPort() throws Exception {
        String json = toNodeJson("{\n"
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
        String expected = String.join("\n",
                "df = spark.read.jdbc(",
                "    url=\"jdbc:postgresql://localhost:5432/sample\",",
                "    table=\"public.orders\",",
                "    properties={",
                "      \"user\": \"app\",",
                "      \"password\": \"secret\",",
                "      \"driver\": \"org.postgresql.Driver\"",
                "    }",
                ")",
                "df = df.limit(10)",
                "");

        printTestInfo("testLoadPostgres_buildsJdbcOptionsFromHostPort", json, actual);
        Assertions.assertThat(actual).isEqualTo(expected);
    }

    @Test
    @DisplayName("load - postgres source (명시적 URL 우선)")
    void testLoadPostgres_respectsExplicitUrl() throws Exception {
        String json = toNodeJson("{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"step\": \"load\",\n"
                + "      \"source\": \"postgres\",\n"
                + "      \"url\": \"jdbc:postgresql://external-host:9999/custom\",\n"
                + "      \"host\": \"should-not-appear\",\n"
                + "      \"port\": \"4444\",\n"
                + "      \"database\": \"ignored\",\n"
                + "      \"table\": \"sales\",\n"
                + "      \"user\": \"app\",\n"
                + "      \"password\": \"secret\",\n"
                + "      \"driver\": \"org.postgresql.Driver\"\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testLoadPostgres_respectsExplicitUrl", json, actual);
        Assertions.assertThat(actual)
                .contains("spark.read.jdbc(")
                .contains("url=\"jdbc:postgresql://external-host:9999/custom\"")
                .contains("\"driver\": \"org.postgresql.Driver\"")
                .doesNotContain("should-not-appear")
                .doesNotContain("4444")
                .doesNotContain("ignored");
    }

    @Test
    @DisplayName("load - postgres source (추가 options 직렬화)")
    void testLoadPostgres_includesCustomOptions() throws Exception {
        String json = toNodeJson("{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"step\": \"load\",\n"
                + "      \"source\": \"postgres\",\n"
                + "      \"url\": \"jdbc:postgresql://localhost:5432/demo\",\n"
                + "      \"table\": \"t\",\n"
                + "      \"user\": \"app\",\n"
                + "      \"password\": \"secret\",\n"
                + "      \"driver\": \"org.postgresql.Driver\",\n"
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
                .contains("\"stringtype\": \"unspecified\"")
                .contains("\"fetchsize\": \"1000\"")
                .contains("\"user\": \"app\"")
                .contains("\"driver\": \"org.postgresql.Driver\"");
    }

    @Test
    @DisplayName("load - postgres source (predicate 지원)")
    void testLoadPostgres_supportsPredicate() throws Exception {
        String json = toNodeJson("{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"step\": \"load\",\n"
                + "      \"source\": \"postgres\",\n"
                + "      \"url\": \"jdbc:postgresql://localhost:5432/sample\",\n"
                + "      \"table\": \"public.orders\",\n"
                + "      \"user\": \"app\",\n"
                + "      \"password\": \"secret\",\n"
                + "      \"driver\": \"org.postgresql.Driver\",\n"
                + "      \"predicate\": [\n"
                + "        \"order_date >= '2024-01-01'\",\n"
                + "        \"order_date < '2024-02-01'\"\n"
                + "      ]\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String actual = PySparkChainGenerator.generate(json);
        String expected = String.join("\n",
                "df = spark.read.jdbc(",
                "    url=\"jdbc:postgresql://localhost:5432/sample\",",
                "    table=\"public.orders\",",
                "    predicates=[",
                "      \"order_date >= '2024-01-01'\",",
                "      \"order_date < '2024-02-01'\"",
                "    ],",
                "    properties={",
                "      \"user\": \"app\",",
                "      \"password\": \"secret\",",
                "      \"driver\": \"org.postgresql.Driver\"",
                "    }",
                ")",
                "");

        printTestInfo("testLoadPostgres_supportsPredicate", json, actual);
        Assertions.assertThat(actual).isEqualTo(expected);
    }
}
