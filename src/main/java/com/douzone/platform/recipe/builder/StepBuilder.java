package com.douzone.platform.recipe.builder;

import com.douzone.platform.recipe.PySparkChainGenerator;
import com.douzone.platform.recipe.util.StringUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.util.ArrayList;
import java.util.List;

/**
 * description    : pypsark 체인메서드의 각 단계(select, filter, groupBy, agg 등)를 생성하는 빌더
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2025. 9. 17.        osh8242       최초 생성
 */
public class StepBuilder {
    private final ExpressionBuilder expressionBuilder = new ExpressionBuilder();
    private final PySparkChainGenerator mainGenerator;

    // 재귀 호출을 위해 PySparkChainGenerator 인스턴스를 주입받습니다.
    public StepBuilder(PySparkChainGenerator mainGenerator) {
        this.mainGenerator = mainGenerator;
    }

    public String buildLoad(JsonNode node) {
        if (node == null) {
            return null;
        }

        String source = StringUtil.getText(node, "source", null);
        if (source == null) {
            return null;
        }

        switch (source.toLowerCase()) {
            case "iceberg":
                return buildIcebergLoad(node);
            case "postgres":
            case "postgresql":
                return buildPostgresLoad(node);
            default:
                return null;
        }
    }

    private String buildIcebergLoad(JsonNode node) {
        String catalog = StringUtil.getText(node, "catalog", null);
        String database = StringUtil.getText(node, "database", null);
        String table = StringUtil.getText(node, "table", null);

        if (table == null) {
            return null;
        }

        StringBuilder identifier = new StringBuilder();
        if (catalog != null && !catalog.isEmpty()) {
            identifier.append(catalog).append('.');
        }
        if (database != null && !database.isEmpty()) {
            identifier.append(database).append('.');
        }
        identifier.append(table);

        return "spark.read.table(" + StringUtil.pyString(identifier.toString()) + ")";
    }

    private String buildPostgresLoad(JsonNode node) {
        String url = StringUtil.getText(node, "url", null);
        if (url == null || url.isEmpty()) {
            String host = StringUtil.getText(node, "host", "localhost");
            String port = StringUtil.getText(node, "port", "5432");
            String database = StringUtil.getText(node, "database", null);
            if (database != null && !database.isEmpty()) {
                url = "jdbc:postgresql://" + host + ":" + port + "/" + database;
            } else {
                url = "jdbc:postgresql://" + host + ":" + port;
            }
        }

        String table = StringUtil.getText(node, "table", null);
        String user = StringUtil.getText(node, "user", null);
        String password = StringUtil.getText(node, "password", null);
        String driver = StringUtil.getText(node, "driver", null);

        StringBuilder sb = new StringBuilder("spark.read.format(\"jdbc\")");
        sb.append("\n  .option(\"url\", ").append(StringUtil.pyString(url)).append(")");
        if (table != null) {
            sb.append("\n  .option(\"dbtable\", ").append(StringUtil.pyString(table)).append(")");
        }
        if (user != null) {
            sb.append("\n  .option(\"user\", ").append(StringUtil.pyString(user)).append(")");
        }
        if (password != null) {
            sb.append("\n  .option(\"password\", ").append(StringUtil.pyString(password)).append(")");
        }
        if (driver != null) {
            sb.append("\n  .option(\"driver\", ").append(StringUtil.pyString(driver)).append(")");
        }

        JsonNode optionsNode = node.get("options");
        if (optionsNode != null && optionsNode.isObject()) {
            optionsNode.fieldNames().forEachRemaining(name -> {
                JsonNode value = optionsNode.get(name);
                sb.append("\n  .option(")
                        .append(StringUtil.pyString(name))
                        .append(", ")
                        .append(StringUtil.pyString(value.asText()))
                        .append(")");
            });
        }

        sb.append("\n  .load()");
        return sb.toString();
    }

    public String buildSelect(JsonNode node) {
        ArrayNode cols = (ArrayNode) node.get("columns");
        List<String> parts = new ArrayList<>();
        for (JsonNode col : cols) {
            JsonNode e = col.get("expr");
            String expr = expressionBuilder.buildExpr(e);
            parts.add(StringUtil.appendAliasIfExists(col, expr));
        }
        return "  .select(" + String.join(", ", parts) + ")\n";
    }

    public String buildWithColumn(JsonNode node) {
        String name = StringUtil.getText(node, "name", null);
        String expr = expressionBuilder.buildExpr(node.get("expr"));
        return "  .withColumn(" + StringUtil.pyString(name) + ", " + expr + ")\n";
    }

    public String buildWithColumns(JsonNode node) {
        JsonNode cols = node.get("cols");
        List<String> kv = new ArrayList<>();
        cols.fieldNames().forEachRemaining(k -> {
            String v = expressionBuilder.buildExpr(cols.get(k));
            kv.add(StringUtil.pyString(k) + ": " + v);
        });
        String body = kv.isEmpty() ? "{}" : "{\n      " + String.join(",\n      ", kv) + "\n    }";
        return "  .withColumns(" + body + ")\n";
    }

    public String buildFilter(JsonNode node) {
        String cond = expressionBuilder.buildExpr(node.get("condition"));
        return "  .filter(" + cond + ")\n";
    }

    /**
     * Join 구문을 생성합니다. right 파라미터가 JSON 객체일 경우 재귀적으로 처리합니다.
     */
    public String buildJoin(JsonNode node) throws Exception {
        JsonNode rightNode = node.get("right");
        String how = StringUtil.getText(node, "how", "inner");
        String leftAlias = StringUtil.getText(node, "leftAlias", null);
        String rightAlias = StringUtil.getText(node, "rightAlias", null);

        // ON 조건 생성
        JsonNode on = node.get("on");
        String onExpr;
        if (on == null || on.isNull()) {
            onExpr = "None";
        } else if (on.isArray()) {
            List<String> conds = new ArrayList<>();
            for (JsonNode c : on) conds.add("(" + expressionBuilder.buildExpr(c) + ")");
            onExpr = String.join(" & ", conds);
        } else {
            onExpr = expressionBuilder.buildExpr(on);
        }

        // Right DataFrame 참조 생성 (재귀적 처리의 핵심)
        String rightRef = buildRightDfReference(rightNode);
        if (rightAlias != null && !rightAlias.isEmpty()) {
            rightRef = "(" + rightRef + ").alias(" + StringUtil.pyString(rightAlias) + ")";
        }

        // Left DataFrame에 alias 적용
        StringBuilder sb = new StringBuilder();
        if (leftAlias != null && !leftAlias.isEmpty()) {
            sb.append("  .alias(").append(StringUtil.pyString(leftAlias)).append(")\n");
        }
        sb.append("  .join(").append(rightRef).append(", ").append(onExpr).append(", ").append(StringUtil.pyString(how)).append(")\n");
        return sb.toString();
    }

    /**
     * Join의 right 부분을 생성합니다. JSON 객체이거나 문자열일 수 있습니다.
     */
    private String buildRightDfReference(JsonNode rightNode) throws Exception {
        if (rightNode == null || rightNode.isNull()) {
            return "None";
        }

        // right가 JSON 객체일 경우, 재귀적으로 PySpark 체인을 생성합니다.
        if (rightNode.isObject()) {
            String inputDf = StringUtil.getText(rightNode, "input", "df");
            ArrayNode steps = (ArrayNode) rightNode.get("steps");
            PySparkChainGenerator.ChainBuildResult subResult = mainGenerator.buildChain(inputDf, steps);
            String baseExpr = subResult.getBaseExpression();
            String subChain = subResult.getChain();

            StringBuilder builder = new StringBuilder();
            builder.append("(\n");
            builder.append(indentLines(baseExpr, "    "));
            if (!subChain.isEmpty()) {
                builder.append(subChain.replaceAll("(?m)^ {2}", "    "));
            }
            builder.append("  )");
            return builder.toString();
        }

        // right가 문자열일 경우, 기존 로직을 사용합니다.
        if (rightNode.isTextual()) {
            return rightNode.asText();
        }

        return "None";
    }

    public String buildGroupBy(JsonNode node) {
        ArrayNode keys = (ArrayNode) node.get("keys");
        List<String> parts = new ArrayList<>();
        for (JsonNode k : keys) parts.add(expressionBuilder.buildExpr(k));
        return "  .groupBy(" + String.join(", ", parts) + ")\n";
    }

    public String buildAgg(JsonNode node) {
        ArrayNode aggs = (ArrayNode) node.get("aggs");
        List<String> parts = new ArrayList<>();
        for (JsonNode agg : aggs) {
            JsonNode e = agg.get("expr");
            String expr = expressionBuilder.buildExpr(e);
            parts.add(StringUtil.appendAliasIfExists(agg, expr));
        }
        return "  .agg(\n      " + String.join(",\n      ", parts) + "\n  )\n";
    }

    public String buildOrderBy(JsonNode node) {
        ArrayNode keys = (ArrayNode) node.get("keys");
        List<String> parts = new ArrayList<>();
        for (JsonNode k : keys) {
            String expr = expressionBuilder.buildExpr(k.get("expr"));
            boolean asc = !k.has("asc") || k.get("asc").asBoolean(true);
            String nulls = StringUtil.getText(k, "nulls", null);
            String sortExpr = expr;
            if (nulls != null) {
                if (asc && "first".equalsIgnoreCase(nulls)) sortExpr = expr + ".asc_nulls_first()";
                else if (asc && "last".equalsIgnoreCase(nulls)) sortExpr = expr + ".asc_nulls_last()";
                else if (!asc && "first".equalsIgnoreCase(nulls)) sortExpr = expr + ".desc_nulls_first()";
                else if (!asc && "last".equalsIgnoreCase(nulls)) sortExpr = expr + ".desc_nulls_last()";
            } else {
                sortExpr = asc ? (expr + ".asc()") : (expr + ".desc()");
            }
            parts.add(sortExpr);
        }
        return "  .orderBy(" + String.join(", ", parts) + ")\n";
    }

    public String buildLimit(JsonNode node) {
        int n = node.get("n").asInt();
        return "  .limit(" + n + ")\n";
    }

    public String buildDropDuplicates(JsonNode node) {
        ArrayNode subset = (ArrayNode) node.get("subset");
        if (subset == null) return "  .dropDuplicates()\n";
        List<String> cols = new ArrayList<>();
        for (JsonNode c : subset) cols.add(StringUtil.pyString(c.asText()));
        return "  .dropDuplicates([" + String.join(", ", cols) + "])\n";
    }

    public String buildRepartition(JsonNode node) {
        StringBuilder b = new StringBuilder("  .repartition(");
        if (node.has("numPartitions")) {
            b.append(node.get("numPartitions").asInt());
            if (node.has("by") && node.get("by").isArray() && !node.get("by").isEmpty()) {
                b.append(", ");
                List<String> by = new ArrayList<>();
                for (JsonNode e : node.get("by")) by.add(expressionBuilder.buildExpr(e));
                b.append(String.join(", ", by));
            }
        }
        b.append(")\n");
        return b.toString();
    }

    public String buildCoalesce(JsonNode node) {
        int n = node.get("numPartitions").asInt();
        return "  .coalesce(" + n + ")\n";
    }

    public String buildSample(JsonNode node) {
        boolean withReplacement = node.has("withReplacement") && node.get("withReplacement").asBoolean(false);
        String fraction = node.get("fraction").asText();
        JsonNode seedNode = node.get("seed");

        StringBuilder sb = new StringBuilder("  .sample(");
        sb.append(StringUtil.pyBool(withReplacement)).append(", ").append(fraction);

        if (seedNode != null && !seedNode.isNull()) {
            String seedValue;
            if (seedNode.isNumber()) {
                seedValue = seedNode.toString();
            } else if (seedNode.isTextual()) {
                seedValue = StringUtil.pyString(seedNode.asText());
            } else {
                seedValue = seedNode.toString();
            }
            sb.append(", ").append(seedValue);
        }

        sb.append(")\n");
        return sb.toString();
    }

    public String buildDrop(JsonNode node) {
        ArrayNode cols = (ArrayNode) node.get("cols");
        List<String> parts = new ArrayList<>();
        for (JsonNode c : cols) parts.add(StringUtil.pyString(c.asText()));
        return "  .drop(" + String.join(", ", parts) + ")\n";
    }

    public String buildWithColumnRenamed(JsonNode node) {
        String src = StringUtil.getText(node, "src", null);
        String dst = StringUtil.getText(node, "dst", null);
        return "  .withColumnRenamed(" + StringUtil.pyString(src) + ", " + StringUtil.pyString(dst) + ")\n";
    }

    public String buildShowAction(JsonNode node, String dfName) {
        int n = node.has("n") ? node.get("n").asInt(20) : 20;  // 기본 20행
        JsonNode truncateNode = node.get("truncate");
        JsonNode verticalNode = node.get("vertical");

        StringBuilder args = new StringBuilder();
        args.append(n);

        if (truncateNode != null && !truncateNode.isNull()) {
            if (truncateNode.isBoolean()) {
                if (!truncateNode.asBoolean(true)) {
                    args.append(", truncate=False");
                }
            } else {
                args.append(", truncate=").append(truncateNode.asText());
            }
        }

        if (verticalNode != null && !verticalNode.isNull()) {
            if (verticalNode.isBoolean()) {
                if (verticalNode.asBoolean(false)) {
                    args.append(", vertical=True");
                }
            } else {
                args.append(", vertical=").append(verticalNode.asText());
            }
        }

        return dfName + ".show(" + args + ")\n";
    }

    public String buildNoArgAction(String actionName, String dfName) {
        return dfName + "." + actionName + "()\n";
    }

    private String indentLines(String expr, String indent) {
        if (expr == null || expr.isEmpty()) {
            return indent + "\n";
        }
        String[] lines = expr.split("\\r?\\n", -1);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < lines.length; i++) {
            String line = lines[i];
            if (i == lines.length - 1 && line.isEmpty()) {
                continue;
            }
            sb.append(indent).append(line).append("\n");
        }
        return sb.toString();
    }

}
