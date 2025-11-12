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

    public String buildNodeStatement(JsonNode node) throws Exception {
        if (node == null || node.isNull()) {
            return "";
        }

        String opName = StringUtil.getText(node, "node", null);
        if (opName == null) {
            return "";
        }

        String input = StringUtil.getText(node, "input", null);
        String output = StringUtil.getText(node, "output", null);

        switch (opName) {
            case "load":
                String loadExpr = buildLoad(node);
                if (loadExpr == null || loadExpr.isEmpty()) {
                    return "";
                }
                return formatDirectAssignment(output, loadExpr);
            case "select":
                return formatTransformation(input, output, buildSelect(node));
            case "withColumn":
                return formatTransformation(input, output, buildWithColumn(node));
            case "withColumns":
                return formatTransformation(input, output, buildWithColumns(node));
            case "filter":
            case "where":
                return formatTransformation(input, output, buildFilter(node));
            case "join":
                return formatTransformation(input, output, buildJoin(node));
            case "groupBy":
                return formatTransformation(input, output, buildGroupBy(node));
            case "agg":
                return formatTransformation(input, output, buildAgg(node));
            case "orderBy":
            case "sort":
                return formatTransformation(input, output, buildOrderBy(node));
            case "toJSON":
                return formatTransformation(input, output, buildToJson(node));
            case "limit":
                return formatTransformation(input, output, buildLimit(node));
            case "distinct":
                return formatTransformation(input, output, "  .distinct()\n");
            case "dropDuplicates":
                return formatTransformation(input, output, buildDropDuplicates(node));
            case "repartition":
                return formatTransformation(input, output, buildRepartition(node));
            case "coalesce":
                return formatTransformation(input, output, buildCoalesce(node));
            case "sample":
                return formatTransformation(input, output, buildSample(node));
            case "drop":
                return formatTransformation(input, output, buildDrop(node));
            case "withColumnRenamed":
                return formatTransformation(input, output, buildWithColumnRenamed(node));
            case "show":
                String target = input;
                if (target == null || target.isEmpty()) {
                    target = output;
                }
                if (target == null || target.isEmpty()) {
                    target = "df";
                }
                return buildShowAction(node, target);
            default:
                return formatTransformation(input, output, buildDefaultStep(opName, node));
        }
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

        List<String> predicates = new ArrayList<>();
        JsonNode predicateNode = node.hasNonNull("predicate") ? node.get("predicate") : null;
        if (predicateNode == null && node.hasNonNull("predicates")) {
            predicateNode = node.get("predicates");
        }
        if (predicateNode != null && !predicateNode.isNull()) {
            if (predicateNode.isArray()) {
                predicateNode.forEach(item -> predicates.add(item.asText()));
            } else {
                predicates.add(predicateNode.asText());
            }
        }

        List<String> propertyEntries = new ArrayList<>();
        if (user != null) {
            propertyEntries.add(StringUtil.pyString("user") + ": " + StringUtil.pyString(user));
        }
        if (password != null) {
            propertyEntries.add(StringUtil.pyString("password") + ": " + StringUtil.pyString(password));
        }
        if (driver != null) {
            propertyEntries.add(StringUtil.pyString("driver") + ": " + StringUtil.pyString(driver));
        }

        JsonNode optionsNode = node.get("options");
        if (optionsNode != null && optionsNode.isObject()) {
            List<String> optionNames = new ArrayList<>();
            optionsNode.fieldNames().forEachRemaining(optionNames::add);
            optionNames.sort(String::compareTo);
            for (String name : optionNames) {
                JsonNode value = optionsNode.get(name);
                propertyEntries.add(StringUtil.pyString(name) + ": " + StringUtil.pyString(value.asText()));
            }
        }

        List<String> parameters = new ArrayList<>();
        parameters.add("    url=" + StringUtil.pyString(url));
        if (table != null) {
            parameters.add("    table=" + StringUtil.pyString(table));
        }
        if (!predicates.isEmpty()) {
            parameters.add("    predicates=" + formatPredicates(predicates));
        }
        parameters.add(buildPropertiesParam(propertyEntries));

        StringBuilder sb = new StringBuilder("spark.read.jdbc(\n");
        sb.append(String.join(",\n", parameters));
        sb.append("\n  )");
        return sb.toString();
    }

    private String formatPredicates(List<String> predicates) {
        if (predicates.isEmpty()) {
            return "[]";
        }
        if (predicates.size() == 1) {
            return "[" + StringUtil.pyString(predicates.get(0)) + "]";
        }

        StringBuilder sb = new StringBuilder("[\n");
        for (int i = 0; i < predicates.size(); i++) {
            sb.append("      ").append(StringUtil.pyString(predicates.get(i)));
            if (i < predicates.size() - 1) {
                sb.append(",\n");
            } else {
                sb.append("\n    ]");
            }
        }
        return sb.toString();
    }

    private String buildPropertiesParam(List<String> propertyEntries) {
        if (propertyEntries.isEmpty()) {
            return "    properties={}";
        }

        StringBuilder sb = new StringBuilder("    properties={\n");
        for (int i = 0; i < propertyEntries.size(); i++) {
            sb.append("      ").append(propertyEntries.get(i));
            if (i < propertyEntries.size() - 1) {
                sb.append(",\n");
            } else {
                sb.append("\n    }");
            }
        }
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

    public String buildToJson(JsonNode node) {
        StringBuilder sb = new StringBuilder("  .toJSON()\n");
        if (node != null && node.has("take") && !node.get("take").isNull()) {
            JsonNode takeNode = node.get("take");
            String takeValue;
            if (takeNode.isIntegralNumber()) {
                takeValue = String.valueOf(takeNode.longValue());
            } else {
                takeValue = takeNode.asText();
            }
            sb.append("  .take(").append(takeValue).append(")\n");
        }
        return sb.toString();
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

    public String buildDefaultStep(String stepName, JsonNode node) {
        if (stepName == null) {
            return "";
        }

        String normalized = stepName.trim();
        if (normalized.isEmpty()) {
            return "";
        }

        List<String> args = new ArrayList<>();
        if (node != null) {
            JsonNode argsNode = node.get("args");
            if (argsNode != null && !argsNode.isNull()) {
                if (argsNode.isArray()) {
                    for (JsonNode arg : argsNode) {
                        args.add(buildDefaultArgument(arg));
                    }
                } else {
                    args.add(buildDefaultArgument(argsNode));
                }
            }
        }

        String joinedArgs = String.join(", ", args);
        return "  ." + normalized + "(" + joinedArgs + ")\n";
    }

    private String buildDefaultArgument(JsonNode arg) {
        if (arg == null || arg.isNull()) {
            return "None";
        }

        if (arg.isObject() && arg.has("type")) {
            return expressionBuilder.buildExpr(arg);
        }

        if (arg.isTextual()) {
            return StringUtil.pyString(arg.asText());
        }

        if (arg.isBoolean()) {
            return StringUtil.pyBool(arg.asBoolean());
        }

        if (arg.isNumber()) {
            return arg.asText();
        }

        if (arg.isArray()) {
            List<String> items = new ArrayList<>();
            for (JsonNode item : arg) {
                items.add(buildDefaultArgument(item));
            }
            return "[" + String.join(", ", items) + "]";
        }

        return arg.toString();
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

    private String formatTransformation(String input, String output, String fragment) {
        String sanitized = sanitizeChainFragment(fragment);
        if (sanitized.isEmpty()) {
            return "";
        }

        String base = (input == null || input.isEmpty()) ? "df" : input;
        String target = (output == null || output.isEmpty()) ? base : output;

        String[] lines = sanitized.split("\\r?\\n");
        StringBuilder sb = new StringBuilder();
        sb.append(target).append(" = ").append(base);
        for (int i = 0; i < lines.length; i++) {
            String line = lines[i];
            if (line.isEmpty()) {
                continue;
            }
            if (i == 0) {
                sb.append(line);
            } else {
                if (line.startsWith(".")) {
                    sb.append("\n    ").append(line);
                } else {
                    sb.append("\n").append(line);
                }
            }
        }
        sb.append("\n");
        return sb.toString();
    }

    private String formatDirectAssignment(String output, String expression) {
        if (expression == null || expression.isEmpty()) {
            return "";
        }

        String target = (output == null || output.isEmpty()) ? "df" : output;
        String trimmed = expression.replaceAll("[\\s\\r\\n]+$", "");
        if (trimmed.isEmpty()) {
            return "";
        }

        String[] lines = trimmed.split("\\r?\\n");
        StringBuilder sb = new StringBuilder();
        sb.append(target).append(" = ").append(lines[0]);
        for (int i = 1; i < lines.length; i++) {
            sb.append("\n    ").append(lines[i]);
        }
        sb.append("\n");
        return sb.toString();
    }

    private String sanitizeChainFragment(String fragment) {
        if (fragment == null) {
            return "";
        }
        String trimmed = fragment.replaceAll("[\\s\\r\\n]+$", "");
        if (trimmed.isEmpty()) {
            return "";
        }

        String[] lines = trimmed.split("\\r?\\n");
        List<String> sanitized = new ArrayList<>();
        for (String line : lines) {
            if (line.trim().isEmpty()) {
                continue;
            }
            int dotIndex = line.indexOf('.');
            if (dotIndex >= 0) {
                String prefix = line.substring(0, dotIndex);
                if (prefix.trim().isEmpty()) {
                    line = line.substring(dotIndex);
                }
            }
            sanitized.add(line);
        }
        return String.join("\n", sanitized);
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
