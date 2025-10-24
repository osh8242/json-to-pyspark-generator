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
            String subChain = mainGenerator.buildChainFromSteps(steps);

            // 생성된 서브 체인을 괄호로 감싸 하나의 표현식으로 만듭니다.
            return "(\n    " + inputDf + "\n" + subChain.replaceAll("(?m)^ {2}", "    ") + "  )";
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
        double fraction = node.get("fraction").asDouble();
        String seed = node.has("seed") ? node.get("seed").asText() : null;
        return "  .sample(" + withReplacement + ", " + fraction + (seed != null ? ", " + seed : "") + ")\n";
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

    public String buildShow(JsonNode node) {
        int n = node.has("n") ? node.get("n").asInt(20) : 20;  // 기본 20행
        boolean truncate = !node.has("truncate") || node.get("truncate").asBoolean(true);  // 기본 true
        boolean vertical = node.has("vertical") && node.get("vertical").asBoolean(false);  // 기본 false

        StringBuilder args = new StringBuilder();
        args.append(n);
        if (!truncate) {
            args.append(", truncate=False");
        }
        if (vertical) {
            if (args.length() > 1) args.append(", ");
            args.append("vertical=True");
        }

        return "  .show(" + args.toString() + ")\n";
    }

}
