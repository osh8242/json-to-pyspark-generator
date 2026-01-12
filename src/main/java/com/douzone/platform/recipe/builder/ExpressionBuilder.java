package com.douzone.platform.recipe.builder;

import com.douzone.platform.recipe.exception.RecipeExpressionException;
import com.douzone.platform.recipe.util.StringUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.util.ArrayList;
import java.util.List;

/**
 * description    :
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2025. 9. 17.        osh8242       최초 생성
 */
public class ExpressionBuilder {
    public String buildExpr(JsonNode e) {
        return buildExpr(e, ExpressionContext.columnContext());
    }

    public String buildExpr(JsonNode e, ExpressionContext context) {
        if (e == null || e.isNull()) return "None";
        String type = StringUtil.getText(e, "type", null);
        if (type == null) return "None";
        switch (type) {
            case "col":
                return buildCol(e, context);
            case "lit":
                return buildLiteral(e, context);
            case "op":
                return buildOp(e, context);
            case "func":
                return buildFunc(e, context);
            case "cast":
                return "(" + buildExpr(e.get("expr"), context) + ").cast(" + StringUtil.pyString(StringUtil.getText(e, "to", "")) + ")";
            case "case":
                return buildCase(e, context);
            case "between":
                return buildBetween(e, context);
            case "isin":
                return buildIsin(e, context);
            case "like":
                return buildLike(e, context);
            case "isNull":
                return buildIsNull(e, context);
            case "isNotNull":
                return buildIsNotNull(e, context);
            default:
                // 3) 지원하지 않는 type 은 무조건 예외
                throw new RecipeExpressionException(
                        "Unsupported expression type: '" + type + "'. expr=" + e
                );
        }
    }

    public String buildCol(JsonNode e, ExpressionContext context) {
        // support table or alias field: {"type":"col", "name":"id", "table":"a"} -> F.col("a.id")
        String name = StringUtil.getText(e, "name", "");
        if (name == null || name.trim().isEmpty()) {
            throw new RecipeExpressionException(
                    "Column expression requires non-empty 'name'. expr=" + e.toString()
            );
        }
        String table = StringUtil.getText(e, "table", null);
        if (table != null && !table.isEmpty()) {
            name = table + "." + name;
        }
        return "F.col(" + StringUtil.pyString(name) + ")";
    }

    public String buildLiteral(JsonNode e, ExpressionContext context) {
        JsonNode v = e.get("value");
        if (context.getLiteralMode() == ExpressionContext.LiteralMode.RAW) {
            return buildRawLiteral(v);
        }
        return buildColumnLiteral(v);
    }

    private String buildColumnLiteral(JsonNode v) {
        if (v == null || v.isNull()) return "F.lit(None)";
        if (v.isTextual()) return "F.lit(" + StringUtil.pyString(v.asText()) + ")";
        if (v.isBoolean()) return "F.lit(" + (v.asBoolean() ? "True" : "False") + ")";
        if (v.isIntegralNumber() || v.isFloatingPointNumber()) return "F.lit(" + v.asText() + ")";
        return "F.lit(" + StringUtil.pyString(v.toString()) + ")";
    }

    private String buildRawLiteral(JsonNode v) {
        if (v == null || v.isNull()) return "None";
        if (v.isTextual()) return StringUtil.pyString(v.asText());
        if (v.isBoolean()) return v.asBoolean() ? "True" : "False";
        if (v.isIntegralNumber() || v.isFloatingPointNumber()) return v.asText();
        return StringUtil.pyString(v.toString());
    }

    public String buildOp(JsonNode e, ExpressionContext context) {
        String op = StringUtil.getText(e, "op", null);
        if (op == null || op.trim().isEmpty()) {
            throw new RecipeExpressionException(
                    "Operator expression requires 'op'. expr=" + e.toString()
            );
        }

        // NOT 단항 연산인 경우
        if ("not".equalsIgnoreCase(op)) {
            JsonNode inner = e.get("expr");
            if (inner == null || inner.isNull()) {
                throw new RecipeExpressionException(
                        "'not' operator requires 'expr'. expr=" + e
                );
            }
            String innerStr = buildExpr(inner, context);
            return "(~(" + innerStr + "))";
        }

        // 이외는 이항 연산
        JsonNode leftNode = e.get("left");
        JsonNode rightNode = e.get("right");
        if (leftNode == null || rightNode == null) {
            throw new RecipeExpressionException(
                    "Binary operator '" + op + "' requires both 'left' and 'right'. expr=" + e
            );
        }

        String left = buildExpr(leftNode, context);
        String right = buildExpr(rightNode, context);

        String pyOp;
        switch (op) {
            case "=":
                pyOp = "==";
                break;
            case "!=":
            case "<>":
                pyOp = "!=";
                break;
            case "==":
            case ">":
            case "<":
            case ">=":
            case "<=":
                pyOp = op;
                break;
            case "and":
                pyOp = "&";
                break;
            case "or":
                pyOp = "|";
                break;
            case "+":
            case "-":
            case "*":
            case "/":
            case "%":
                pyOp = op;
                break;
            default:
                throw new RecipeExpressionException(
                        "Unsupported operator: '" + op + "'. expr=" + e
                );
        }

        return "(" + left + " " + pyOp + " " + right + ")";
    }

    public String buildFunc(JsonNode e, ExpressionContext context) {
        String name = StringUtil.getText(e, "name", null);
        if (name == null || name.trim().isEmpty()) {
            throw new RecipeExpressionException(
                    "Function expression requires 'name'. expr=" + e.toString()
            );
        }

        JsonNode argsNode = e.get("args");
        List<String> parts = new ArrayList<>();
        if (argsNode != null && argsNode.isArray()) {
            ArrayNode args = (ArrayNode) argsNode;
            for (int i = 0; i < args.size(); i++) {
                JsonNode a = args.get(i);
                ExpressionContext argContext = adjustContextForFunctionArg(name, i, context);
                parts.add(buildExpr(a, argContext));
            }
        } else if (argsNode != null && !argsNode.isNull()) {
            // args 가 단일 객체로 들어온 경우도 그냥 하나짜리 arg로 처리
            ExpressionContext argContext = adjustContextForFunctionArg(name, 0, context);
            parts.add(buildExpr(argsNode, argContext));
        }

        return "F." + name + "(" + String.join(", ", parts) + ")";
    }


    public String buildCase(JsonNode e, ExpressionContext context) {
        ArrayNode when = (ArrayNode) e.get("when");
        String elseExpr = e.has("else") ? buildExpr(e.get("else"), context) : "None";
        String s = null;
        if (when != null) {
            for (int i = 0; i < when.size(); i++) {
                JsonNode w = when.get(i);
                String cond = buildExpr(w.get("if"), context);
                String thenExpr = buildExpr(w.get("then"), context);
                if (i == 0) s = "F.when(" + cond + ", " + thenExpr + ")";
                else s = "(" + s + ").when(" + cond + ", " + thenExpr + ")";
            }
        }
        if (s == null) s = "F.lit(None)";
        return "(" + s + ").otherwise(" + elseExpr + ")";
    }

    public String buildBetween(JsonNode e, ExpressionContext context) {
        if (e == null || e.isNull()) return "None";

        JsonNode expr = e.get("expr");
        JsonNode low = e.get("low");
        JsonNode high = e.get("high");

        // 안전하게 재귀 변환 (null이면 "None"으로 대체)
        String exprPart = expr != null && !expr.isNull() ? buildExpr(expr, context) : "None";
        String lowPart = low != null && !low.isNull() ? buildExpr(low, context) : "None";
        String highPart = high != null && !high.isNull() ? buildExpr(high, context) : "None";

        String betweenExpr = "(" + exprPart + ").between(" + lowPart + ", " + highPart + ")";

        // optional: not flag 지원
        boolean neg = e.has("not") && e.get("not").asBoolean(false);
        if (neg) {
            return "~(" + betweenExpr + ")";
        } else {
            return betweenExpr;
        }
    }

    /**
     * 'isin' 표현식을 생성합니다.
     * <p>
     * 스칼라 예시:
     * {
     *   "type": "isin",
     *   "expr": { ... },
     *   "values": [ { ... }, { ... } ]
     * }
     *  => (expr).isin(v1, v2)
     * <p>
     * 튜플 예시:
     * {
     *   "type": "isin",
     *   "expr": [ { ... }, { ... } ],
     *   "values": [
     *     [ { ... }, { ... } ],
     *     [ { ... }, { ... } ]
     *   ]
     * }
     *  => struct(expr1, expr2).isin(
     *         struct(v1a, v2a),
     *         struct(v1b, v2b)
     *     )
     */
    private String buildIsin(JsonNode e, ExpressionContext context) {
        JsonNode exprNode = e.get("expr");

        // expr 가 배열이면 (col1, col2, ...) 튜플로 해석
        boolean tupleMode = exprNode != null && exprNode.isArray();

        String exprPart;
        if (tupleMode) {
            // 튜플의 각 요소를 buildExpr 한 뒤 struct(...) 로 감싸기
            List<String> exprItems = new ArrayList<>();
            for (JsonNode item : exprNode) {
                exprItems.add(buildExpr(item, context));
            }
            exprPart = "F.struct(" + String.join(", ", exprItems) + ")"; // struct(col1, col2, ...)
        } else {
            // 기존 스칼라 모드
            exprPart = buildExpr(exprNode, context);
        }

        ArrayNode values = (ArrayNode) e.get("values");
        List<String> valueParts = new ArrayList<>();

        if (values != null) {
            for (JsonNode v : values) {
                if (tupleMode) {
                    // values[i] 도 배열: [ v1, v2, ... ] → struct(v1, v2, ...)
                    List<String> items = new ArrayList<>();
                    for (JsonNode item : v) {
                        items.add(buildExpr(item, context));
                    }
                    valueParts.add("F.struct(" + String.join(", ", items) + ")");
                } else {
                    // 기존 스칼라 모드
                    valueParts.add(buildExpr(v, context));
                }
            }
        }

        String isinExpr = "(" + exprPart + ").isin(" + String.join(", ", valueParts) + ")";

        boolean neg = e.has("not") && e.get("not").asBoolean(false);
        return neg ? "~(" + isinExpr + ")" : isinExpr;
    }

    /**
     * 'like' 표현식을 생성합니다. (rlike 등도 동일하게 사용 가능)
     * JSON 예시: { "type": "like", "expr": {...}, "pattern": "%test%" }
     */
    private String buildLike(JsonNode e, ExpressionContext context) {
        String exprPart = buildExpr(e.get("expr"), context);
        String pattern = StringUtil.getText(e, "pattern", "");

        String likeExpr = "(" + exprPart + ").like(" + StringUtil.pyString(pattern) + ")";

        boolean neg = e.has("not") && e.get("not").asBoolean(false);
        return neg ? "~(" + likeExpr + ")" : likeExpr;
    }

    /**
     * 'isNull' 표현식을 생성합니다.
     * JSON 예시: { "type": "isNull", "expr": {...} }
     */
    private String buildIsNull(JsonNode e, ExpressionContext context) {
        String exprPart = buildExpr(e.get("expr"), context);
        return "(" + exprPart + ").isNull()";
    }

    /**
     * 'isNotNull' 표현식을 생성합니다.
     * JSON 예시: { "type": "isNotNull", "expr": {...} }
     */
    private String buildIsNotNull(JsonNode e, ExpressionContext context) {
        String exprPart = buildExpr(e.get("expr"), context);
        return "(" + exprPart + ").isNotNull()";
    }

    private ExpressionContext adjustContextForFunctionArg(String funcName, int argIndex, ExpressionContext parentContext) {
        if (requiresRawLiteral(funcName, argIndex)) {
            return parentContext.withLiteralMode(ExpressionContext.LiteralMode.RAW);
        }
        return parentContext;
    }

    private boolean requiresRawLiteral(String funcName, int argIndex) {
        if (funcName == null) return false;
        String normalized = funcName.toLowerCase();
        switch (normalized) {
            case "concat_ws":
                return argIndex == 0;
            case "round":
            case "date_format":
            case "from_unixtime":
            case "to_date":
                return argIndex == 1;
            case "regexp_extract":
                return argIndex == 2;
            case "substring":
            case "substr":
            case "regexp_replace":
            case "translate":
                return argIndex == 1 || argIndex == 2;
            default:
                return false;
        }
    }

}
