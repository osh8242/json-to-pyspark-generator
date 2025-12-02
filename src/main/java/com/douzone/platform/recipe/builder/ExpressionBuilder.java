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
        if (e == null || e.isNull()) return "None";
        String type = StringUtil.getText(e, "type", null);
        if (type == null) return "None";
        switch (type) {
            case "col":
                return buildCol(e);
            case "lit":
                return buildLiteral(e);
            case "op":
                return buildOp(e);
            case "func":
                return buildFunc(e);
            case "cast":
                return "(" + buildExpr(e.get("expr")) + ").cast(" + StringUtil.pyString(StringUtil.getText(e, "to", "")) + ")";
            case "case":
                return buildCase(e);
            case "between":
                return buildBetween(e);
            case "isin":
                return buildIsin(e);
            case "like":
                return buildLike(e);
            case "isNull":
                return buildIsNull(e);
            case "isNotNull":
                return buildIsNotNull(e);
            default:
                // 3) 지원하지 않는 type 은 무조건 예외
                throw new RecipeExpressionException(
                        "Unsupported expression type: '" + type + "'. expr=" + e
                );
        }
    }

    public String buildCol(JsonNode e) {
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

    public String buildLiteral(JsonNode e) {
        JsonNode v = e.get("value");
        if (v == null || v.isNull()) return "F.lit(None)";
        if (v.isTextual()) return "F.lit(" + StringUtil.pyString(v.asText()) + ")";
        if (v.isBoolean()) return "F.lit(" + (v.asBoolean() ? "True" : "False") + ")";
        if (v.isIntegralNumber() || v.isFloatingPointNumber()) return "F.lit(" + v.asText() + ")";
        return "F.lit(" + StringUtil.pyString(v.toString()) + ")";
    }

    public String buildOp(JsonNode e) {
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
            String innerStr = buildExpr(inner);
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

        String left = buildExpr(leftNode);
        String right = buildExpr(rightNode);

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

    public String buildFunc(JsonNode e) {
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
            for (JsonNode a : args) {
                parts.add(buildExpr(a));
            }
        } else if (argsNode != null && !argsNode.isNull()) {
            // args 가 단일 객체로 들어온 경우도 그냥 하나짜리 arg로 처리
            parts.add(buildExpr(argsNode));
        }

        return "F." + name + "(" + String.join(", ", parts) + ")";
    }


    public String buildCase(JsonNode e) {
        ArrayNode when = (ArrayNode) e.get("when");
        String elseExpr = e.has("else") ? buildExpr(e.get("else")) : "None";
        String s = null;
        if (when != null) {
            for (int i = 0; i < when.size(); i++) {
                JsonNode w = when.get(i);
                String cond = buildExpr(w.get("if"));
                String thenExpr = buildExpr(w.get("then"));
                if (i == 0) s = "F.when(" + cond + ", " + thenExpr + ")";
                else s = "(" + s + ").when(" + cond + ", " + thenExpr + ")";
            }
        }
        if (s == null) s = "F.lit(None)";
        return "(" + s + ").otherwise(" + elseExpr + ")";
    }

    public String buildBetween(JsonNode e) {
        if (e == null || e.isNull()) return "None";

        JsonNode expr = e.get("expr");
        JsonNode low = e.get("low");
        JsonNode high = e.get("high");

        // 안전하게 재귀 변환 (null이면 "None"으로 대체)
        String exprPart = expr != null && !expr.isNull() ? buildExpr(expr) : "None";
        String lowPart = low != null && !low.isNull() ? buildExpr(low) : "None";
        String highPart = high != null && !high.isNull() ? buildExpr(high) : "None";

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
    private String buildIsin(JsonNode e) {
        JsonNode exprNode = e.get("expr");

        // expr 가 배열이면 (col1, col2, ...) 튜플로 해석
        boolean tupleMode = exprNode != null && exprNode.isArray();

        String exprPart;
        if (tupleMode) {
            // 튜플의 각 요소를 buildExpr 한 뒤 struct(...) 로 감싸기
            List<String> exprItems = new ArrayList<>();
            for (JsonNode item : exprNode) {
                exprItems.add(buildExpr(item));
            }
            exprPart = "F.struct(" + String.join(", ", exprItems) + ")"; // struct(col1, col2, ...)
        } else {
            // 기존 스칼라 모드
            exprPart = buildExpr(exprNode);
        }

        ArrayNode values = (ArrayNode) e.get("values");
        List<String> valueParts = new ArrayList<>();

        if (values != null) {
            for (JsonNode v : values) {
                if (tupleMode) {
                    // values[i] 도 배열: [ v1, v2, ... ] → struct(v1, v2, ...)
                    List<String> items = new ArrayList<>();
                    for (JsonNode item : v) {
                        items.add(buildExpr(item));
                    }
                    valueParts.add("F.struct(" + String.join(", ", items) + ")");
                } else {
                    // 기존 스칼라 모드
                    valueParts.add(buildExpr(v));
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
    private String buildLike(JsonNode e) {
        String exprPart = buildExpr(e.get("expr"));
        String pattern = StringUtil.getText(e, "pattern", "");

        String likeExpr = "(" + exprPart + ").like(" + StringUtil.pyString(pattern) + ")";

        boolean neg = e.has("not") && e.get("not").asBoolean(false);
        return neg ? "~(" + likeExpr + ")" : likeExpr;
    }

    /**
     * 'isNull' 표현식을 생성합니다.
     * JSON 예시: { "type": "isNull", "expr": {...} }
     */
    private String buildIsNull(JsonNode e) {
        String exprPart = buildExpr(e.get("expr"));
        return "(" + exprPart + ").isNull()";
    }

    /**
     * 'isNotNull' 표현식을 생성합니다.
     * JSON 예시: { "type": "isNotNull", "expr": {...} }
     */
    private String buildIsNotNull(JsonNode e) {
        String exprPart = buildExpr(e.get("expr"));
        return "(" + exprPart + ").isNotNull()";
    }

}
