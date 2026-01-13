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
        if (context.getLiteralMode() == ExpressionContext.LiteralMode.COLUMN_COERCE_NUMERIC) {
            return buildCoercedNumericColumnLiteral(v); // 추가
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
        if (requiresCoerceNumericLiteral(funcName)) {
            return parentContext.withLiteralMode(ExpressionContext.LiteralMode.COLUMN_COERCE_NUMERIC);
        }
        if (requiresRawLiteral(funcName, argIndex)) {
            return parentContext.withLiteralMode(ExpressionContext.LiteralMode.RAW);
        }
        return parentContext;
    }

    private boolean requiresRawLiteral(String funcName, int argIndex) {
        if (funcName == null) return false;

        String normalized = funcName.toLowerCase();

        switch (normalized) {

            // -----------------------------
            // 1) separator / format string
            // -----------------------------
            case "concat_ws":
                // sep (string)
                return argIndex == 0;

            case "date_format":
            case "from_unixtime":
            case "to_date":
            case "to_timestamp":
            case "unix_timestamp":
                // format (string) - 보통 2번째 인자
                return argIndex == 1;

            case "format_string":
                // format (string) - 1번째 인자
                return argIndex == 0;

            case "trunc":
                // format (string) - 2번째 인자 ("MM", "YYYY" ...)
                return argIndex == 1;

            case "next_day":
                // dayOfWeek (string) - 2번째 인자 ("Mon", "Tuesday" ...)
                return argIndex == 1;

            // -----------------------------
            // 2) regex / replace / translate
            // -----------------------------
            case "regexp_extract":
                // (str, pattern, idx)
                // pattern: string, idx: int  --> 둘 다 RAW가 맞음
                return argIndex == 1 || argIndex == 2;

            case "regexp_replace":
                // (str, pattern, replacement) => pattern, replacement string
                return argIndex == 1 || argIndex == 2;

            case "translate":
                // (col, matching, replace) => matching, replace string
                return argIndex == 1 || argIndex == 2;

            // split(str, pattern[, limit]) : pattern string, limit int
            case "split":
                return argIndex == 1 || argIndex == 2;

            // -----------------------------
            // 3) substring / locate / pad
            // -----------------------------
            case "substring":
            case "substr":
                // (str, pos, len) => pos/len int
                return argIndex == 1 || argIndex == 2;

            case "locate":
                // (substr, str[, pos]) => substr string, pos int
                return argIndex == 0 || argIndex == 2;

            case "left":
            case "right":
            case "instr":
                // (str, substr) => substr string
                return argIndex == 1;

            case "lpad":
            case "rpad":
                // (str, len, pad) => len int, pad string
                return argIndex == 1 || argIndex == 2;

            // -----------------------------
            // 4) date offsets (int)
            // -----------------------------
            case "date_add":
            case "date_sub":
            case "add_months":
                // (date, days/months) => int
                return argIndex == 1;

            // -----------------------------
            // 5) numeric scale / options
            // -----------------------------
            case "round":
            case "bround":
                // (col, scale) => int
                return argIndex == 1;

            case "sha2":
                // (col, numBits) => int (224/256/384/512)
                return argIndex == 1;

            case "months_between":
                // (date1, date2[, roundOff]) => roundOff bool (3번째 인자)
                return argIndex == 2;

            // window(timeCol, windowDuration, slideDuration, startTime)
            // duration들은 raw string
            case "window":
                return argIndex == 1 || argIndex == 2 || argIndex == 3;

            default:
                return false;
        }
    }

    private boolean requiresCoerceNumericLiteral(String funcName) {
        if (funcName == null) return false;
        String n = funcName.trim().toLowerCase();

        // 산술 try_*만 대상으로 제한 (원하면 startsWith("try_")로 넓혀도 됨)
        switch (n) {
            case "try_divide":
            case "try_multiply":
            case "try_add":
            case "try_subtract":
            case "try_mod":
                return true;
            default:
                return false;
        }
    }

    private String buildCoercedNumericColumnLiteral(JsonNode v) {
        return "F.lit(" + buildRawNumericLiteral(v) + ")";
    }

    /**
     * try_* 산술 함수에서 쓰기 위한 raw numeric literal
     * - v가 숫자면 그대로
     * - v가 문자열("2", "2.3", "-10", "1e3")이면 숫자로 파싱 가능할 때만 숫자처럼 출력
     * - 그 외(예: "abc")는 예외 (산술 try_* 인자에 문자열이 들어오는 걸 조기에 차단)
     */
    private String buildRawNumericLiteral(JsonNode v) {
        if (v == null || v.isNull()) return "None";
        if (v.isIntegralNumber() || v.isFloatingPointNumber()) return v.asText();
        if (v.isBoolean()) return v.asBoolean() ? "True" : "False";

        if (v.isTextual()) {
            String s = v.asText();
            if (s == null) return "None";
            s = s.trim();
            if (s.isEmpty()) return "None";

            // True/False/None 허용
            if ("true".equalsIgnoreCase(s)) return "True";
            if ("false".equalsIgnoreCase(s)) return "False";
            if ("none".equalsIgnoreCase(s) || "null".equalsIgnoreCase(s)) return "None";

            // 정수/실수/지수 형태면 숫자로 취급 (따옴표 없이)
            if (s.matches("[-+]?\\d+")) return s;
            if (s.matches("[-+]?(\\d+\\.\\d*|\\d*\\.\\d+)([eE][-+]?\\d+)?")) return s;
            if (s.matches("[-+]?\\d+([eE][-+]?\\d+)?")) return s; // 1e3 같은 케이스

            throw new RecipeExpressionException(
                    "Numeric literal required (try_*). But got non-numeric string: '" + s + "'"
            );
        }

        // 배열/객체 등은 불허
        throw new RecipeExpressionException("Numeric literal required (try_*). expr=" + v);
    }


}
