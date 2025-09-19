package com.douzone.platform.recipe;

import com.douzone.platform.recipe.builder.StepBuilder;
import com.douzone.platform.recipe.util.StringUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

public class PySparkChainGenerator {

    private final StepBuilder stepBuilder;
    private final ObjectMapper om = new ObjectMapper();

    // 생성자에서 StepBuilder를 초기화하며, 재귀 호출을 위해 자기 자신의 참조를 넘겨줍니다.
    public PySparkChainGenerator() {
        this.stepBuilder = new StepBuilder(this);
    }

    /**
     * 외부에서 호출하는 정적 진입점 메서드입니다.
     */
    public static String generate(String json) throws Exception {
        PySparkChainGenerator generator = new PySparkChainGenerator();
        return generator.generatePySparkCode(json);
    }

    /**
     * 실제 코드 생성을 담당하는 인스턴스 메서드입니다.
     * PySpark 스크립트의 전체적인 틀(import, out = ...)을 생성합니다.
     */
    public String generatePySparkCode(String json) throws Exception {
        JsonNode root = om.readTree(json);
        return generatePySparkCode(root);
    }

    public String generatePySparkCode(JsonNode root) throws Exception {
        String inputDf = StringUtil.getText(root, "input", "df");
        String outDf = StringUtil.getText(root, "output", "result_df");
        ArrayNode steps = (ArrayNode) root.get("steps");

        StringBuilder sb = new StringBuilder();
        sb.append("from pyspark.sql import functions as F\n\n");
        sb.append(outDf).append(" = (\n");
        sb.append("  ").append(inputDf).append("\n");
        sb.append(buildChainFromSteps(steps));
        sb.append(")\n");
        return sb.toString();
    }

    /**
     * steps 배열을 받아 체인 메서드 문자열을 생성하는 재귀적 핵심 로직입니다.
     * 이 메서드는 순수하게 메서드 체인(.filter(...).join(...)) 부분만 생성합니다.
     */
    public String buildChainFromSteps(ArrayNode steps) throws Exception {
        if (steps == null) {
            return "";
        }

        StringBuilder sb = new StringBuilder();

        for (JsonNode step : steps) {
            String opName = StringUtil.getText(step, "step", null);
            if (opName == null) continue;
            switch (opName) {
                case "select":
                    sb.append(stepBuilder.buildSelect(step));
                    break;
                case "withColumn":
                    sb.append(stepBuilder.buildWithColumn(step));
                    break;
                case "withColumns":
                    sb.append(stepBuilder.buildWithColumns(step));
                    break;
                case "filter":
                case "where":
                    sb.append(stepBuilder.buildFilter(step));
                    break;
                case "join":
                    sb.append(stepBuilder.buildJoin(step));
                    break;
                case "groupBy":
                    sb.append(stepBuilder.buildGroupBy(step));
                    break;
                case "agg":
                    sb.append(stepBuilder.buildAgg(step));
                    break;
                case "orderBy":
                case "sort":
                    sb.append(stepBuilder.buildOrderBy(step));
                    break;
                case "limit":
                    sb.append(stepBuilder.buildLimit(step));
                    break;
                case "distinct":
                    sb.append("  .distinct()\n");
                    break;
                case "dropDuplicates":
                    sb.append(stepBuilder.buildDropDuplicates(step));
                    break;
                case "repartition":
                    sb.append(stepBuilder.buildRepartition(step));
                    break;
                case "coalesce":
                    sb.append(stepBuilder.buildCoalesce(step));
                    break;
                case "sample":
                    sb.append(stepBuilder.buildSample(step));
                    break;
                case "drop":
                    sb.append(stepBuilder.buildDrop(step));
                    break;
                case "withColumnRenamed":
                    sb.append(stepBuilder.buildWithColumnRenamed(step));
                    break;
                default:
                    break;
            }
        }

        return sb.toString();
    }

}
