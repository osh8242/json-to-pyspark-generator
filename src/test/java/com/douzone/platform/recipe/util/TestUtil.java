package com.douzone.platform.recipe.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * description    :
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2025. 9. 17.        osh8242       최초 생성
 */
public class TestUtil {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    /**
     * 반복되는 PySpark 스크립트의 헤더와 풋터를 생성하여 테스트 코드의 가독성을 높이는 헬퍼 메서드입니다.
     * @param stepCode 테스트할 step의 PySpark 코드 조각
     * @return 완전한 PySpark 스크립트 문자열
     */
    public static String buildFullScript(String stepCode, String... actionLines) {
        StringBuilder sb = new StringBuilder();

        if (stepCode != null && !stepCode.trim().isEmpty()) {
            for (String segment : splitSegments(stepCode)) {
                if (segment.isEmpty()) {
                    continue;
                }
                String[] lines = segment.split("\\r?\\n");
                if (lines.length == 0) {
                    continue;
                }

                String firstLine = lines[0];
                int dotIndex = firstLine.indexOf('.');
                if (dotIndex < 0) {
                    continue;
                }

                sb.append("df = df").append(firstLine.substring(dotIndex)).append("\n");
                for (int i = 1; i < lines.length; i++) {
                    String line = lines[i];
                    String trimmed = line.trim();
                    if (trimmed.startsWith(".")) {
                        sb.append("    ").append(trimmed).append("\n");
                    } else {
                        sb.append(line).append("\n");
                    }
                }
            }
        }

        if (actionLines != null) {
            for (String action : actionLines) {
                if (action == null || action.isEmpty()) {
                    continue;
                }
                String normalized = action.replace("result_df", "df");
                sb.append(normalized);
                if (!normalized.endsWith("\n")) {
                    sb.append("\n");
                }
            }
        }

        return sb.toString();
    }

    /**
     * 테스트 정보(입력 JSON, 생성된 코드)를 콘솔에 출력합니다.
     * @param testName 현재 실행 중인 테스트 이름
     * @param json 입력으로 사용된 JSON 문자열
     * @param actual 생성된 PySpark 코드 문자열
     */
    public static void printTestInfo(String testName, String json, String actual) {
        System.out.println("========== " + testName + " ==========");
        System.out.println("\n[Input JSON]\n" + json);
        System.out.println("\n[Generated PySpark Code]\n" + actual);
        System.out.println("========================================\n");
    }

    /**
     * 구 버전(step 기반) JSON을 node 기반 구조로 변환합니다.
     */
    public static String toNodeJson(String legacyJson) throws Exception {
        JsonNode parsed = MAPPER.readTree(legacyJson);
        if (!(parsed instanceof ObjectNode)) {
            return legacyJson;
        }

        ObjectNode root = (ObjectNode) parsed;
        String baseInput = root.hasNonNull("input") ? root.get("input").asText() : "df";
        ArrayNode steps = root.withArray("steps");

        String previousOutput = isIdentifier(baseInput) ? baseInput : "df";
        String activeInput = baseInput;

        for (JsonNode node : steps) {
            if (!(node instanceof ObjectNode)) {
                continue;
            }

            ObjectNode step = (ObjectNode) node;
            String nodeName = null;
            if (step.hasNonNull("node")) {
                nodeName = step.get("node").asText();
            } else if (step.has("step")) {
                JsonNode legacyStep = step.remove("step");
                if (legacyStep != null && !legacyStep.isNull()) {
                    nodeName = legacyStep.asText();
                    step.put("node", nodeName);
                }
            }

            if (nodeName == null) {
                continue;
            }

            if (!step.hasNonNull("input") || step.get("input").asText().isEmpty()) {
                step.put("input", activeInput);
            } else {
                activeInput = step.get("input").asText();
            }

            boolean isAction = "show".equals(nodeName);
            if (isAction) {
                if (!step.hasNonNull("input") || step.get("input").asText().isEmpty()) {
                    step.put("input", previousOutput);
                }
                step.remove("output");
                continue;
            }

            String outputName = null;
            if (step.hasNonNull("output") && !step.get("output").asText().isEmpty()) {
                outputName = step.get("output").asText();
            } else {
                outputName = previousOutput;
                if (!isIdentifier(outputName)) {
                    outputName = "df";
                }
                step.put("output", outputName);
            }

            previousOutput = outputName;
            if (!isIdentifier(previousOutput)) {
                previousOutput = "df";
                step.put("output", previousOutput);
            }
            activeInput = previousOutput;
        }

        root.remove("input");
        return MAPPER.writeValueAsString(root);
    }

    private static List<String> splitSegments(String stepCode) {
        List<String> segments = new ArrayList<>();
        if (stepCode == null || stepCode.isEmpty()) {
            return segments;
        }

        String[] lines = stepCode.split("\\r?\\n");
        List<String> current = new ArrayList<>();
        String previousTrimmed = null;

        for (String line : lines) {
            if (line == null) {
                continue;
            }
            String trimmed = line.trim();
            if (trimmed.isEmpty()) {
                continue;
            }

            boolean startsWithDot = trimmed.startsWith(".");
            boolean newSegment = startsWithDot && !current.isEmpty();
            if (newSegment) {
                if (previousTrimmed != null && (previousTrimmed.startsWith(".alias") || previousTrimmed.startsWith(".toJSON"))) {
                    newSegment = false;
                }
            }

            if (newSegment) {
                segments.add(String.join("\n", current));
                current = new ArrayList<>();
            }

            current.add(line);
            previousTrimmed = trimmed;
        }

        if (!current.isEmpty()) {
            segments.add(String.join("\n", current));
        }

        return segments;
    }

    private static boolean isIdentifier(String value) {
        return value != null && IDENTIFIER.matcher(value).matches();
    }
}
