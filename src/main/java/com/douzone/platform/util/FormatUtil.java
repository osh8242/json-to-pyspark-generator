package com.douzone.platform.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.douzone.platform.util.UtilObject.OBJECT_MAPPER;

/**
 * 유틸리티 클래스 - 다양한 형식 변환과 데이터 처리 기능 제공
 * ===========================================================
 * 날짜              작성자              비고
 * -----------------------------------------------------------
 * 7/3/24         osh8242         최초 생성
 */
public class FormatUtil {

    /**
     * ObjectMapper 인스턴스를 반환합니다.
     *
     * @return ObjectMapper 인스턴스
     */
    public static ObjectMapper getMapper() {
        return OBJECT_MAPPER;
    }

    /**
     * 객체를 깊은 복사합니다.
     *
     * @param <T>    제네릭 타입
     * @param object 복사할 원본 객체
     * @return 복사된 객체
     */
    public static <T> T deepCopy(T object) {
        try {
            return OBJECT_MAPPER.readValue(getJson(object), new TypeReference<T>() {
            });
        } catch (JsonProcessingException e) {
            throw new RuntimeException("깊은 복사 중 오류 발생", e);
        }
    }

    /**
     * 객체를 JSON 문자열로 변환합니다.
     *
     * @param object 변환할 객체
     * @return JSON 문자열
     */
    public static String getJson(Object object) {
        return getJson(object, false);
    }

    public static String getJson(Object object, boolean pretty) {
        try {
            ObjectMapper mapper = FormatUtil.getMapper();
            if (pretty) return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(object);
            else return mapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("객체를 JSON으로 변환하는 중 오류 발생", e);
        }
    }

    /**
     * InputStream에서 문자열을 읽어옵니다.
     *
     * @param inputStream 입력 스트림
     * @return 읽어온 문자열
     * @throws IOException 입출력 오류가 발생한 경우
     */
    public static String getStringFromInputStream(InputStream inputStream) throws IOException {
        StringBuilder sb = new StringBuilder();
        try (BufferedReader in = new BufferedReader(new InputStreamReader(inputStream))) {
            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                sb.append(inputLine);
            }
        }
        return sb.toString();
    }

    /**
     * 시간 문자열에서 'T'를 공백으로 변경하는 메서드.
     * 1990-01-01T23:59:59 -> 1990-01-01 23:59:59
     *
     * @param value 입력 문자열
     * @return 'T'가 제거된 문자열
     */
    public static String formatDateTimeString(String value) {
        if (value != null && value.matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}")) {
            // T를 공백으로 변환
            return value.replace("T", " ");
        }
        return value; // 다른 형식은 그대로 반환
    }

    /**
     * 현재 날짜를 지정된 형식으로 반환합니다.
     *
     * @param format 날짜 형식 (예: "yyyy-MM-dd")
     * @return 형식에 맞게 포맷된 현재 날짜
     */
    public static String getCurrentDate(String format) {
        Date currentDate = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat(format);
        return formatter.format(currentDate);
    }

    /**
     * 입력 문자열과 구분자 목록을 받아 해당 문자열을 구분자로 구분하여 돌려준다.
     * 단, single quotation (') 으로 감싸준 문자열 상태 literal 부분은 구분하지 않는다.
     * (구분자도 요소로 포함됨)
     *
     * @param input             문자열
     * @param delimiters        구분자 목록 리스트 (AND, OR .. )
     * @param includeDelimiters 구분자를 결과에 포함할 것인지 여부 (default: false)
     * @return 구분자로 구분된 String[]
     */
    public static String[] splitByDelimitersOutsideQuotes(String input, List<String> delimiters, boolean includeDelimiters) {
        boolean inQuotes = false; // 작은따옴표 상태
        List<String> list = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        int i = 0;

        while (i < input.length()) {
            char ch = input.charAt(i);

            // 작은따옴표인지 먼저 체크
            if (ch == '\'') {
                // 1) ''(작은따옴표 두 번)이면 그대로 추가하고 상태 토글하지 않음
                if (i + 1 < input.length() && input.charAt(i + 1) == '\'') {
                    sb.append("''");
                    i += 2;
                }
                // 2) \' (앞 문자가 백슬래시)인 경우에는 문자 그대로 추가
                else if (i > 0 && input.charAt(i - 1) == '\\') {
                    sb.append(ch);
                    i++;
                }
                // 3) 그 외의 경우에는 inQuotes 상태 반전
                else {
                    inQuotes = !inQuotes;
                    sb.append(ch);
                    i++;
                }
            } else if (!inQuotes) {
                // 문자열 상태가 아닐 때 구분자 검사
                boolean matched = false;
                for (String delimiter : delimiters) {
                    if (input.startsWith(delimiter, i)) {
                        // 현재까지의 문자열 추가
                        if (sb.length() > 0) {
                            addFromStringToList(sb.toString(), list);
                            sb = new StringBuilder();
                        }
                        // 구분자를 결과에 포함할지 여부
                        if (includeDelimiters) {
                            addFromStringToList(delimiter, list);
                        }
                        i += delimiter.length();
                        matched = true;
                        break;
                    }
                }
                if (!matched) {
                    sb.append(ch);
                    i++;
                }
            } else {
                // 문자열 상태라면 그대로 추가
                sb.append(ch);
                i++;
            }
        }

        // 마지막 남은 문자열이 있다면 리스트에 추가
        if (sb.length() > 0) {
            addFromStringToList(sb.toString(), list);
        }

        return list.toArray(new String[0]);
    }

    public static String[] splitByDelimitersOutsideQuotes(String input, String delimiter) {
        return splitByDelimitersOutsideQuotes(input, Collections.singletonList(delimiter), false);
    }

    public static String[] splitByDelimitersOutsideQuotes(String input, String delimiter, boolean includeDelimiters) {
        return splitByDelimitersOutsideQuotes(input, Collections.singletonList(delimiter), includeDelimiters);
    }

    public static String[] splitByDelimitersOutsideQuotes(String input, List<String> delimiters) {
        return splitByDelimitersOutsideQuotes(input, delimiters, false);
    }

    /**
     * 문자열 상태가 아닌 경우와 대괄호([])로 감싸져있지 않은 경우에 등장하는 구분자(delimiter) 문자열을 기준으로 분할하는 메서드.
     *
     * @param input     원본 문자열
     * @param delimiter 구분자로 사용할 문자열
     * @return 구분자로 분할된 문자열 배열
     */
    public static String[] splitByDelimitersOutsideQuotesAndParenthesis(String input, String delimiter) {
        List<String> list = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        int bracketDepth = 0; // 대괄호([])의 깊이
        boolean inQuotes = false; // 작은따옴표 상태
        int i = 0;
        int delimiterLength = delimiter.length();

        while (i < input.length()) {
            char ch = input.charAt(i);

            // 작은따옴표 처리
            if (ch == '\'') {
                // 1) '' (작은따옴표 두 개)인 경우, 그대로 추가 후 인덱스 2 증가
                if (i + 1 < input.length() && input.charAt(i + 1) == '\'') {
                    sb.append("''");
                    i += 2;
                    continue;
                }
                // 2) 백슬래시로 이스케이프된 작은따옴표인 경우, 그대로 추가
                else if (i > 0 && input.charAt(i - 1) == '\\') {
                    sb.append(ch);
                    i++;
                    continue;
                }
                // 3) 그 외의 경우, inQuotes 상태 토글 후 추가
                else {
                    inQuotes = !inQuotes;
                    sb.append(ch);
                    i++;
                    continue;
                }
            }

            // 작은따옴표 안이면 그대로 추가
            if (inQuotes) {
                sb.append(ch);
                i++;
                continue;
            } else {
                // 대괄호 처리
                if (ch == '[') {
                    bracketDepth++;
                    sb.append(ch);
                    i++;
                    continue;
                } else if (ch == ']') {
                    // 대괄호 깊이가 0 미만으로 내려가지 않도록 보장
                    bracketDepth = Math.max(0, bracketDepth - 1);
                    sb.append(ch);
                    i++;
                    continue;
                }

                // 현재 위치가 대괄호 밖(bracketDepth == 0)이고, delimiter 문자열과 일치하면 분할 처리
                if (bracketDepth == 0 && i + delimiterLength <= input.length()
                        && input.substring(i, i + delimiterLength).equals(delimiter)) {
                    addFromStringToList(sb.toString(), list);
                    sb = new StringBuilder();
                    i += delimiterLength; // delimiter 길이만큼 건너뛰기
                    continue;
                } else {
                    sb.append(ch);
                    i++;
                }
            }
        }

        // 마지막 남은 문자열이 있다면 리스트에 추가
        if (sb.length() > 0) {
            addFromStringToList(sb.toString(), list);
        }

        return list.toArray(new String[0]);
    }

    /**
     * SQL LIKE 패턴을 정규 표현식으로 변환
     *
     * @param likePattern SQL LIKE 패턴 (예: '%ABC%' 또는 'A_C%' 등)
     * @param ignoreCase  대소문자 구분 여부 (true: 대소문자 구분 안함, false: 구분함)
     * @return 정규 표현식
     */
    public static String convertLikeExprToRegex(String likePattern, boolean ignoreCase) {
        // 정규 표현식의 시작과 끝을 포함하여 변환된 패턴을 반환
        String regex = likePattern;

        // %를 .*로 변환
        if (regex.startsWith("%") && regex.endsWith("%")) {
            regex = regex.substring(1, regex.length() - 1);  // 양쪽의 %를 제거
            regex = ".*" + regex + ".*"; // 양쪽에 .*을 추가
        } else if (regex.startsWith("%")) {
            regex = regex.substring(1);  // 앞의 %만 제거
            regex = ".*" + regex + "$";  // 앞에 .* 추가
        } else if (regex.endsWith("%")) {
            regex = regex.substring(0, regex.length() - 1);  // 뒤의 %만 제거
            regex = "^" + regex + ".*";  // 뒤에 .* 추가
        }

        if (ignoreCase) regex = "(?i)" + regex; // 대소문자 구분 안함

        return regex;
    }

    public static String convertLikeExprToRegex(String likePattern) {
        return convertLikeExprToRegex(likePattern, true);
    }

    public static int indexOfOutsideQuotes(String input, String target) {
        if (input == null || input.isEmpty() || target == null || target.isEmpty() || input.length() < target.length())
            return -1;

        boolean inQuotes = false; // 작은따옴표 상태
        for (int i = 0; i <= input.length() - target.length(); i++) {
            char ch = input.charAt(i);
            if (ch == '\'') {
                // 1) ''(작은따옴표 두 번)이면 상태 토글하지 않음
                if (i + 1 < input.length() && input.charAt(i + 1) == '\'') {
                    i++;
                    continue;
                }
                // 2) \' (앞 문자가 백슬래시)인 경우에도 상태 토글하지 않음
                else if (i > 0 && input.charAt(i - 1) == '\\') {
                    continue;
                }
                // 3) 그 외의 경우에는 inQuotes 상태 반전
                else {
                    inQuotes = !inQuotes;
                    continue;
                }
            }
            if (!inQuotes && input.startsWith(target, i)) return i;
        }

        return -1;
    }

    /**
     * 문자열에서 따옴표(')와 대괄호([]) 밖에 특정 문자열(target)이 존재하는지 확인하고,
     * 존재한다면 해당 위치의 인덱스를 반환합니다.
     *
     * @param input  원본 문자열
     * @param target 찾고자 하는 문자열
     * @return target이 따옴표와 대괄호 밖에 존재하는 첫 번째 인덱스, 없으면 -1
     */
    public static int indexOfOutsideQuotesAndParenthesis(String input, String target) {
        if (input == null || target == null || target.isEmpty() || input.length() < target.length()) {
            return -1;
        }

        boolean inQuotes = false; // 작은따옴표 상태
        int bracketDepth = 0;     // 대괄호 깊이

        for (int i = 0; i <= input.length() - target.length(); i++) {
            char currentChar = input.charAt(i);

            // 작은따옴표 처리
            if (currentChar == '\'') {
                // 1) '' (작은따옴표 두 번)인 경우
                if (i + 1 < input.length() && input.charAt(i + 1) == '\'') {
                    i++; // 두 번째 작은따옴표는 무시
                    continue;
                }
                // 2) \' (앞 문자가 백슬래시)인 경우
                else if (i > 0 && input.charAt(i - 1) == '\\') {
                    // 이스케이프된 작은따옴표는 상태 변경 없이 무시
                    continue;
                }
                // 3) 그 외의 경우에는 작은따옴표 상태 토글
                else {
                    inQuotes = !inQuotes;
                    continue;
                }
            }

            // 대괄호 처리
            if (!inQuotes) { // 따옴표 안에 있을 때는 대괄호 무시
                if (currentChar == '[') {
                    bracketDepth++;
                } else if (currentChar == ']') {
                    if (bracketDepth > 0) {
                        bracketDepth--;
                    }
                }
            }

            // 현재 위치가 따옴표 밖이고 대괄호 깊이가 0인 경우에만 target 검사
            if (!inQuotes && bracketDepth == 0) {
                if (input.startsWith(target, i)) {
                    return i;
                }
            }
        }

        return -1;
    }

    /**
     * 작은따옴표 밖에서만 특정 문자열을 다른 문자열로 대체합니다.
     *
     * @param input       원본 문자열
     * @param target      바꿀 단어
     * @param replacement 대체 단어
     * @return 작은따옴표 밖에서 target이 replacement로 대체된 결과 문자열
     */
    public static String replaceOutsideQuotes(String input, String target, String replacement) {
        if (input == null || input.isEmpty() || target == null || target.isEmpty()) {
            return input;
        }

        StringBuilder sb = new StringBuilder();
        boolean inQuotes = false;
        int i = 0;

        while (i < input.length()) {
            char ch = input.charAt(i);

            // 작은따옴표 처리
            if (ch == '\'') {
                // 1) '' (작은따옴표 두 번)인 경우
                if (i + 1 < input.length() && input.charAt(i + 1) == '\'') {
                    // 상태 전환 없이 그대로 추가
                    sb.append("''");
                    i += 2;
                }
                // 2) \' (앞 문자가 백슬래시)인 경우
                else if (i > 0 && input.charAt(i - 1) == '\\') {
                    // 상태 전환 없이 그대로 추가
                    sb.append(ch);
                    i++;
                }
                // 3) 그 외에는 작은따옴표 상태 토글
                else {
                    inQuotes = !inQuotes;
                    sb.append(ch);
                    i++;
                }
            }
            // 작은따옴표 밖일 때만 target 검사
            else if (!inQuotes && input.startsWith(target, i)) {
                // target 발견 시 replacement로 교체
                sb.append(replacement);
                i += target.length();
            }
            // 그 외 문자
            else {
                sb.append(ch);
                i++;
            }
        }

        return sb.toString();
    }

    private static void addFromStringToList(String str, List<String> list) {
        if (str != null && !str.trim().isEmpty()) list.add(str.trim());
    }

    // 특정 패턴: 컬럼명 + &p 또는 &s, 숫자(1자 이상), 점(.), y/m/w/d 로 끝나는지 검사
    // 예: DS.BTDT&p20.y
    public static final Pattern DATE_CALCULATION_PATTERN =
            Pattern.compile("^(.+)&([ps])(\\d+)\\.([ymwd])$");

    public static boolean containsDateAdjustPattern(String input) {
        return DATE_CALCULATION_PATTERN.matcher(input).matches();
    }

    public static boolean isLiteralExpression(String str) {
        return str.startsWith("'") && str.endsWith("'");
    }

    public static boolean isNumeric(String str) {
        if (str == null || str.isEmpty()) return false;
        // ^[+-]? : 부호가 있을 수도, 없을 수도
        // \\d+   : 정수부 (0-9 한 자리 이상)
        // (\\.\\d+)? : 소수부 (없어도 됨)
        // $      : 문자열 끝
        return str.matches("^[+-]?\\d+(\\.\\d+)?$");
    }

    public static boolean isPositiveInteger(String str) {
        if (str == null || str.isEmpty()) return false;

        // 정규표현식 "^[1-9]\\d*$"는 첫 번째 숫자가 1~9로 시작하고, 이후에 0 이상의 숫자가 올 수 있음을 의미합니다.
        return str.matches("^[1-9]\\d*$");
    }

    public static String normalizeWhitespace(String code) {
        return code.replaceAll("\\s+", " ").trim();
    }

}
