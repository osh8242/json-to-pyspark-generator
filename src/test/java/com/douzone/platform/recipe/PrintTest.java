package com.douzone.platform.recipe;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.douzone.platform.recipe.util.TestUtil.printTestInfo;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * description    :
 * ===========================================================
 * DATE              AUTHOR             NOTE
 * -----------------------------------------------------------
 * 2025. 11. 17.        osh8242       최초 생성
 */
public class PrintTest {

    // =========================
    // JSONL 관련 테스트
    // =========================

    @Test
    @DisplayName("Print(JSONL): df 에서 상위 N개를 JSON Lines 로 출력 (기본, header=false)")
    void testPrintJsonLinesWithN() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"print\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"params\": {\n"
                + "        \"n\": 100,\n"
                + "        \"format\": \"jsonl\"\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected =
                "for line in df.limit(100).toJSON().collect():\n"
                        + "    print(line)\n";

        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testPrintJsonLinesWithN", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Print(JSONL): n, format 미지정 시 기본값 사용 (format=jsonl, n=100, header=false)")
    void testPrintJsonLinesDefaultParams() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"print\",\n"
                + "      \"input\": \"df\"\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected =
                "for line in df.limit(100).toJSON().collect():\n"
                        + "    print(line)\n";

        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testPrintJsonLinesDefaultParams", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Print(JSONL): 커스텀 input DataFrame 에 대해 출력 (header=false)")
    void testPrintJsonLinesCustomInput() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"print\",\n"
                + "      \"input\": \"df_result\",\n"
                + "      \"params\": {\n"
                + "        \"n\": 50,\n"
                + "        \"format\": \"jsonl\"\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected =
                "for line in df_result.limit(50).toJSON().collect():\n"
                        + "    print(line)\n";

        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testPrintJsonLinesCustomInput", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Print(JSONL): select 이후 결과에 대해 JSON Lines 출력 (header=false)")
    void testSelectThenPrintJsonLines() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"select\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"output\": \"df_selected\",\n"
                + "      \"params\": {\n"
                + "        \"columns\": [\n"
                + "          { \"expr\": { \"type\": \"col\", \"name\": \"name\" } },\n"
                + "          { \"expr\": { \"type\": \"col\", \"name\": \"age\" } }\n"
                + "        ]\n"
                + "      }\n"
                + "    },\n"
                + "    {\n"
                + "      \"node\": \"print\",\n"
                + "      \"input\": \"df_selected\",\n"
                + "      \"params\": {\n"
                + "        \"n\": 10,\n"
                + "        \"format\": \"jsonl\"\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected =
                "df_selected = df.select(F.col(\"name\"), F.col(\"age\"))\n"
                        + "for line in df_selected.limit(10).toJSON().collect():\n"
                        + "    print(line)\n";

        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testSelectThenPrintJsonLines", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Print(JSONL): header=true 인 경우 스키마 JSON + JSON Lines 출력")
    void testPrintJsonLinesWithHeader() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"print\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"params\": {\n"
                + "        \"n\": 10,\n"
                + "        \"format\": \"jsonl\",\n"
                + "        \"header\": true\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected =
                "import json\n"
                        + "_schema = df.dtypes\n"
                        + "_schema_header = {name: dtype for (name, dtype) in _schema}\n"
                        + "print(json.dumps(_schema_header, ensure_ascii=False))\n"
                        + "for line in df.limit(10).toJSON().collect():\n"
                        + "    print(line)\n";

        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testPrintJsonLinesWithHeader", json, actual);
        assertEquals(expected, actual);
    }

    // =========================
    // JSON 배열 관련 테스트
    // =========================

    @Test
    @DisplayName("Print(JSON Array): JSON 배열 형식으로 출력 (header=false)")
    void testPrintJsonArray() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"print\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"params\": {\n"
                + "        \"n\": 5,\n"
                + "        \"format\": \"json\"\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected =
                "import json\n"
                        + "_rows = df.limit(5).toJSON().collect()\n"
                        + "_data = [json.loads(r) for r in _rows]\n"
                        + "print(json.dumps(_data, ensure_ascii=False))\n";

        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testPrintJsonArray", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Print(JSON Array): header=true 인 경우 [스키마 JSON, 데이터...] 배열 출력")
    void testPrintJsonArrayWithHeader() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"print\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"params\": {\n"
                + "        \"n\": 5,\n"
                + "        \"format\": \"json\",\n"
                + "        \"header\": true\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected =
                "import json\n"
                        + "_rows = df.limit(5).toJSON().collect()\n"
                        + "_schema = df.dtypes\n"
                        + "_schema_header = {name: dtype for (name, dtype) in _schema}\n"
                        + "_data = [_schema_header] + [json.loads(r) for r in _rows]\n"
                        + "print(json.dumps(_data, ensure_ascii=False))\n";

        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testPrintJsonArrayWithHeader", json, actual);
        assertEquals(expected, actual);
    }

    // =========================
    // CSV 관련 테스트
    // =========================

    @Test
    @DisplayName("Print(CSV): 헤더 포함 CSV 형식으로 출력 (컬럼명.컬럼타입)")
    void testPrintCsvWithHeader() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"print\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"params\": {\n"
                + "        \"n\": 10,\n"
                + "        \"format\": \"csv\",\n"
                + "        \"header\": true\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected =
                "import csv, sys\n"
                        + "_df_preview = df.limit(10)\n"
                        + "_rows = _df_preview.collect()\n"
                        + "_cols = _df_preview.columns\n"
                        + "_schema = _df_preview.dtypes\n"
                        + "writer = csv.writer(sys.stdout, delimiter=',')\n"
                        + "_header = [name + '.' + dtype for (name, dtype) in _schema]\n"
                        + "writer.writerow(_header)\n"
                        + "for r in _rows:\n"
                        + "    writer.writerow([r[c] for c in _cols])\n";

        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testPrintCsvWithHeader", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Print(CSV): 헤더 없이 CSV 형식으로 출력")
    void testPrintCsvWithoutHeader() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"print\",\n"
                + "      \"input\": \"df_result\",\n"
                + "      \"params\": {\n"
                + "        \"n\": 3,\n"
                + "        \"format\": \"csv\",\n"
                + "        \"header\": false\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected =
                "import csv, sys\n"
                        + "_df_preview = df_result.limit(3)\n"
                        + "_rows = _df_preview.collect()\n"
                        + "_cols = _df_preview.columns\n"
                        + "_schema = _df_preview.dtypes\n"
                        + "writer = csv.writer(sys.stdout, delimiter=',')\n"
                        + "for r in _rows:\n"
                        + "    writer.writerow([r[c] for c in _cols])\n";

        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testPrintCsvWithoutHeader", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Print(CSV): 커스텀 구분자와 헤더(컬럼명.컬럼타입)로 CSV 출력")
    void testPrintCsvWithCustomDelimiter() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"print\",\n"
                + "      \"input\": \"df\",\n"
                + "      \"params\": {\n"
                + "        \"n\": 10,\n"
                + "        \"format\": \"csv\",\n"
                + "        \"header\": true,\n"
                + "        \"delimiter\": \"|\"\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected =
                "import csv, sys\n"
                        + "_df_preview = df.limit(10)\n"
                        + "_rows = _df_preview.collect()\n"
                        + "_cols = _df_preview.columns\n"
                        + "_schema = _df_preview.dtypes\n"
                        + "writer = csv.writer(sys.stdout, delimiter='|')\n"
                        + "_header = [name + '.' + dtype for (name, dtype) in _schema]\n"
                        + "writer.writerow(_header)\n"
                        + "for r in _rows:\n"
                        + "    writer.writerow([r[c] for c in _cols])\n";

        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testPrintCsvWithCustomDelimiter", json, actual);
        assertEquals(expected, actual);
    }

}
