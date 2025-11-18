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
public class ShowFormatTest {

    // =========================
    // JSONL 관련 테스트
    // =========================

    @Test
    @DisplayName("Show(JSONL): df 에서 상위 N개를 JSON Lines 로 출력 (기본, header=false)")
    void testShowJsonLinesWithN() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"show\",\n"
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

        printTestInfo("testShowJsonLinesWithN", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Show(JSONL): n, format 미지정 시 기본값 사용 (format=jsonl, n=100, header=false)")
    void testShowJsonLinesDefaultParams() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"show\",\n"
                + "      \"input\": \"df\"\n"
                + "    }\n"
                + "  ]\n"
                + "}";

        String expected =
                "for line in df.limit(100).toJSON().collect():\n"
                        + "    print(line)\n";

        String actual = PySparkChainGenerator.generate(json);

        printTestInfo("testShowJsonLinesDefaultParams", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Show(JSONL): 커스텀 input DataFrame 에 대해 출력 (header=false)")
    void testShowJsonLinesCustomInput() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"show\",\n"
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

        printTestInfo("testShowJsonLinesCustomInput", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Show(JSONL): select 이후 결과에 대해 JSON Lines 출력 (header=false)")
    void testSelectThenShowJsonLines() throws Exception {
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
                + "      \"node\": \"show\",\n"
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

        printTestInfo("testSelectThenShowJsonLines", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Show(JSONL): header=true 인 경우 스키마 JSON + JSON Lines 출력")
    void testShowJsonLinesWithHeader() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"show\",\n"
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

        printTestInfo("testShowJsonLinesWithHeader", json, actual);
        assertEquals(expected, actual);
    }

    // =========================
    // JSON 배열 관련 테스트
    // =========================

    @Test
    @DisplayName("Show(JSON Array): JSON 배열 형식으로 출력 (header=false)")
    void testShowJsonArray() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"show\",\n"
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

        printTestInfo("testShowJsonArray", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Show(JSON Array): header=true 인 경우 [스키마 JSON, 데이터...] 배열 출력")
    void testShowJsonArrayWithHeader() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"show\",\n"
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

        printTestInfo("testShowJsonArrayWithHeader", json, actual);
        assertEquals(expected, actual);
    }

    // =========================
    // CSV 관련 테스트
    // =========================

    @Test
    @DisplayName("Show(CSV): 헤더 포함 CSV 형식으로 출력 (컬럼명.컬럼타입)")
    void testShowCsvWithHeader() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"show\",\n"
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

        printTestInfo("testShowCsvWithHeader", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Show(CSV): 헤더 없이 CSV 형식으로 출력")
    void testShowCsvWithoutHeader() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"show\",\n"
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

        printTestInfo("testShowCsvWithoutHeader", json, actual);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("Show(CSV): 커스텀 구분자와 헤더(컬럼명.컬럼타입)로 CSV 출력")
    void testShowCsvWithCustomDelimiter() throws Exception {
        String json = "{\n"
                + "  \"steps\": [\n"
                + "    {\n"
                + "      \"node\": \"show\",\n"
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

        printTestInfo("testShowCsvWithCustomDelimiter", json, actual);
        assertEquals(expected, actual);
    }

}
