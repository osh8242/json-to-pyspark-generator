# JSON to PySpark Generator

구조화된 JSON "레시피"를 실행 가능한 PySpark DataFrame 코드로 변환하는 가벼운 Java 라이브러리입니다.  
UI나 다른 서비스가 Spark 연산 시퀀스(Load, Select, Filter, Join 등)를 JSON으로 만들어 보내면, 이 라이브러리가 그 JSON을 파싱해서 재귀 서브쿼리, 표현식 빌딩, `show`(테이블 미리보기 또는 JSON/CSV 출력), `save` 같은 헬퍼 액션까지 포함한 완성된 PySpark 스크립트를 만들어 줍니다.

## 주요 특징
- **단계별 PySpark 코드 생성** – `PySparkChainGenerator`가 `steps[]`의 각 엔트리(load, select, filter, join, groupBy, agg, orderBy, limit, distinct, drop, withColumn* 등)를 순차적으로 처리하며, 체이닝된 PySpark DataFrame 코드를 관용적인 스타일로 생성합니다.
- **풍부한 Expression Builder** – `ExpressionBuilder`를 통해 컬럼, 리터럴, 이항/불리언 연산, 중첩 함수, CASE, BETWEEN, IS IN(튜플 지원), LIKE, (is) null 검사 등을 표현할 수 있습니다.
- **재귀 Join & 서브쿼리 지원** – join의 `right` 측에 또 다른 레시피(JSON)를 중첩해서 넣을 수 있으며, 제너레이터가 이를 재귀적으로 처리하면서 alias와 들여쓰기를 자동으로 맞춰 줍니다.
- **액션 헬퍼** – `show`(전통적인 `.show()` 프리뷰 또는 JSON/JSONL/CSV 덤프)와 `save`(JDBC/Postgres 저장) 같은 비-할당 액션을 독립된 문장으로 렌더링합니다.
- **테이블 탐색 기능** – `PySparkChainGenerator.extractTables(json)`은 입력·load·join 정보를 스캔해서 참조된 모든 테이블/데이터프레임 목록을 추출해 주며, 계보(lineage)나 의존성 분석에 활용할 수 있습니다.

## Project layout
```
└── src/main/java/com/douzone/platform
    ├── recipe
    │ ├── PySparkChainGenerator.java # 진입점 & 전체 오케스트레이션
    │ ├── builder
    │ │ ├── StepBuilder.java # 스텝 단위 렌더링
    │ │ └── ExpressionBuilder.java # Expression DSL → PySpark 변환
    │ └── util/StringUtil.java # JSON 헬퍼 & quoting 유틸
    └── util # 범용 JSON/날짜 헬퍼
```

## 시작하기
### 선행 조건
- Java 8+
- Maven 3.8+

### 빌드
```bash
mvn clean package
```
This produces `target/json-to-pyspark-generator-<version>.jar` which can be added to other JVM services.

## JSON schema at a glance
At the top level the generator expects a JSON document with a `steps` array. Each step has:
- `node`: 연산 이름 (예: load, select, filter, join, show, save).
- `input`: 입력 DataFrame 이름(옵션, 기본값은 df).
- `output`: 결과 DataFrame alias(옵션, 생략 시 in-place 변환으로 간주).
- `params`: 각 연산별 설정 값(컬럼, 표현식, join 정의, load/save 옵션 등).

일부 연산(특히 join.right)은 자체 input과 steps를 가진 별도의 JSON 레시피를 중첩해 넣을 수 있으며, 이를 통해 재귀 서브쿼리를 구성할 수 있습니다.

## Usage example
```java
import com.douzone.platform.recipe.PySparkChainGenerator;

String recipeJson = """
{
  "steps": [
    {
      "node": "load",
      "output": "orders_df",
      "params": {
        "source": "iceberg",
        "catalog": "hadoop_prod",
        "namespace": "sales",
        "table": "orders"
      }
    },
    {
      "node": "withColumn",
      "input": "orders_df",
      "output": "orders_df",
      "params": {
        "name": "order_date",
        "expr": {
          "type": "func",
          "name": "to_date",
          "args": [ { "type": "col", "name": "order_ts" } ]
        }
      }
    },
    {
      "node": "filter",
      "input": "orders_df",
      "output": "filtered",
      "params": {
        "condition": {
          "type": "between",
          "expr": { "type": "col", "name": "order_date" },
          "low": { "type": "lit", "value": "2024-01-01" },
          "high": { "type": "lit", "value": "2024-01-31" }
        }
      }
    },
    {
      "node": "show",
      "input": "filtered",
      "params": { "n": 5, "truncate": false }
    }
  ]
}
""";

String pysparkScript = PySparkChainGenerator.generate(recipeJson);
System.out.println(pysparkScript);
```
출력되는 스크립트는 대략 다음과 비슷한 형태입니다.
```python
orders_df = spark.read.table("hadoop_prod.sales.orders")
orders_df = orders_df.withColumn("order_date", F.to_date(F.col("order_ts")))
filtered = orders_df.filter((F.col("order_date")).between(F.lit("2024-01-01"), F.lit("2024-01-31")))
filtered.show(5, truncate=False)
```

### 중첩 join 예시
`StepBuilder`는 오른쪽 측이 또 다른 레시피인 재귀 join을 지원합니다.
```json
{
  "node": "join",
  "input": "orders_df",
  "output": "joined",
  "params": {
    "how": "inner",
    "on": [
      {
        "type": "op",
        "op": "=",
        "left": { "type": "col", "name": "customer_id", "table": "orders_df" },
        "right": { "type": "col", "name": "customer_id", "table": "right" }
      }
    ],
    "rightAlias": "right",
    "right": {
      "input": "customers_df",
      "steps": [
        {
          "node": "load",
          "output": "customers_df",
          "params": {
            "source": "postgres",
            "host": "db.internal",
            "database": "dwh",
            "table": "customer_dim",
            "user": "pipeline",
            "password": "***"
          }
        },
        { "node": "select", "input": "customers_df", "params": { "columns": [ { "expr": { "type": "col", "name": "customer_id" } }, { "expr": { "type": "col", "name": "segment" } } ] } }
      ]
    }
  }
}
```
`PySparkChainGenerator.buildChain` 은 이 중첩 레시피를 렌더링하고, 들여쓰기를 맞춘 뒤 join 호출의 오른쪽 DataFrame으로 삽입합니다.

### 참조된 테이블 탐색
```java
Set<String> tables = PySparkChainGenerator.extractTables(recipeJson);
// -> ["orders_df", "customers_df", "hadoop_prod.sales.orders", "customer_dim"]
```
This is useful for pre-flight dependency checks or lineage visualizations.

## 제너레이터 확장 방법
- Add new operations inside `StepBuilder` (e.g., window functions, dropna) and call them from the `switch` in `PySparkChainGenerator`.
- Extend `ExpressionBuilder` with additional `type` handlers for new DSL constructs (e.g., `regexp_extract`).
- Enhance output formatting by adjusting helper methods like `buildChain`, `indentLines`, or introducing templates.

## License
이 저장소는 아직 별도의 라이선스를 명시하고 있지 않습니다. 외부로 재배포하기 전에 반드시 프로젝트 소유자와 확인해 주세요.