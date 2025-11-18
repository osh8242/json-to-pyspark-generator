# JSON to PySpark Generator

A lightweight Java library that turns a structured JSON "recipe" into executable PySpark DataFrame code.  
It is designed for low-code data pipelines where a UI or another service emits JSON describing a sequence of Spark operations (load, select, filter, join, etc.). The library parses that JSON and produces fully formatted PySpark scripts, including recursive sub-queries, expression building, and helper actions such as `show` (table preview or JSON/CSV emit), and `save`.

## Key features
- **Step-by-step PySpark generation** – `PySparkChainGenerator` walks through every `steps[]` entry (load, select, filter, join, groupBy, agg, orderBy, limit, distinct, drop, withColumn*, etc.) and emits idiomatic chained DataFrame code.
- **Rich expression builder** – Supports columns, literals, binary/boolean ops, nested functions, CASE, BETWEEN, IS IN (tuple aware), LIKE, and (is) null checks via `ExpressionBuilder`.
- **Recursive joins & sub-queries** – A join's `right` side can itself be a nested recipe; the generator handles aliasing and indentation automatically.
- **Action helpers** – Non-assignment actions such as `show` (classic `.show()` preview or JSON/JSONL/CSV dumps) and `save` (JDBC/Postgres) are rendered as standalone statements.
- **Table discovery** – `PySparkChainGenerator.extractTables(json)` scans inputs, loads, and joins to list all referenced tables/dataframes for lineage and dependency analysis.

## Project layout
```
└── src/main/java/com/douzone/platform
    ├── recipe
    │   ├── PySparkChainGenerator.java      # Entry point & orchestration
    │   ├── builder
    │   │   ├── StepBuilder.java            # Per-step rendering
    │   │   └── ExpressionBuilder.java      # Expression DSL → PySpark
    │   └── util/StringUtil.java            # JSON helpers & quoting utilities
    └── util                               # Generic JSON/date helpers
```

## Getting started
### Prerequisites
- Java 8+
- Maven 3.8+

### Build
```bash
mvn clean package
```
This produces `target/json-to-pyspark-generator-<version>.jar` which can be added to other JVM services.

## JSON schema at a glance
At the top level the generator expects a JSON document with a `steps` array. Each step has:
- `node`: the operation name (e.g., `load`, `select`, `filter`, `join`, `show`, `save`).
- `input`: optional name of the input DataFrame (defaults to `df`).
- `output`: optional target DataFrame alias; absent implies in-place transformation.
- `params`: operation-specific payload (columns, expressions, join definition, load/save options, etc.).

Some operations (notably `join.right`) can embed another JSON recipe with its own `input` & `steps`, enabling recursive sub-queries.

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
The printed script would look similar to:
```python
orders_df = spark.read.table("hadoop_prod.sales.orders")
orders_df = orders_df.withColumn("order_date", F.to_date(F.col("order_ts")))
filtered = orders_df.filter((F.col("order_date")).between(F.lit("2024-01-01"), F.lit("2024-01-31")))
filtered.show(5, truncate=False)
```

### Nested join example
`StepBuilder` supports recursive joins where the right side is another recipe:
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
`PySparkChainGenerator.buildChain` renders the nested recipe, indents it, and uses it as the right-hand DataFrame in the join call.

### Discovering referenced tables
```java
Set<String> tables = PySparkChainGenerator.extractTables(recipeJson);
// -> ["orders_df", "customers_df", "hadoop_prod.sales.orders", "customer_dim"]
```
This is useful for pre-flight dependency checks or lineage visualizations.

## Extending the generator
- Add new operations inside `StepBuilder` (e.g., window functions, dropna) and call them from the `switch` in `PySparkChainGenerator`.
- Extend `ExpressionBuilder` with additional `type` handlers for new DSL constructs (e.g., `regexp_extract`).
- Enhance output formatting by adjusting helper methods like `buildChain`, `indentLines`, or introducing templates.

## License
This repository does not currently declare a license. Please confirm with the project owners before external redistribution.
