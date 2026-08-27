# ShQveL component

ShQveL augments SQLancer++'s dialect-agnostic generator with SQL fragments synthesized for the target DBMS. The learned fragments are loaded into the existing statement, schema, expression, and clause generators; ShQveL does not replace those generators.

Two global options enable the integration:

- `--enable-extra-features`: enable the external feature fragments
- `--enable-learning`: enable the learning of the feature fragments

The `general` command provides finer controls. Statement, data-type, expression, and clause learning are enabled by default and can be switched off independently with `--enable-statement-learning`, `--enable-datatype-learning`, `--enable-expression-learning`, and `--enable-clause-learning`. `--learning-interval-seconds` controls the minimum interval between dynamic learning requests and defaults to 60 seconds; setting it to 0 disables throttling.

`--enable-direct-validation true` executes validation SQL through the target DBMS's JDBC driver before retaining newly learned fragments. Validation uses a separate native connection and removes candidates rejected by the DBMS. Fragment kinds without a safe validation statement remain available without direct validation.

Requirements:

- [OpenAI API Key](https://platform.openai.com/docs/api-reference/authentication)
- Python 3.12 or above

```bash
# Install the requirements for documentation retrieval
pip install -r requirements.txt
```

For example, to test DuckDB with learning, native validation, and extra features enabled, run:

```bash
OPENAI_API_KEY=YOUR_OPENAI_API_KEY java -jar target/sqlancer-2.0.0.jar \
  --use-reducer --enable-extra-features --enable-learning --num-threads 1 --num-tries 200 \
  general --database-engine duckdb --oracle WHERE --enable-direct-validation true
```
