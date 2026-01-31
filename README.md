# SQLancer++

A tool for testing SQL database management systems with automatic dialect adaptability.

## Getting Started

Requirements:
* Java 11 or above
* [Maven](https://maven.apache.org/) (`sudo apt install maven` on Ubuntu)
* The DBMS that you want to test (embedded DBMSs such as DuckDB, H2, and SQLite do not require a setup)


```
# unzip the file first then get into the directory
cd sqlancer
mvn package -DskipTests
java -jar target/sqlancer-2.0.0.jar --use-reducer general  --database-engine sqlite
```

If the execution prints progress information every five seconds, then the tool works as expected. Note that SQLancer++ might find bugs in SQLite. Before reporting these, be sure to check that they can still be reproduced when using the latest development version. The shortcut CTRL+C can be used to terminate SQLancer++ manually. If SQLancer++ does not find any bugs, it executes infinitely. The option `--num-tries` can be used to control after how many bugs SQLancer++ terminates. Alternatively, the option `--timeout-seconds` can be used to specify the maximum duration that SQLancer++ is allowed to run.


----

## Adapt SQLancer++ to custom DBMSs

To add support for a new DBMS:

1. **Add an enum** in `GeneralDatabaseEngineFactory` inside `GeneralOptions.java` (e.g., `MYDBMS`).
2. **Configure JDBC** in `dbconfigs/jdbc.properties`: supply a URL template and default connection properties for your engine (with placeholders `{host}`, `{port}`, `{user}`, `{password}`).
3. **Optional:** Provide DBMS-specific environment setup or custom initialization by overriding `cleanOrSetUpDatabase` in the enum if your system needs more than the default database creation and table cleanup logic (see `COCKROACHDB` for an example).

By default, SQLancer++ will use `cleanOrSetUpDatabase` method to create a clean space for tables after connecting to the system by using above JDBC String. It will first try to `CREATE DATABASE` and `USE` it (like in MySQL). If it fails, it will try to blindly use `DROP TABLE` to clean all the possible tables.

The general workflow of SQLancer++ is as follows:
1. Connect to a database
2. Setup a new database state
3. Query the database and validate the results

## Steps

1. Add a new enum value `$DBMS` in `GeneralDatabaseEngineFactory` (`GeneralOptions.java`) and add the corresponding entries in `dbconfigs/jdbc.properties`. Build with `mvn package -DskipTests`.
2. Start your DBMS instance.
3. Run: `java -jar target/sqlancer-2.0.0.jar --use-reducer general --database-engine $DBMS`
4. Check `logs/general` or the shell output for bugs.
   - `*-cur.log`: all executed statements
   - `*.log`: statements that triggered a potential bug (reduced if `--use-reducer` is enabled)

## Arguments

You could adjust some arguments to control the testing process. Here are some of them which might be helpful:

- `--num-threads $i`: the number of threads to run the test. The default value is 4. You could set it to a higher value if you have a powerful machine and there are not so many bugs. Set to 1 if there is too many issues.
- `--use-reducer`: enable the reducer to reduce the bug-triggering query. Do not enable it if you want to see the full SQL statements.
- `--oracle $ORACLE`: the oracle to use. The default value is `WHERE`. You could also try `NoREC`.
- `--use-deduplicator`: enable the bug deduplicator to reduce duplication in best effort. To enable it, add `--use-deduplicator` after `general` in the command.
