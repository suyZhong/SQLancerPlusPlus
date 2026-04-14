package sqlancer.general;

import java.io.BufferedReader;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.auto.service.AutoService;

import sqlancer.AbstractAction;
import sqlancer.DatabaseEngineFactory;
import sqlancer.DatabaseProvider;
import sqlancer.ExecutionTimer;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.Reproducer;
import sqlancer.SQLConnection;
import sqlancer.SQLGlobalState;
import sqlancer.SQLProviderAdapter;
import sqlancer.StatementExecutor;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.Query;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.general.GeneralErrorHandler.GeneratorNode;
import sqlancer.general.GeneralOptions.GeneralDatabaseEngineFactory;
import sqlancer.general.GeneralSchema.GeneralTable;
import sqlancer.general.ast.GeneralBinaryOperator;
import sqlancer.general.ast.GeneralFunction;
import sqlancer.general.gen.GeneralAlterTableGenerator;
import sqlancer.general.gen.GeneralDeleteGenerator;
import sqlancer.general.gen.GeneralIndexGenerator;
import sqlancer.general.gen.GeneralInsertGenerator;
import sqlancer.general.gen.GeneralStatementGenerator;
import sqlancer.general.gen.GeneralTableGenerator;
import sqlancer.general.gen.GeneralUpdateGenerator;
import sqlancer.general.gen.GeneralViewGenerator;
import sqlancer.general.learner.GeneralFragments;

@AutoService(DatabaseProvider.class)
public class GeneralProvider extends SQLProviderAdapter<GeneralProvider.GeneralGlobalState, GeneralOptions> {

    public GeneralProvider() {
        super(GeneralGlobalState.class, GeneralOptions.class);
    }

    public enum Action implements AbstractAction<GeneralGlobalState> {

        INSERT(GeneralInsertGenerator::getQuery), //
        CREATE_INDEX(GeneralIndexGenerator::getQuery), //
        VACUUM((g) -> {
            ExpectedErrors errors = new ExpectedErrors();
            GeneralErrors.addExpressionErrors(errors);
            g.handler.addScore(GeneratorNode.VACUUM);
            return new SQLQueryAdapter("VACUUM;", errors);
        }), ANALYZE((g) -> {
            ExpectedErrors errors = new ExpectedErrors();
            GeneralErrors.addExpressionErrors(errors);
            g.handler.addScore(GeneratorNode.ANALYZE);
            return new SQLQueryAdapter("ANALYZE;", errors);
        }), //
        DELETE(GeneralDeleteGenerator::generate), UPDATE(GeneralUpdateGenerator::getQuery),
        GENERAL_COMMAND(GeneralStatementGenerator::getQuery), ALTER_TABLE(GeneralAlterTableGenerator::getQuery), //
        CREATE_VIEW(GeneralViewGenerator::generate); //
        // EXPLAIN((g) -> {
        // ExpectedErrors errors = new ExpectedErrors();
        // GeneralErrors.addExpressionErrors(errors);
        // GeneralErrors.addGroupByErrors(errors);
        // return new SQLQueryAdapter(
        // "EXPLAIN " + GeneralToStringVisitor
        // .asString(GeneralRandomQuerySynthesizer.generateSelect(g,
        // Randomly.smallNumber() + 1)),
        // errors);
        // })

        private final SQLQueryProvider<GeneralGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<GeneralGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(GeneralGlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }

        public static Action[] getAvailableActions(GeneralErrorHandler handler) {
            // return all the actions that is true in the generator options
            return Arrays.stream(values()).filter(a -> handler.getOption(GeneratorNode.valueOf(a.toString())))
                    .toArray(Action[]::new);
        }
    }

    private static int mapActions(GeneralGlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        switch (a) {
        case INSERT:
            return r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
        case CREATE_INDEX:
            if (!globalState.getDbmsSpecificOptions().testIndexes) {
                return 0;
            }
            // fall through
            return r.getInteger(1, globalState.getDbmsSpecificOptions().maxNumUpdates + 1);
        case VACUUM: // seems to be ignored
        case ANALYZE: // seems to be ignored
            return r.getInteger(0, 2);
        case UPDATE:
            return r.getInteger(0, globalState.getDbmsSpecificOptions().maxNumUpdates + 1);
        case DELETE:
        case ALTER_TABLE:
        case CREATE_VIEW:
            return r.getInteger(0, globalState.getDbmsSpecificOptions().maxNumViews + 1);
        case GENERAL_COMMAND:
            return r.getInteger(5, 10);
        default:
            throw new AssertionError(a);
        }
    }

    public static class GeneralGlobalState extends SQLGlobalState<GeneralOptions, GeneralSchema> {
        private GeneralSchema schema = new GeneralSchema(new ArrayList<>());
        private final GeneralErrorHandler handler = new GeneralErrorHandler();
        private final GeneralLearningManager manager = new GeneralLearningManager();
        private GeneralTable updateTable;
        private boolean creatingDatabase; // is currently creating database

        private final Map<String, String> testObjectMap = new HashMap<>();

        private static final File CONFIG_DIRECTORY = new File("dbconfigs");

        @Override
        public GeneralSchema getSchema() {
            // TODO should we also check here if the saved schema match the jdbc schema?
            return schema;
        }

        public String replaceTestObject(String key) {
            if (testObjectMap.isEmpty()) {
                return key;
            } else {
                String result = key;
                for (Map.Entry<String, String> entry : testObjectMap.entrySet()) {
                    result = result.replace(entry.getKey(), entry.getValue());
                }
                return result;
            }
        }

        public void setTestObject(String key, String value) {
            testObjectMap.put(key, value);
        }

        public void cleanTestObject() {
            testObjectMap.clear();
        }

        public GeneralErrorHandler getHandler() {
            return handler;
        }

        public GeneralLearningManager getLearningManager() {
            return manager;
        }

        public String getProviderName() {
            return getDbmsSpecificOptions().getDatabaseEngineFactory().toString();
        }

        public void setSchema(List<GeneralTable> tables) {
            this.schema = new GeneralSchema(tables);
        }

        public void setUpdateTable(GeneralTable updateTable) {
            this.updateTable = updateTable;
        }

        public GeneralTable getUpdateTable() {
            return updateTable;
        }

        public boolean getCreatingDatabase() {
            return creatingDatabase;
        }

        public void setCreatingDatabase(boolean creatingDatabase) {
            this.creatingDatabase = creatingDatabase;
        }

        @Override
        public void updateSchema() {
            if (updateTable != null) {
                List<GeneralTable> databaseTables = new ArrayList<>(schema.getDatabaseTables());
                boolean found = false;
                // substitute or add the table with the new one according to getName
                for (int i = 0; i < databaseTables.size(); i++) {
                    if (databaseTables.get(i).getName().equals(updateTable.getName())) {
                        databaseTables.set(i, updateTable);
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    databaseTables.add(updateTable);
                }
                setSchema(databaseTables);
            }
            updateTable = null;
        }

        @Override
        public void executeEpilogue(Query<?> q, boolean success, ExecutionTimer timer) throws Exception {
            boolean logExecutionTime = getOptions().logExecutionTime();
            if (success && getOptions().printSucceedingStatements()) {
                System.out.println(q.getQueryString());
            }
            if (logExecutionTime && success) {
                getLogger().writeCurrent(" -- " + timer.end().asString());
            }
            if (q.couldAffectSchema() && success) {
                updateSchema();
            }
        }

        @Override
        protected GeneralSchema readSchema() throws SQLException {
            // TODO refactor things here
            return schema;
        }

        // Override execute statement
        @Override
        public boolean executeStatement(Query<SQLConnection> q, String... fills) throws Exception {
            boolean success = false;
            try {
                success = super.executeStatement(q, fills);
            } catch (Exception e) {
                handler.appendScoreToTable(false, false, q.getUnterminatedQueryString(), e.getMessage());
                getLogger().writeCurrent(" -- " + e.getMessage());
                throw e;
            }
            if (!success) {
                // The error was an expected error caught inside SQLQueryAdapter.execute().
                String errorMsg = (q instanceof SQLQueryAdapter)
                        ? ((SQLQueryAdapter) q).getLastErrorMessage() : null;
                handler.appendScoreToTable(false, false, q.getUnterminatedQueryString(), errorMsg);
            } else {
                handler.appendScoreToTable(true, false, q.getUnterminatedQueryString());
            }
            return success;
        }

        @Override
        public void updateHandler(boolean status) {
            // status means whether the execution is stopped by bug or not
            String databaseName = getDatabaseName();
            if (getOptions().enableLearning()) {
                GeneralTableGenerator.getFragments().dumpFragments(this);
                GeneralIndexGenerator.getFragments().dumpFragments(this);
                GeneralSchema.getFragments().dumpFragments(this);
                GeneralStatementGenerator.getFragments().dumpFragments(this);
                if (status && Randomly.getBoolean()) {
                    // randomly pick one of the fragment to update by LLM
                    GeneralFragments f = Randomly.fromOptions(GeneralTableGenerator.getFragments(),
                            GeneralIndexGenerator.getFragments(), GeneralStatementGenerator.getFragments());
                    f.updateFragmentsFromLearner(this);
                }
            }
            if (getDbmsSpecificOptions().enableErrorHandling) {
                GeneralErrorHandler.incrementExecDatabaseNum();
                if (!status) {
                    // print the last item of handler.
                    System.out.println(databaseName);
                    System.out.println(handler.getLastGeneratorScore());
                    handler.appendHistory(databaseName);
                } else {
                    handler.calcAverageScore();
                    if (getDbmsSpecificOptions().enableFeedback) {
                        handler.updateGeneratorOptions();
                        if (getOptions().enableExtraFeatures()) {
                            handler.updateFragments();
                        }
                    }
                    if (getDbmsSpecificOptions().untypeExpr) {
                        handler.setOption(GeneratorNode.UNTYPE_EXPR, true);
                    }
                }
                if (getOptions().debugLogs()) {
                    handler.printStatistics();
                }
                handler.saveStatistics(this);
                handler.dumpFeatureStatistics(this);
                handler.dumpCompositeExamples(this);
                if (handler.getCurDepth(databaseName) < getOptions().getMaxExpressionDepth()) {
                    handler.incrementCurDepth(databaseName);
                }
                if (getOptions().debugLogs()) {
                    System.out.println(databaseName + "Current depth: " + handler.getCurDepth(databaseName));
                }
            }
        }

        @Override
        public boolean checkIfDuplicate() {
            if (!getDbmsSpecificOptions().useDeduplicator) {
                return false;
            }
            return handler.checkIfDuplicate();
        }

        public File getConfigDirectory() {
            return new File(CONFIG_DIRECTORY,
                    getDbmsSpecificOptions().getDatabaseEngineFactory().toString().toLowerCase());
        }

        public String getDbmsNameForLearning() {
            GeneralOptions options = getDbmsSpecificOptions();
            if (options.compatibleWith != "") {
                return String.format("%s (compatible with %s)", options.getDatabaseEngineFactory().toString(),
                        options.compatibleWith);
            } else {
                return options.getDatabaseEngineFactory().toString();
            }
        }

        public boolean checkIfQueriesAreValid(GeneralGlobalState globalState, List<String> queries,
                String databaseName) {
            GeneralDatabaseEngineFactory databaseEngine = globalState.getDbmsSpecificOptions()
                    .getDatabaseEngineFactory();
            try (Connection conn = DriverManager.getConnection(databaseEngine.getJDBCString(globalState))) {
                try (Statement s = conn.createStatement()) {
                    s.execute("DROP TABLE " + databaseName);
                } catch (SQLException e) {
                    // do nothing
                }
                Statement stmt = conn.createStatement();
                for (String query : queries) {
                    stmt.addBatch(query);
                }
                stmt.executeBatch();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                // System.out.println("Error: " + e.getMessage());
                return false;
            }
            return true;
        }

    }

    // TODO: we might need another method to check if there's any data in the table
    @Override
    protected void checkViewsAreValid(GeneralGlobalState globalState) {
        List<GeneralTable> views = globalState.getSchema().getViews();
        for (GeneralTable view : views) {
            SQLQueryAdapter q = new SQLQueryAdapter("SELECT * FROM " + view.getName(), new ExpectedErrors(), false,
                    globalState.getOptions().canonicalizeSqlString());
            try {
                if (!q.execute(globalState)) {
                    dropView(globalState, view.getName());
                }
            } catch (Throwable t) {
                dropView(globalState, view.getName());
            }
        }
        String sb = "SELECT COUNT(*) FROM ";
        List<GeneralTable> databaseTables = globalState.getSchema().getDatabaseTables();
        // Select all the tables using cross join
        for (int i = 0; i < databaseTables.size(); i++) {
            sb += databaseTables.get(i).getName();
            if (i != databaseTables.size() - 1) {
                sb += ", ";
            }
        }
        // check if query result is larger than 1000 rows
        SQLQueryAdapter q2 = new SQLQueryAdapter(sb, new ExpectedErrors(), false,
                globalState.getOptions().canonicalizeSqlString());
        SQLancerResultSet resultSet;
        try {
            resultSet = q2.executeAndGet(globalState);
            globalState.getLogger().writeCurrent(sb);
            // check if the result is larger than 100K
            while (resultSet.next()) {
                if (globalState.getOptions().debugLogs()) {
                    System.out.println("Join table size: " + resultSet.getLong(1));
                }
                if (resultSet.getLong(1) > 5000) {
                    // drop all the views
                    globalState.getLogger().writeCurrent("-- size:" + resultSet.getLong(1));
                    System.out.println("Join table size exceeds 10000, dropping all views");
                    for (GeneralTable view : views) {
                        dropView(globalState, view.getName());
                    }
                }
            }
        } catch (Throwable t) {
            throw new AssertionError(t);
        }
        if (globalState.getOptions().debugLogs()) {
            globalState.getSchema().printTables();
        }
    }

    private void dropView(GeneralGlobalState globalState, String viewName) {
        try {
            globalState.getLogger().writeCurrent("DROP VIEW " + viewName);
            globalState.getManager().execute(new SQLQueryAdapter("DROP VIEW " + viewName, true));
        } catch (Throwable t2) {
            globalState.getLogger().writeCurrent(" -- " + t2.getMessage());
        } finally {
            List<GeneralTable> databaseTables = new ArrayList<>(globalState.getSchema().getDatabaseTables());
            for (int i = 0; i < databaseTables.size(); i++) {
                if (databaseTables.get(i).getName().equals(viewName)) {
                    databaseTables.remove(i);
                    break;
                }
            }
            globalState.setSchema(databaseTables);
        }
    }

    public boolean checkTableIsValid(GeneralGlobalState globalState, String tableName) {
        globalState.getLogger().writeCurrent("SELECT * FROM " + tableName);
        SQLQueryAdapter q = new SQLQueryAdapter("SELECT * FROM " + tableName, new ExpectedErrors(), false,
                globalState.getOptions().canonicalizeSqlString());
        try {
            if (!q.execute(globalState)) {
                dropTable(globalState, tableName);
                return false;
            }
        } catch (Throwable t) {
            // if it's query error, we need to drop the table
            globalState.getLogger().writeCurrent("-- query failed " + t.getMessage());
            dropTable(globalState, tableName);
            return false;
        }
        return true;
    }

    private void dropTable(GeneralGlobalState globalState, String tableName) {
        try {
            String dropStmt = globalState.getDbmsSpecificOptions().getDatabaseEngineFactory()
                    .getDropTableStatement(tableName);
            globalState.getLogger().writeCurrent(dropStmt);
            globalState.getManager().execute(new SQLQueryAdapter(dropStmt, true));
        } catch (Throwable t2) {
            globalState.getLogger().writeCurrent("-- Warning: drop table fail");
        } finally {
            List<GeneralTable> databaseTables = new ArrayList<>(globalState.getSchema().getDatabaseTables());
            for (int i = 0; i < databaseTables.size(); i++) {
                if (databaseTables.get(i).getName().equals(tableName)) {
                    databaseTables.remove(i);
                    break;
                }
            }
            globalState.setSchema(databaseTables);
        }
    }

    @Override
    public void generateDatabase(GeneralGlobalState globalState) throws Exception {
        DatabaseEngineFactory<GeneralGlobalState> databaseEngineFactory = globalState.getDbmsSpecificOptions()
                .getDatabaseEngineFactory();
        // globalState.setCreatingDatabase(true);
        GeneralSchema.GeneralDataType.calcWeight();
        for (int i = 0; i < Randomly.fromOptions(1, 2); i++) {
            boolean success;
            int nrTries = 0;
            do {
                SQLQueryAdapter qt = GeneralTableGenerator.getQuery(globalState);
                GeneralTable updateTable = globalState.getUpdateTable();
                // TODO add error handling here
                success = globalState.executeStatement(qt);
                // We need to check if the table could be select
                if (success) {
                    success = checkTableIsValid(globalState, updateTable.getName());
                }
            } while (!success && nrTries++ < 500);
        }
        if (globalState.getSchema().getDatabaseTables().isEmpty()) {
            throw new AssertionError("Failed to create any table"); // TODO
        }
        StatementExecutor<GeneralGlobalState, Action> se = new StatementExecutor<>(globalState,
                Action.getAvailableActions(globalState.getHandler()), GeneralProvider::mapActions, (q) -> {
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                    // check if the update table consumed
                    if (q.couldAffectSchema() && globalState.getUpdateTable() != null) {
                        throw new AssertionError();
                    }
                });
        se.executeStatements();
        databaseEngineFactory.syncData(globalState);
        // execute the general commands
        if (globalState.getDbmsSpecificOptions().testRandomCommands) {
            for (int i = 0; i < Randomly.fromOptions(5, 6, 7); i++) {
                SQLQueryAdapter sg = GeneralStatementGenerator.getQuery(globalState);
                globalState.executeStatement(sg);
            }
        }
        // globalState.setCreatingDatabase(false);
    }

    public void tryDeleteFile(String fname) {
        try {
            File f = new File(fname);
            f.delete();
        } catch (Exception e) {
        }
    }

    public void tryDeleteDatabase(String dbpath) {
        if (dbpath.equals("") || dbpath.equals(":memory:")) {
            return;
        }
        tryDeleteFile(dbpath);
        tryDeleteFile(dbpath + ".wal");
    }

    @Override
    public SQLConnection createDatabase(GeneralGlobalState globalState) throws SQLException {
        DatabaseEngineFactory<GeneralGlobalState> databaseEngineFactory = globalState.getDbmsSpecificOptions()
                .getDatabaseEngineFactory();
        String databaseName = globalState.getDatabaseName();

        // Try CREATE DATABASE:
        Connection conn = databaseEngineFactory.cleanOrSetUpDatabase(globalState, databaseName);
        globalState.getHandler().setOption(GeneratorNode.CREATE_DATABASE, databaseEngineFactory.isNewSchema());

        return new SQLConnection(conn);
    }

    @Override
    public String getDBMSName() {
        return "general";
    }

    @Override
    public void initializeFeatures(GeneralGlobalState globalState) {
        GeneralSchema.getFragments().loadFragmentsFromFile(globalState);
        GeneralSchema.GeneralDataType.calcWeight();
        GeneralTableGenerator.getFragments().loadFragmentsFromFile(globalState);
        GeneralIndexGenerator.getFragments().loadFragmentsFromFile(globalState);
        GeneralFunction.loadFunctionsFromFile(globalState);
        GeneralBinaryOperator.getFragments().loadFragmentsFromFile(globalState);
        GeneralBinaryOperator.loadOperatorsFromFragments(globalState);

        if (globalState.getOptions().enableLearning()) {
            GeneralStatementGenerator.getFragments().updateFragmentsFromLearner(globalState);
            GeneralSchema.getFragments().updateFragmentsFromLearner(globalState);
            GeneralFunction.getFragments().updateFragmentsFromLearner(globalState);
            GeneralIndexGenerator.getFragments().updateFragmentsFromLearner(globalState);
            GeneralTableGenerator.getFragments().updateFragmentsFromLearner(globalState);
        }

    }

    @Override
    public Reproducer<GeneralGlobalState> generateAndTestDatabaseWithMaskTemplateLearning(
            GeneralGlobalState globalState) throws Exception {
        String dbmsName = globalState.getDbmsSpecificOptions().getDatabaseEngineFactory().toString().toLowerCase();
        // TODO not sure whether diable should come before or after the learning
        globalState.getHandler().disableOptions(String.format("dbconfigs/%s/disabled_options.csv", dbmsName));
        globalState.getLearningManager().learnTypeByTopic(globalState);
        return super.generateAndTestDatabase(globalState);
    }

    @Override
    public boolean reproduceBugFromFile(GeneralGlobalState globalState) throws Exception {
        String bugFile = globalState.getOptions().getReproduceBugfile();
        if (bugFile.length() > 0) {
            // assume the bug is already a executable SQL file separated by ;
            // open the file and execute the queries
            Path path = Path.of(bugFile);
            if (!path.toFile().exists()) {
                throw new AssertionError("File not found: " + bugFile);
            }
            // List<String> lines = Files.readAllLines(path);
            StringBuilder sb = new StringBuilder();
            try (BufferedReader reader = Files.newBufferedReader(path)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    sb.append(line);
                }
            }
            String[] rawqueries = sb.toString().split(";");
            List<String> queries = new ArrayList<>();
            for (String query : rawqueries) {
                if (!query.isBlank()) {
                    queries.add(query);
                }
            }
            // List<String> queries =
            for (String query : queries) {
                try (Statement s = globalState.getConnection().createStatement()) {
                    s.execute(query);
                    globalState.getState().logStatement(query);
                } catch (SQLException t) {
                    System.err.println("Error: " + t.getMessage());
                    return false;
                }
            }
            return true;
        }
        return true;
        // return super.reproduceBugFromFile(globalState);
    }

}
