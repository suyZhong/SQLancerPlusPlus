package sqlancer.general.oracle;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Main;
import sqlancer.Randomly;
import sqlancer.Reproducer;
import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.oracle.TestOracle;
import sqlancer.general.GeneralErrorHandler.GeneratorNode;
import sqlancer.general.GeneralProvider.GeneralGlobalState;
import sqlancer.general.GeneralSchema;
import sqlancer.general.GeneralSchema.GeneralColumn;
import sqlancer.general.GeneralSchema.GeneralTable;
import sqlancer.general.GeneralSchema.GeneralTables;
import sqlancer.general.GeneralToStringVisitor;
import sqlancer.general.ast.GeneralExpression;
import sqlancer.general.ast.GeneralJoin;
import sqlancer.general.ast.GeneralSelect;
import sqlancer.general.gen.GeneralRandomQuerySynthesizer;

/**
 * Fuzzing oracle: executes SELECT queries with random predicates and reports bugs when the DBMS
 * raises an internal/unexpected error or loses its connection (indicating a crash). Result
 * correctness is not checked.
 *
 * The triggering SELECT is logged to the oracle's local state so it appears in the reduced bug
 * report. The reproducer uses the last statement in the candidate state, which lets the
 * StatementReducer correctly determine that the SELECT cannot be removed.
 */
public class GeneralFuzzingOracle implements TestOracle<GeneralGlobalState> {

    private static final String[] INTERNAL_ERROR_KEYWORDS = { "internal error", "unexpected error", "panic",
            "assertion failed", "assertion error", "fatal error", "segmentation fault", "stack overflow",
            "null pointer", "out of memory", "aborted", "corrupted" };

    private final GeneralGlobalState state;
    private Reproducer<GeneralGlobalState> reproducer;

    public GeneralFuzzingOracle(GeneralGlobalState state) {
        this.state = state;
    }

    private class GeneralFuzzingReproducer implements Reproducer<GeneralGlobalState> {
        private String errorMessage;

        GeneralFuzzingReproducer(String errorMessage) {
            this.errorMessage = errorMessage;
        }

        @Override
        public String getErrorMessage() {
            return errorMessage;
        }

        /**
         * Re-runs the last statement from the candidate state. Returning {@code false} when the SELECT
         * has been removed tells the reducer that the SELECT cannot be dropped from the bug report.
         */
        @Override
        public boolean bugStillTriggers(GeneralGlobalState globalState) {
            List<? extends sqlancer.common.query.Query<?>> stmts = globalState.getState().getStatements();
            if (stmts.isEmpty()) {
                return false;
            }
            // Oracle SELECT is always the last logged statement.
            // If the reducer removed it, the last stmt will be DDL → not a SELECT → bug can't trigger.
            String oracleQuery = stmts.get(stmts.size() - 1).getQueryString().trim();
            if (!oracleQuery.toUpperCase().startsWith("SELECT")) {
                return false;
            }
            try (Statement stmt = globalState.getConnection().createStatement();
                    ResultSet rs = stmt.executeQuery(oracleQuery)) {
                while (rs.next()) {
                }
                return false;
            } catch (SQLException e) {
                if (isInternalOrCrashError(e)) {
                    this.errorMessage = e.getMessage();
                    return true;
                }
                return false;
            } catch (Exception e) {
                // Non-SQL exception (connection reset etc.) also signals a crash
                this.errorMessage = e.getMessage();
                return true;
            }
        }
    }

    @Override
    public void check() throws SQLException {
        reproducer = null;

        GeneralSchema schema = state.getSchema();
        GeneralTables targetTables = schema.getRandomTableNonEmptyTables();
        List<GeneralColumn> columns = targetTables.getColumns();

        ExpressionGenerator<Node<GeneralExpression>> gen = GeneralRandomQuerySynthesizer.getExpressionGenerator(state,
                columns);
        Node<GeneralExpression> whereClause = gen.generateExpression();

        List<GeneralTable> tables = targetTables.getTables();
        List<TableReferenceNode<GeneralExpression, GeneralTable>> tableList = tables.stream()
                .map(t -> new TableReferenceNode<GeneralExpression, GeneralTable>(t)).collect(Collectors.toList());
        List<Node<GeneralExpression>> joins;
        if (Randomly.getBoolean() || !state.getHandler().getOption(GeneratorNode.SUBQUERY)) {
            joins = GeneralJoin.getJoins(tableList, state);
        } else {
            joins = GeneralJoin.getJoinsWithSubquery(tableList, state);
        }

        GeneralSelect select = new GeneralSelect();
        List<Node<GeneralExpression>> fetchColumns = new ArrayList<>();
        if (Randomly.getBoolean()) {
            fetchColumns.add(new ColumnReferenceNode<>(new GeneralColumn("*", null, false, false)));
        } else {
            fetchColumns = Randomly.nonEmptySubset(columns).stream()
                    .map(c -> new ColumnReferenceNode<GeneralExpression, GeneralColumn>(c))
                    .collect(Collectors.toList());
        }
        select.setFetchColumns(fetchColumns);
        select.setFromList(tableList.stream().collect(Collectors.toList()));
        select.setJoinList(joins.stream().collect(Collectors.toList()));
        select.setWhereClause(whereClause);

        String queryString = GeneralToStringVisitor.asString(select);

        // Log to the current oracle-run local state.
        // OracleRunReproductionState.close() flushes this to the main state only on the bug path
        // (i.e. when success=false), so it won't pollute successful runs.
        state.getState().getLocalState().log(queryString);

        try (Statement stmt = state.getConnection().createStatement()) {
            if (state.getOptions().logEachSelect()) {
                state.getLogger().writeCurrent(queryString);
            }
            try (ResultSet rs = stmt.executeQuery(queryString)) {
                while (rs.next()) {
                }
            }
            Main.nrSuccessfulActions.addAndGet(1);
            state.getHandler().appendScoreToTable(true, true, queryString);
        } catch (SQLException e) {
            Main.nrUnsuccessfulActions.addAndGet(1);
            if (isInternalOrCrashError(e)) {
                state.getHandler().appendScoreToTable(true, true, queryString);
                String errorMessage = "Internal/unexpected DBMS error.\nQuery: " + queryString + "\nError: "
                        + e.getMessage();
                reproducer = new GeneralFuzzingReproducer(errorMessage);
                throw new AssertionError(errorMessage);
            }
            state.getHandler().appendScoreToTable(false, true, queryString, e.getMessage());
            state.getLogger().writeCurrent("-- " + e.getMessage());
            throw new IgnoreMeException();
        }
    }

    /**
     * Returns true when the exception likely indicates a server-side crash or internal error rather
     * than a normal SQL error that the DBMS rejected gracefully.
     */
    static boolean isInternalOrCrashError(SQLException e) {
        // SQLState class "08" covers all connection exceptions (crash / connection loss)
        String sqlState = e.getSQLState();
        if (sqlState != null && sqlState.startsWith("08")) {
            return true;
        }
        String msg = e.getMessage();
        if (msg != null) {
            String lower = msg.toLowerCase();
            for (String keyword : INTERNAL_ERROR_KEYWORDS) {
                if (lower.contains(keyword)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public Reproducer<GeneralGlobalState> getLastReproducer() {
        return reproducer;
    }
}
