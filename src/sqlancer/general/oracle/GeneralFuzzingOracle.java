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
        private final String queryString;
        private String errorMessage;

        GeneralFuzzingReproducer(String queryString, String errorMessage) {
            this.queryString = queryString;
            this.errorMessage = errorMessage;
        }

        @Override
        public String getErrorMessage() {
            return errorMessage;
        }

        @Override
        public boolean bugStillTriggers(GeneralGlobalState globalState) {
            try (Statement stmt = globalState.getConnection().createStatement();
                    ResultSet rs = stmt.executeQuery(queryString)) {
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
                String errorMessage = "Internal/unexpected DBMS error triggered by fuzzing query.\n" + "Query: "
                        + queryString + "\nError: " + e.getMessage();
                reproducer = new GeneralFuzzingReproducer(queryString, errorMessage);
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
    private static boolean isInternalOrCrashError(SQLException e) {
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
