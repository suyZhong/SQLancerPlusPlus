package sqlancer.general.oracle;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Main;
import sqlancer.Randomly;
import sqlancer.Reproducer;
import sqlancer.SQLConnection;
import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.common.ast.newast.NewPostfixTextNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.common.oracle.NoRECBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.general.GeneralErrorHandler.GeneratorNode;
import sqlancer.general.GeneralErrors;
import sqlancer.general.GeneralProvider.GeneralGlobalState;
import sqlancer.general.GeneralSchema;
import sqlancer.general.GeneralSchema.GeneralColumn;
import sqlancer.general.GeneralSchema.GeneralTable;
import sqlancer.general.GeneralSchema.GeneralTables;
import sqlancer.general.GeneralToStringVisitor;
import sqlancer.general.ast.GeneralExpression;
import sqlancer.general.ast.GeneralJoin;
import sqlancer.general.ast.GeneralSelect;
import sqlancer.general.gen.GeneralExpressionGenerator;
import sqlancer.general.gen.GeneralTypedExpressionGenerator;

public class GeneralNoRECOracle extends NoRECBase<GeneralGlobalState> implements TestOracle<GeneralGlobalState> {

    private final GeneralSchema s;
    private Reproducer<GeneralGlobalState> reproducer;
    private GeneralSelect optimizedSelect;

    public GeneralNoRECOracle(GeneralGlobalState globalState) {
        super(globalState);
        this.s = globalState.getSchema();
        GeneralErrors.addExpressionErrors(errors);
    }

    private class GeneralNoRECReproducer implements Reproducer<GeneralGlobalState> {
        final String secondQueryString;
        final String firstQueryString;
        private String errorMessage;

        GeneralNoRECReproducer(String secondQueryString, String firstQueryString, String errorMessage) {
            this.secondQueryString = secondQueryString;
            this.firstQueryString = firstQueryString;
            this.errorMessage = errorMessage;
        }

        @Override
        public String getErrorMessage() {
            return errorMessage;
        }

        @Override
        public boolean bugStillTriggers(GeneralGlobalState globalState) {
            try {
                int secondCount = 0;
                SQLQueryAdapter q = new SQLQueryAdapter(secondQueryString, errors);
                SQLancerResultSet srs;
                try {
                    srs = q.executeAndGet(globalState);
                } catch (Exception e) {
                    this.errorMessage = e.getMessage();
                    return true;
                }
                if (srs == null) {
                    secondCount = -1;
                } else {
                    while (srs.next()) {
                        secondCount += srs.getBoolean(1) ? 1 : 0;
                    }
                    srs.close();
                }

                // first count
                int firstCount = -1;
                try (Statement stat = globalState.getConnection().createStatement()) {
                    try (ResultSet rs = stat.executeQuery(firstQueryString)) {
                        firstCount = 0;
                        while (rs.next()) {
                            firstCount++;
                        }
                    }
                } catch (SQLException e) {
                    // Query failed, treat as inconclusive
                    return false;
                }

                if (firstCount == -1 || secondCount == -1) {
                    return false;
                }
                if (firstCount != secondCount) {
                    this.errorMessage = firstQueryString + "; -- " + firstCount + "\n" + secondQueryString + " -- "
                            + secondCount;
                    return true;
                }
            } catch (SQLException ignored) {
            }
            return false;
        }
    }

    @Override
    public void check() throws SQLException {
        reproducer = null;
        GeneralTables randomTables = s.getRandomTableNonEmptyTables();
        List<GeneralColumn> columns = randomTables.getColumns();
        ExpressionGenerator<Node<GeneralExpression>> gen;
        if (state.getHandler().getOption(GeneratorNode.UNTYPE_EXPR) && Randomly.getBooleanWithSmallProbability()) {
            gen = new GeneralExpressionGenerator(state).setColumns(columns);
            state.getHandler().addScore(GeneratorNode.UNTYPE_EXPR);
        } else {
            gen = new GeneralTypedExpressionGenerator(state).setColumns(columns);
        }
        Node<GeneralExpression> randomWhereCondition = gen.generateExpression();
        List<GeneralTable> tables = randomTables.getTables();
        List<TableReferenceNode<GeneralExpression, GeneralTable>> tableList = tables.stream()
                .map(t -> new TableReferenceNode<GeneralExpression, GeneralTable>(t)).collect(Collectors.toList());
        List<Node<GeneralExpression>> joins = GeneralJoin.getJoins(tableList, state);

        int secondCount = getSecondQuery(tableList.stream().collect(Collectors.toList()), randomWhereCondition, joins);
        int firstCount = getFirstQueryCount(con, tableList.stream().collect(Collectors.toList()), columns,
                randomWhereCondition, joins);
        if (firstCount == -1 || secondCount == -1) {
            state.getHandler().appendScoreToTable(false, true);
            throw new IgnoreMeException();
        }
        if (firstCount != secondCount) {
            state.getHandler().appendScoreToTable(true, true, unoptimizedQueryString);
            String errorMessage = optimizedQueryString + "; -- " + firstCount + "\n" + unoptimizedQueryString + " -- "
                    + secondCount;
            reproducer = new GeneralNoRECReproducer(unoptimizedQueryString, optimizedQueryString, errorMessage);
            throw new AssertionError(errorMessage);
        }
        state.getHandler().appendScoreToTable(true, true, optimizedQueryString);
    }

    private int getSecondQuery(List<Node<GeneralExpression>> tableList, Node<GeneralExpression> randomWhereCondition,
            List<Node<GeneralExpression>> joins) throws SQLException {
        GeneralSelect select = new GeneralSelect();
        // select.setGroupByClause(groupBys);
        // GeneralExpression isTrue =
        // GeneralPostfixOperation.create(randomWhereCondition,
        // PostfixOperator.IS_TRUE);
        Node<GeneralExpression> asText = new NewPostfixTextNode<>(randomWhereCondition, " IS TRUE ");
        select.setFetchColumns(Arrays.asList(asText));
        select.setFromList(tableList);
        // select.setSelectType(SelectType.ALL);
        select.setJoinList(joins);
        int secondCount = 0;
        unoptimizedQueryString = GeneralToStringVisitor.asString(select);
        // errors.add("canceling statement due to statement timeout");
        SQLQueryAdapter q = new SQLQueryAdapter(unoptimizedQueryString, errors);
        SQLancerResultSet rs;
        try {
            rs = q.executeAndGetLogged(state);
        } catch (Exception e) {
            throw new AssertionError(unoptimizedQueryString, e);
        }
        if (rs == null) {
            return -1;
        }
        try {
            while (rs.next()) {
                secondCount += rs.getBoolean(1) ? 1 : 0;
            }
        } catch (Exception e) {
            rs.close();
            Main.nrUnsuccessfulActions.addAndGet(1);
            state.getHandler().appendScoreToTable(false, true, unoptimizedQueryString, e.getMessage());
            state.getLogger().writeCurrent("-- " + e.getMessage());
            throw new IgnoreMeException();
        }
        rs.close();
        return secondCount;
    }

    private int getFirstQueryCount(SQLConnection con, List<Node<GeneralExpression>> tableList,
            List<GeneralColumn> columns, Node<GeneralExpression> randomWhereCondition,
            List<Node<GeneralExpression>> joins) throws SQLException {
        optimizedSelect = new GeneralSelect();
        // select.setGroupByClause(groupBys);
        // GeneralAggregate aggr = new GeneralAggregate(
        List<Node<GeneralExpression>> allColumns = columns.stream()
                .map((c) -> new ColumnReferenceNode<GeneralExpression, GeneralColumn>(c)).collect(Collectors.toList());
        // GeneralAggregateFunction.COUNT);
        // select.setFetchColumns(Arrays.asList(aggr));
        optimizedSelect.setFetchColumns(allColumns);
        optimizedSelect.setFromList(tableList);
        optimizedSelect.setWhereClause(randomWhereCondition);
        // if (Randomly.getBooleanWithSmallProbability()) {
        // select.setOrderByExpressions(new
        // GeneralExpressionGenerator(state).setColumns(columns).generateOrderBys());
        // }
        // select.setSelectType(SelectType.ALL);
        optimizedSelect.setJoinList(joins);
        optimizedQueryString = GeneralToStringVisitor.asString(optimizedSelect);
        int firstCount = 0;
        try (Statement stat = con.createStatement()) {
            if (options.logEachSelect()) {
                logger.writeCurrent(optimizedQueryString);
            }
            try (ResultSet rs = stat.executeQuery(optimizedQueryString)) {
                while (rs.next()) {
                    firstCount++;
                }
            }
        } catch (SQLException e) {
            state.getHandler().appendScoreToTable(false, true, optimizedQueryString, e.getMessage());
            state.getLogger().writeCurrent(e.getMessage());
            throw new IgnoreMeException();
        }
        return firstCount;
    }

    @Override
    public Reproducer<GeneralGlobalState> getLastReproducer() {
        return reproducer;
    }

}
