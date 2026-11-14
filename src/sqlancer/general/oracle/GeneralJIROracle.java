package sqlancer.general.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.ComparatorHelper;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.Reproducer;
import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.general.GeneralErrorHandler;
import sqlancer.general.GeneralErrorHandler.GeneratorNode;
import sqlancer.general.GeneralProvider.GeneralGlobalState;
import sqlancer.general.GeneralSchema.GeneralColumn;
import sqlancer.general.GeneralSchema.GeneralTable;
import sqlancer.general.GeneralToStringVisitor;
import sqlancer.general.ast.GeneralBinaryLogicalOperator;
import sqlancer.general.ast.GeneralConstant;
import sqlancer.general.ast.GeneralExists;
import sqlancer.general.ast.GeneralExpression;
import sqlancer.general.ast.GeneralJoin;
import sqlancer.general.ast.GeneralJoin.JoinType;
import sqlancer.general.ast.GeneralJoin.OuterType;
import sqlancer.general.ast.GeneralSelect;
import sqlancer.general.ast.GeneralSelect.GeneralSubquery;
import sqlancer.general.gen.GeneralRandomQuerySynthesizer;

/**
 * Checks relations that hold between a query with a join and variants of the query in which the join is replaced. The
 * relation depends on the type of the join that is replaced:
 *
 * <ul>
 * <li>INNER: partitioning the ON condition by a predicate, its negation, and its nullness yields the same rows.</li>
 * <li>LEFT: the rows are those of the inner join and those of the anti join.</li>
 * <li>RIGHT: the rows contain those of the inner join.</li>
 * <li>FULL: the rows contain those of the inner, left, right, and anti joins.</li>
 * <li>NATURAL LEFT: the rows contain those of the natural inner join.</li>
 * <li>NATURAL FULL: the rows contain those of the natural inner, left, and right joins.</li>
 * </ul>
 *
 * The anti join is expressed as a NOT EXISTS predicate over the right side of the join.
 */
public class GeneralJIROracle extends GeneralQueryPartitioningBase {

    private Reproducer<GeneralGlobalState> reproducer;
    private List<Node<GeneralExpression>> joins;
    private List<Node<GeneralExpression>> fromList;
    private List<GeneralColumn> fetchColumns;
    private int joinIndex;
    private GeneralJoin join;
    private String originalQueryString;
    private List<String> originalResultSet;

    public GeneralJIROracle(GeneralGlobalState state) {
        super(state);
    }

    private class GeneralJIRReproducer implements Reproducer<GeneralGlobalState> {
        private final List<String> queryStrings;
        private final boolean contained;
        private String errorMessage;

        GeneralJIRReproducer(List<String> queryStrings, boolean contained, String errorMessage) {
            this.queryStrings = queryStrings;
            this.contained = contained;
            this.errorMessage = errorMessage;
        }

        @Override
        public String getErrorMessage() {
            return errorMessage;
        }

        @Override
        public boolean bugStillTriggers(GeneralGlobalState globalState) {
            try {
                List<String> resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors,
                        globalState);
                compare(resultSet, getResultSets(queryStrings, globalState), queryStrings, contained, globalState);
            } catch (AssertionError triggeredError) {
                this.errorMessage = triggeredError.getMessage();
                return true;
            } catch (SQLException | IgnoreMeException ignored) {
                return false;
            }
            return false;
        }
    }

    @Override
    public void check() throws SQLException {
        reproducer = null;
        generateSelect();
        originalQueryString = GeneralToStringVisitor.asString(select);
        GeneralErrorHandler handler = state.getHandler();
        try {
            originalResultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);
        } catch (Exception e) {
            handler.appendScoreToTable(false, true, originalQueryString, e.getMessage());
            throw e;
        }
        // The features of the original query are recorded on their own, so that a failure of a derived query is
        // attributed only to the features that the derived query adds.
        handler.appendScoreToTable(true, true, originalQueryString);

        if (join.getJoinType() == JoinType.NATURAL) {
            checkNaturalJoin();
            return;
        }
        switch (join.getJoinType()) {
        case INNER:
            checkInnerJoin();
            break;
        case LEFT:
            checkLeftJoin();
            break;
        case RIGHT:
            checkContainsJoinType(JoinType.INNER);
            break;
        case FULL:
            checkFullJoin();
            break;
        default:
            throw new AssertionError(join.getJoinType());
        }
    }

    private void generateSelect() {
        s = state.getSchema();
        targetTables = s.getRandomTableNonEmptyTables();
        GeneralErrorHandler handler = state.getHandler();
        List<GeneralTable> tables = targetTables.getTables();
        List<TableReferenceNode<GeneralExpression, GeneralTable>> tableList = tables.stream()
                .map(t -> new TableReferenceNode<GeneralExpression, GeneralTable>(t)).collect(Collectors.toList());
        boolean allowSubquery = handler.getOption(GeneratorNode.SUBQUERY);
        if (Randomly.getBoolean() || !allowSubquery) {
            joins = GeneralJoin.getJoins(tableList, state);
        } else {
            joins = GeneralJoin.getJoinsWithSubquery(tableList, state);
        }
        if (joins.isEmpty()) {
            // the oracle requires a join, which the general generator produces only occasionally
            if (!handler.getOption(GeneratorNode.JOIN)) {
                throw new IgnoreMeException();
            }
            if (tableList.size() >= 2 && (Randomly.getBoolean() || !allowSubquery)) {
                joins.add(GeneralJoin.getJoin(tableList, state));
            } else if (allowSubquery) {
                List<GeneralTable> subqueryTables = List.of(Randomly.fromList(tables));
                joins.add(GeneralJoin.getJoinWithSubquery(tableList, subqueryTables, "sub0", state));
            } else {
                throw new IgnoreMeException();
            }
        }
        // The last join is the one that is replaced. Some DBMSs treat the comma as a join operator that is as binding
        // as the others, so that an outer join that follows the replaced join would have it as an operand, in which
        // case the relations do not hold.
        joinIndex = joins.size() - 1;
        join = (GeneralJoin) joins.get(joinIndex);
        if (!hasRelation(join)) {
            throw new IgnoreMeException();
        }
        fromList = tableList.stream().collect(Collectors.toList());

        select = new GeneralSelect();
        select.setFetchColumns(generateFetchColumns());
        select.setJoinList(joins);
        select.setFromList(fromList);
        select.setWhereClause(null);
    }

    private static boolean hasRelation(GeneralJoin candidate) {
        return candidate.getJoinType() != JoinType.NATURAL || candidate.getOuterType() == OuterType.LEFT
                || candidate.getOuterType() == OuterType.FULL;
    }

    @Override
    List<Node<GeneralExpression>> generateFetchColumns() {
        // The columns are listed explicitly, since the anti join replaces those of the right side by NULL.
        List<GeneralColumn> columns = new ArrayList<>(targetTables.getColumns());
        for (Node<GeneralExpression> j : joins) {
            Node<GeneralExpression> right = ((GeneralJoin) j).getRightTable();
            if (right instanceof GeneralSubquery) {
                columns.addAll(((GeneralSubquery) right).getTable().getColumns());
            }
        }
        fetchColumns = Randomly.nonEmptySubset(columns);
        return fetchColumns.stream().map(c -> new ColumnReferenceNode<GeneralExpression, GeneralColumn>(c))
                .collect(Collectors.toList());
    }

    private void checkInnerJoin() throws SQLException {
        gen = GeneralRandomQuerySynthesizer.getExpressionGenerator(state, getJoinColumns());
        initializeTernaryPredicateVariants();
        List<String> queryStrings = new ArrayList<>();
        for (Node<GeneralExpression> partition : List.of(predicate, negatedPredicate, isNullPredicate)) {
            Node<GeneralExpression> onCondition = new NewBinaryOperatorNode<>(join.getOnCondition(), partition,
                    GeneralBinaryLogicalOperator.AND);
            queryStrings.add(getQueryString(join.withOnCondition(onCondition)));
        }
        checkRelation(combine(queryStrings), false);
    }

    private void checkLeftJoin() throws SQLException {
        if (!state.getHandler().getOption(GeneratorNode.EXISTS)) {
            throw new IgnoreMeException();
        }
        List<String> queryStrings = new ArrayList<>();
        queryStrings.add(getQueryString(getJoinWithType(JoinType.INNER, null)));
        queryStrings.add(getAntiJoinQueryString());
        checkRelation(combine(queryStrings), false);
    }

    private void checkFullJoin() throws SQLException {
        checkContainsJoinType(JoinType.INNER);
        if (state.getHandler().getOption(GeneratorNode.LEFT_JOIN)) {
            checkContainsJoinType(JoinType.LEFT);
        }
        if (state.getHandler().getOption(GeneratorNode.RIGHT_JOIN)) {
            checkContainsJoinType(JoinType.RIGHT);
        }
        if (state.getHandler().getOption(GeneratorNode.EXISTS)) {
            checkRelation(List.of(getAntiJoinQueryString()), true);
        }
    }

    private void checkNaturalJoin() throws SQLException {
        checkContainsNaturalJoinType(null);
        if (join.getOuterType() == OuterType.FULL) {
            GeneralErrorHandler handler = state.getHandler();
            if (handler.getOption(GeneratorNode.LEFT_NATURAL_JOIN)) {
                handler.addScore(GeneratorNode.LEFT_NATURAL_JOIN);
                checkContainsNaturalJoinType(OuterType.LEFT);
            }
            if (handler.getOption(GeneratorNode.RIGHT_NATURAL_JOIN)) {
                handler.addScore(GeneratorNode.RIGHT_NATURAL_JOIN);
                checkContainsNaturalJoinType(OuterType.RIGHT);
            }
        }
    }

    private void checkContainsJoinType(JoinType joinType) throws SQLException {
        checkRelation(List.of(getQueryString(getJoinWithType(joinType, null))), true);
    }

    private void checkContainsNaturalJoinType(OuterType outerType) throws SQLException {
        state.getHandler().addScore(GeneratorNode.NATURAL_JOIN);
        checkRelation(List.of(getQueryString(join.withJoinType(JoinType.NATURAL, outerType))), true);
    }

    private GeneralJoin getJoinWithType(JoinType joinType, OuterType outerType) {
        state.getHandler().addScore(GeneratorNode.valueOf(joinType.name() + "_JOIN"));
        return join.withJoinType(joinType, outerType);
    }

    /**
     * Executes the derived queries and compares their rows with those of the original query.
     *
     * @param queryStrings
     *            the derived queries, whose rows are concatenated
     * @param contained
     *            whether the rows of the derived queries are expected to be contained in those of the original query,
     *            rather than to be the same
     */
    private void checkRelation(List<String> queryStrings, boolean contained) throws SQLException {
        GeneralErrorHandler handler = state.getHandler();
        String firstQueryString = queryStrings.get(0);
        List<String> resultSet;
        try {
            resultSet = getResultSets(queryStrings, state);
        } catch (Exception e) {
            handler.appendScoreToTable(false, true, firstQueryString, e.getMessage());
            throw e;
        }
        handler.appendScoreToTable(true, true, firstQueryString);
        try {
            compare(originalResultSet, resultSet, queryStrings, contained, state);
        } catch (AssertionError e) {
            reproducer = new GeneralJIRReproducer(queryStrings, contained, e.getMessage());
            throw e;
        }
    }

    private List<String> getResultSets(List<String> queryStrings, GeneralGlobalState globalState) throws SQLException {
        List<String> resultSet = new ArrayList<>();
        for (String queryString : queryStrings) {
            resultSet.addAll(ComparatorHelper.getResultSetFirstColumnAsString(queryString, errors, globalState));
        }
        return resultSet;
    }

    private void compare(List<String> originalRows, List<String> derivedRows, List<String> queryStrings,
            boolean contained, GeneralGlobalState globalState) {
        if (contained) {
            ComparatorHelper.assumeResultSetIsContained(derivedRows, originalRows, String.join(";", queryStrings),
                    List.of(originalQueryString), globalState, ComparatorHelper::canonicalizeResultValue);
        } else {
            ComparatorHelper.assumeResultSetsAreEqual(originalRows, derivedRows, originalQueryString, queryStrings,
                    globalState, ComparatorHelper::canonicalizeResultValue);
        }
    }

    private List<String> combine(List<String> queryStrings) {
        if (Randomly.getBoolean()) {
            return List.of(String.join(" UNION ALL ", queryStrings));
        }
        return queryStrings;
    }

    private String getQueryString(GeneralJoin replacement) {
        List<Node<GeneralExpression>> replacedJoins = new ArrayList<>(joins);
        replacedJoins.set(joinIndex, replacement);
        select.setJoinList(replacedJoins);
        String queryString = GeneralToStringVisitor.asString(select);
        select.setJoinList(joins);
        return queryString;
    }

    private String getAntiJoinQueryString() {
        state.getHandler().addScore(GeneratorNode.EXISTS);
        String rightName = getRightTable().getName();
        List<Node<GeneralExpression>> antiFetchColumns = new ArrayList<>();
        for (GeneralColumn column : fetchColumns) {
            if (column.getTable().getName().equals(rightName)) {
                antiFetchColumns.add(GeneralConstant.createNullConstant());
            } else {
                antiFetchColumns.add(new ColumnReferenceNode<GeneralExpression, GeneralColumn>(column));
            }
        }
        GeneralSelect subquery = new GeneralSelect();
        subquery.setFetchColumns(List.of(GeneralConstant.createIntConstant(1)));
        subquery.setFromList(List.of(join.getRightTable()));
        subquery.setWhereClause(join.getOnCondition());

        // the left table takes the place of the join, so that the operands of the other joins are unchanged
        List<Node<GeneralExpression>> antiJoins = new ArrayList<>(joins);
        antiJoins.set(joinIndex, join.getLeftTable());

        GeneralSelect antiSelect = new GeneralSelect();
        antiSelect.setFetchColumns(antiFetchColumns);
        antiSelect.setFromList(fromList);
        antiSelect.setJoinList(antiJoins);
        antiSelect.setWhereClause(new GeneralExists(subquery, true));
        return GeneralToStringVisitor.asString(antiSelect);
    }

    private GeneralTable getRightTable() {
        Node<GeneralExpression> right = join.getRightTable();
        if (right instanceof GeneralSubquery) {
            return ((GeneralSubquery) right).getTable();
        }
        return (GeneralTable) ((TableReferenceNode<?, ?>) right).getTable();
    }

    private List<GeneralColumn> getJoinColumns() {
        List<GeneralColumn> columns = new ArrayList<>(join.getLeftTable().getTable().getColumns());
        columns.addAll(getRightTable().getColumns());
        return columns;
    }

    @Override
    public Reproducer<GeneralGlobalState> getLastReproducer() {
        return reproducer;
    }

}
