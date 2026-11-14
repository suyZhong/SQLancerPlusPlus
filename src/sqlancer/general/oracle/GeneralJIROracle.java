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
 * <li>SEMI and ANTI: the rows of both joins are those of the left side of the join. The rows of each are those of the
 * left side for which a row of the right side exists, or does not exist, respectively.</li>
 * <li>NATURAL LEFT: the rows contain those of the natural inner join.</li>
 * <li>NATURAL FULL: the rows contain those of the natural inner, left, and right joins.</li>
 * </ul>
 *
 * Where the anti join is not the replaced join, it is expressed as a NOT EXISTS predicate over the right side of the
 * join, or as an anti join if the DBMS supports its syntax.
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
        private final List<String> firstQueryStrings;
        private final List<String> secondQueryStrings;
        private final boolean contained;
        private String errorMessage;

        GeneralJIRReproducer(List<String> firstQueryStrings, List<String> secondQueryStrings, boolean contained,
                String errorMessage) {
            this.firstQueryStrings = firstQueryStrings;
            this.secondQueryStrings = secondQueryStrings;
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
                compare(getResultSets(firstQueryStrings, globalState, false),
                        getResultSets(secondQueryStrings, globalState, false), firstQueryStrings, secondQueryStrings,
                        contained, globalState);
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
        case SEMI:
        case ANTI:
            checkSemiOrAntiJoin();
            break;
        case NATURAL:
            checkNaturalJoin();
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
        // The general generator does not produce semi and anti joins, so that one is requested here.
        JoinType semiOrAntiJoinType = getSemiOrAntiJoinType();
        Node<GeneralExpression> lastJoin = null;
        if (semiOrAntiJoinType != null) {
            lastJoin = generateJoin(tableList, semiOrAntiJoinType, "subj");
        }
        if (Randomly.getBoolean() || !allowSubquery) {
            joins = GeneralJoin.getJoins(tableList, state);
        } else {
            joins = GeneralJoin.getJoinsWithSubquery(tableList, state);
        }
        if (lastJoin != null) {
            joins.add(lastJoin);
        } else if (joins.isEmpty()) {
            // the oracle requires a join, which the general generator produces only occasionally
            joins.add(generateJoin(tableList, null, "sub0"));
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

    private JoinType getSemiOrAntiJoinType() {
        GeneralErrorHandler handler = state.getHandler();
        if (!handler.getOption(GeneratorNode.JOIN) || Randomly.getBoolean() || Randomly.getBoolean()) {
            return null;
        }
        List<JoinType> joinTypes = new ArrayList<>();
        if (handler.getOption(GeneratorNode.SEMI_JOIN)) {
            joinTypes.add(JoinType.SEMI);
        }
        if (handler.getOption(GeneratorNode.ANTI_JOIN)) {
            joinTypes.add(JoinType.ANTI);
        }
        return joinTypes.isEmpty() ? null : Randomly.fromList(joinTypes);
    }

    private Node<GeneralExpression> generateJoin(List<TableReferenceNode<GeneralExpression, GeneralTable>> tableList,
            JoinType joinType, String subqueryName) {
        GeneralErrorHandler handler = state.getHandler();
        if (!handler.getOption(GeneratorNode.JOIN)) {
            throw new IgnoreMeException();
        }
        boolean allowSubquery = handler.getOption(GeneratorNode.SUBQUERY);
        if (tableList.size() >= 2 && (Randomly.getBoolean() || !allowSubquery)) {
            return GeneralJoin.getJoin(tableList, joinType, state);
        } else if (allowSubquery) {
            List<GeneralTable> subqueryTables = List.of(Randomly.fromList(targetTables.getTables()));
            return GeneralJoin.getJoinWithSubquery(tableList, subqueryTables, subqueryName, joinType, state);
        }
        throw new IgnoreMeException();
    }

    private static boolean hasRelation(GeneralJoin candidate) {
        return candidate.getJoinType() != JoinType.NATURAL || candidate.getOuterType() == OuterType.LEFT
                || candidate.getOuterType() == OuterType.FULL;
    }

    private static boolean isSemiOrAnti(GeneralJoin candidate) {
        return candidate.getJoinType() == JoinType.SEMI || candidate.getJoinType() == JoinType.ANTI;
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
        if (isSemiOrAnti(join)) {
            // the right side of a semi or anti join is not visible
            columns.removeIf(this::isRightColumn);
        }
        fetchColumns = Randomly.nonEmptySubset(columns);
        return getColumnReferences(false);
    }

    private boolean isRightColumn(GeneralColumn column) {
        return column.getTable().getName().equals(getRightTable().getName());
    }

    /**
     * Gets the fetch columns of the query.
     *
     * @param hideRightSide
     *            whether the columns of the right side of the join are replaced by NULL, which is what a query fetches
     *            for the rows of the left side that have no match
     *
     * @return the fetch columns
     */
    private List<Node<GeneralExpression>> getColumnReferences(boolean hideRightSide) {
        List<Node<GeneralExpression>> columns = new ArrayList<>();
        for (GeneralColumn column : fetchColumns) {
            if (hideRightSide && isRightColumn(column)) {
                columns.add(GeneralConstant.createNullConstant());
            } else {
                columns.add(new ColumnReferenceNode<GeneralExpression, GeneralColumn>(column));
            }
        }
        return columns;
    }

    private void checkInnerJoin() throws SQLException {
        gen = GeneralRandomQuerySynthesizer.getExpressionGenerator(state, getJoinColumns());
        initializeTernaryPredicateVariants();
        List<String> queryStrings = new ArrayList<>();
        for (Node<GeneralExpression> partition : List.of(predicate, negatedPredicate, isNullPredicate)) {
            Node<GeneralExpression> onCondition = new NewBinaryOperatorNode<>(join.getOnCondition(), partition,
                    GeneralBinaryLogicalOperator.AND);
            queryStrings.add(getQueryString(false, join.withOnCondition(onCondition), null));
        }
        checkRelation(List.of(originalQueryString), combine(queryStrings), false);
    }

    private void checkLeftJoin() throws SQLException {
        GeneralErrorHandler handler = state.getHandler();
        boolean allowExists = handler.getOption(GeneratorNode.EXISTS);
        boolean allowAntiJoin = handler.getOption(GeneratorNode.ANTI_JOIN);
        if (!allowExists && !allowAntiJoin) {
            throw new IgnoreMeException();
        }
        List<String> queryStrings = new ArrayList<>();
        queryStrings.add(getQueryString(false, getJoinWithType(JoinType.INNER), null));
        if (allowAntiJoin && (Randomly.getBoolean() || !allowExists)) {
            queryStrings.add(getQueryString(true, getJoinWithType(JoinType.ANTI), null));
        } else {
            queryStrings.add(getExistsQueryString(true));
        }
        checkRelation(List.of(originalQueryString), combine(queryStrings), false);
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
            checkRelation(List.of(getExistsQueryString(true)), List.of(originalQueryString), true);
        }
    }

    private void checkSemiOrAntiJoin() throws SQLException {
        GeneralErrorHandler handler = state.getHandler();
        boolean semi = join.getJoinType() == JoinType.SEMI;
        JoinType complement = semi ? JoinType.ANTI : JoinType.SEMI;
        boolean allowExists = handler.getOption(GeneratorNode.EXISTS);
        boolean allowComplement = handler.getOption(GeneratorNode.valueOf(complement.name() + "_JOIN"));
        if (allowComplement && (Randomly.getBoolean() || !allowExists)) {
            List<String> queryStrings = new ArrayList<>();
            queryStrings.add(originalQueryString);
            queryStrings.add(getQueryString(false, getJoinWithType(complement), null));
            checkRelation(combine(queryStrings), List.of(getQueryString(false, join.getLeftTable(), null)), false);
        } else if (allowExists) {
            checkRelation(List.of(originalQueryString), List.of(getExistsQueryString(!semi)), false);
        } else {
            throw new IgnoreMeException();
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
        checkRelation(List.of(getQueryString(false, getJoinWithType(joinType), null)), List.of(originalQueryString),
                true);
    }

    private void checkContainsNaturalJoinType(OuterType outerType) throws SQLException {
        state.getHandler().addScore(GeneratorNode.NATURAL_JOIN);
        checkRelation(List.of(getQueryString(false, join.withJoinType(JoinType.NATURAL, outerType), null)),
                List.of(originalQueryString), true);
    }

    private GeneralJoin getJoinWithType(JoinType joinType) {
        state.getHandler().addScore(GeneratorNode.valueOf(joinType.name() + "_JOIN"));
        return join.withJoinType(joinType, null);
    }

    /**
     * Executes the queries of both sides and compares their rows.
     *
     * @param firstQueryStrings
     *            the queries of the first side, whose rows are concatenated
     * @param secondQueryStrings
     *            the queries of the second side, whose rows are concatenated
     * @param contained
     *            whether the rows of the first side are expected to be contained in those of the second side, rather
     *            than to be the same
     */
    private void checkRelation(List<String> firstQueryStrings, List<String> secondQueryStrings, boolean contained)
            throws SQLException {
        GeneralErrorHandler handler = state.getHandler();
        String derivedQueryString = firstQueryStrings.get(0).equals(originalQueryString) ? secondQueryStrings.get(0)
                : firstQueryStrings.get(0);
        List<String> firstResultSet;
        List<String> secondResultSet;
        try {
            firstResultSet = getResultSets(firstQueryStrings, state, true);
            secondResultSet = getResultSets(secondQueryStrings, state, true);
        } catch (Exception e) {
            handler.appendScoreToTable(false, true, derivedQueryString, e.getMessage());
            throw e;
        }
        handler.appendScoreToTable(true, true, derivedQueryString);
        try {
            compare(firstResultSet, secondResultSet, firstQueryStrings, secondQueryStrings, contained, state);
        } catch (AssertionError e) {
            reproducer = new GeneralJIRReproducer(firstQueryStrings, secondQueryStrings, contained, e.getMessage());
            throw e;
        }
    }

    private List<String> getResultSets(List<String> queryStrings, GeneralGlobalState globalState,
            boolean reuseOriginalResultSet) throws SQLException {
        List<String> resultSet = new ArrayList<>();
        for (String queryString : queryStrings) {
            if (reuseOriginalResultSet && queryString.equals(originalQueryString)) {
                resultSet.addAll(originalResultSet);
            } else {
                resultSet.addAll(ComparatorHelper.getResultSetFirstColumnAsString(queryString, errors, globalState));
            }
        }
        return resultSet;
    }

    private void compare(List<String> firstResultSet, List<String> secondResultSet, List<String> firstQueryStrings,
            List<String> secondQueryStrings, boolean contained, GeneralGlobalState globalState) {
        String firstQueryString = String.join(";", firstQueryStrings);
        if (contained) {
            ComparatorHelper.assumeResultSetIsContained(firstResultSet, secondResultSet, firstQueryString,
                    secondQueryStrings, globalState, ComparatorHelper::canonicalizeResultValue);
        } else {
            ComparatorHelper.assumeResultSetsAreEqual(firstResultSet, secondResultSet, firstQueryString,
                    secondQueryStrings, globalState, ComparatorHelper::canonicalizeResultValue);
        }
    }

    private List<String> combine(List<String> queryStrings) {
        if (Randomly.getBoolean()) {
            return List.of(String.join(" UNION ALL ", queryStrings));
        }
        return queryStrings;
    }

    /**
     * Gets a variant of the original query.
     *
     * @param hideRightSide
     *            whether the fetch columns of the right side of the join are replaced by NULL
     * @param replacement
     *            what takes the place of the join. The place is kept, so that the operands of the other joins are
     *            unchanged.
     * @param whereClause
     *            the WHERE clause of the variant, which can be null
     *
     * @return the variant
     */
    private String getQueryString(boolean hideRightSide, Node<GeneralExpression> replacement,
            Node<GeneralExpression> whereClause) {
        List<Node<GeneralExpression>> replacedJoins = new ArrayList<>(joins);
        replacedJoins.set(joinIndex, replacement);
        GeneralSelect variant = new GeneralSelect();
        variant.setFetchColumns(getColumnReferences(hideRightSide));
        variant.setFromList(fromList);
        variant.setJoinList(replacedJoins);
        variant.setWhereClause(whereClause);
        return GeneralToStringVisitor.asString(variant);
    }

    /**
     * Gets the query that fetches the rows of the left side of the join for which a matching row of the right side
     * exists, or for which none exists.
     *
     * @param negated
     *            whether the rows without a match are fetched, which are those of the anti join
     *
     * @return the query
     */
    private String getExistsQueryString(boolean negated) {
        state.getHandler().addScore(GeneratorNode.EXISTS);
        GeneralSelect subquery = new GeneralSelect();
        subquery.setFetchColumns(List.of(GeneralConstant.createIntConstant(1)));
        subquery.setFromList(List.of(join.getRightTable()));
        subquery.setWhereClause(join.getOnCondition());
        return getQueryString(true, join.getLeftTable(), new GeneralExists(subquery, negated));
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
