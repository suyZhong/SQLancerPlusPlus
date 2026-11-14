package sqlancer.general.ast;

import java.util.ArrayList;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.general.GeneralErrorHandler;
import sqlancer.general.GeneralErrorHandler.GeneratorNode;
import sqlancer.general.GeneralProvider.GeneralGlobalState;
import sqlancer.general.GeneralSchema.GeneralColumn;
import sqlancer.general.GeneralSchema.GeneralTable;
import sqlancer.general.GeneralSchema.GeneralTables;
import sqlancer.general.ast.GeneralSelect.GeneralSubquery;
import sqlancer.general.gen.GeneralRandomQuerySynthesizer;

public class GeneralJoin implements Node<GeneralExpression> {

    private final TableReferenceNode<GeneralExpression, GeneralTable> leftTable;
    private final Node<GeneralExpression> rightTable;
    private final JoinType joinType;
    private final Node<GeneralExpression> onCondition;
    private OuterType outerType;

    public enum JoinType {
        INNER, NATURAL, LEFT, RIGHT, FULL;

        private static final List<JoinType> VALUE_NOT_NJ = List.of(INNER, LEFT, RIGHT, FULL);

        public static JoinType getRandom() {
            return Randomly.fromOptions(values());
        }

        public static JoinType getRandomByOptions(GeneralErrorHandler handler) {
            // TODO refactor this function and also DBFunction one
            JoinType joinType;
            GeneratorNode node;
            do {
                joinType = getRandom();
                node = GeneratorNode.valueOf(joinType.name() + "_JOIN");
            } while (!handler.getOption(node) || !Randomly.getBooleanWithSmallProbability());
            handler.addScore(node);
            return joinType;
        }

        public static JoinType getRandomByOptionsWithoutNJ(GeneralErrorHandler handler) {
            JoinType joinType;
            GeneratorNode node;
            do {
                joinType = Randomly.fromList(VALUE_NOT_NJ);
                node = GeneratorNode.valueOf(joinType.name() + "_JOIN");
            } while (!handler.getOption(node) || !Randomly.getBoolean());
            handler.addScore(node);
            return joinType;
        }
    }

    public enum OuterType {
        FULL, LEFT, RIGHT;

        public static OuterType getRandom() {
            return Randomly.fromOptions(values());
        }

        public static OuterType getRandomByOptions(GeneralErrorHandler handler) {
            // TODO refactor this function and also DBFunction one
            OuterType outerType;
            GeneratorNode node;
            do {
                outerType = getRandom();
                node = GeneratorNode.valueOf(outerType.name() + "_NATURAL_JOIN");
            } while (!handler.getOption(node) || !Randomly.getBooleanWithSmallProbability());
            handler.addScore(node);
            return outerType;
        }
    }

    public GeneralJoin(TableReferenceNode<GeneralExpression, GeneralTable> leftTable,
            TableReferenceNode<GeneralExpression, GeneralTable> rightTable, JoinType joinType,
            Node<GeneralExpression> whereCondition) {
        this.leftTable = leftTable;
        this.rightTable = rightTable;
        this.joinType = joinType;
        this.onCondition = whereCondition;
    }

    public GeneralJoin(TableReferenceNode<GeneralExpression, GeneralTable> leftTable,
            Node<GeneralExpression> rightTable, JoinType joinType, Node<GeneralExpression> whereCondition) {
        this.leftTable = leftTable;
        this.rightTable = rightTable;
        this.joinType = joinType;
        this.onCondition = whereCondition;
    }

    public TableReferenceNode<GeneralExpression, GeneralTable> getLeftTable() {
        return leftTable;
    }

    public Node<GeneralExpression> getRightTable() {
        return rightTable;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public Node<GeneralExpression> getOnCondition() {
        return onCondition;
    }

    private void setOuterType(OuterType outerType) {
        this.outerType = outerType;
    }

    public OuterType getOuterType() {
        return outerType;
    }

    /**
     * Creates a copy of this join that differs only in its join type.
     *
     * @param newJoinType
     *            the join type of the copy
     * @param newOuterType
     *            the outer type of the copy, which is only meaningful for natural joins
     *
     * @return the copied join
     */
    public GeneralJoin withJoinType(JoinType newJoinType, OuterType newOuterType) {
        GeneralJoin join = new GeneralJoin(leftTable, rightTable, newJoinType, onCondition);
        join.setOuterType(newOuterType);
        return join;
    }

    /**
     * Creates a copy of this join that differs only in its ON condition.
     *
     * @param newOnCondition
     *            the ON condition of the copy
     *
     * @return the copied join
     */
    public GeneralJoin withOnCondition(Node<GeneralExpression> newOnCondition) {
        GeneralJoin join = new GeneralJoin(leftTable, rightTable, joinType, newOnCondition);
        join.setOuterType(outerType);
        return join;
    }

    public static List<Node<GeneralExpression>> getJoins(
            List<TableReferenceNode<GeneralExpression, GeneralTable>> tableList, GeneralGlobalState globalState) {
        List<Node<GeneralExpression>> joinExpressions = new ArrayList<>();
        GeneralErrorHandler handler = globalState.getHandler();
        while (tableList.size() >= 2 && Randomly.getBooleanWithRatherLowProbability()
                && handler.getOption(GeneratorNode.JOIN)) {
            joinExpressions.add(getJoin(tableList, globalState));
        }
        return joinExpressions;
    }

    /**
     * Joins the first two tables of the list, which are removed from it.
     *
     * @param tableList
     *            the tables that are not yet joined, of which there must be at least two
     * @param globalState
     *            the global state
     *
     * @return the join
     */
    public static Node<GeneralExpression> getJoin(List<TableReferenceNode<GeneralExpression, GeneralTable>> tableList,
            GeneralGlobalState globalState) {
        TableReferenceNode<GeneralExpression, GeneralTable> leftTable = tableList.remove(0);
        TableReferenceNode<GeneralExpression, GeneralTable> rightTable = tableList.remove(0);
        List<GeneralColumn> columns = new ArrayList<>(leftTable.getTable().getColumns());
        columns.addAll(rightTable.getTable().getColumns());
        ExpressionGenerator<Node<GeneralExpression>> joinGen = GeneralRandomQuerySynthesizer
                .getExpressionGenerator(globalState, columns);
        return getJoinExpression(leftTable, rightTable, joinGen, globalState);
    }

    /**
     * Joins the first table of the list, which is removed from it, with a subquery over the given tables.
     *
     * @param tableList
     *            the tables that are not yet joined, of which there must be at least one
     * @param subqueryTables
     *            the tables that the subquery selects from
     * @param subqueryName
     *            the alias of the subquery
     * @param globalState
     *            the global state
     *
     * @return the join
     */
    public static Node<GeneralExpression> getJoinWithSubquery(
            List<TableReferenceNode<GeneralExpression, GeneralTable>> tableList, List<GeneralTable> subqueryTables,
            String subqueryName, GeneralGlobalState globalState) {
        GeneralSubquery subquery = GeneralRandomQuerySynthesizer.generateSubquery(globalState, subqueryName,
                new GeneralTables(subqueryTables));
        TableReferenceNode<GeneralExpression, GeneralTable> leftTable = tableList.remove(0);
        List<GeneralColumn> columns = new ArrayList<>(leftTable.getTable().getColumns());
        columns.addAll(subquery.getTable().getColumns());
        Node<GeneralExpression> join = getJoinExpression(leftTable, subquery,
                GeneralRandomQuerySynthesizer.getExpressionGenerator(globalState, columns), globalState);
        globalState.getHandler().addScore(GeneratorNode.SUBQUERY);
        return join;
    }

    private static Node<GeneralExpression> getJoinExpression(
            TableReferenceNode<GeneralExpression, GeneralTable> leftTable, Node<GeneralExpression> rightTable,
            ExpressionGenerator<Node<GeneralExpression>> joinGen, GeneralGlobalState globalState) {
        Node<GeneralExpression> joinExpression = null;
        JoinType joinType;
        if (rightTable instanceof GeneralSubquery) {
            joinType = JoinType.getRandomByOptionsWithoutNJ(globalState.getHandler());
        } else {
            joinType = JoinType.getRandomByOptions(globalState.getHandler());
        }
        switch (joinType) {
        case INNER:
            joinExpression = GeneralJoin.createInnerJoin(leftTable, rightTable, joinGen.generateExpression());
            break;
        case NATURAL:
            joinExpression = GeneralJoin.createNaturalJoin(leftTable, rightTable,
                    OuterType.getRandomByOptions(globalState.getHandler()));
            break;
        case LEFT:
            joinExpression = GeneralJoin.createLeftOuterJoin(leftTable, rightTable, joinGen.generateExpression());
            break;
        case RIGHT:
            joinExpression = GeneralJoin.createRightOuterJoin(leftTable, rightTable, joinGen.generateExpression());
            break;
        case FULL:
            joinExpression = GeneralJoin.createFullOuterJoin(leftTable, rightTable, joinGen.generateExpression());
            break;
        default:
            throw new AssertionError();
        }
        return joinExpression;
    }

    public static List<Node<GeneralExpression>> getJoinsWithSubquery(
            List<TableReferenceNode<GeneralExpression, GeneralTable>> tableList, GeneralGlobalState globalState) {
        List<Node<GeneralExpression>> joinExpressions = new ArrayList<>();
        if (Randomly.getBoolean()) {
            joinExpressions = getJoins(tableList, globalState);
        }
        if (tableList.isEmpty() || Randomly.getBooleanWithRatherLowProbability()) {
            return joinExpressions;
        }
        List<GeneralTable> tables = new ArrayList<>();
        tables.add(Randomly.fromList(tableList).getTable());
        for (int i = 0; i < Randomly.smallNumber(); i++) {
            if (tableList.isEmpty()) {
                return joinExpressions;
            }
            joinExpressions.add(getJoinWithSubquery(tableList, tables, "sub" + i, globalState));
        }

        return joinExpressions;
    }

    public static GeneralJoin createRightOuterJoin(TableReferenceNode<GeneralExpression, GeneralTable> left,
            Node<GeneralExpression> right, Node<GeneralExpression> predicate) {
        return new GeneralJoin(left, right, JoinType.RIGHT, predicate);
    }

    public static GeneralJoin createFullOuterJoin(TableReferenceNode<GeneralExpression, GeneralTable> left,
            Node<GeneralExpression> right, Node<GeneralExpression> predicate) {
        return new GeneralJoin(left, right, JoinType.FULL, predicate);
    }

    public static GeneralJoin createLeftOuterJoin(TableReferenceNode<GeneralExpression, GeneralTable> left,
            Node<GeneralExpression> right, Node<GeneralExpression> predicate) {
        return new GeneralJoin(left, right, JoinType.LEFT, predicate);
    }

    public static GeneralJoin createInnerJoin(TableReferenceNode<GeneralExpression, GeneralTable> left,
            Node<GeneralExpression> right, Node<GeneralExpression> predicate) {
        return new GeneralJoin(left, right, JoinType.INNER, predicate);
    }

    public static Node<GeneralExpression> createNaturalJoin(TableReferenceNode<GeneralExpression, GeneralTable> left,
            Node<GeneralExpression> right, OuterType naturalJoinType) {
        GeneralJoin join = new GeneralJoin(left, right, JoinType.NATURAL, null);
        join.setOuterType(naturalJoinType);
        return join;
    }

}
