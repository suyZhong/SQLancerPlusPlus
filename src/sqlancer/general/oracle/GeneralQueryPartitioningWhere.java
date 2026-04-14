package sqlancer.general.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.Randomly;
import sqlancer.Reproducer;
import sqlancer.general.GeneralErrors;
import sqlancer.general.GeneralProvider.GeneralGlobalState;
import sqlancer.general.GeneralToStringVisitor;

public class GeneralQueryPartitioningWhere extends GeneralQueryPartitioningBase {
    private Reproducer<GeneralGlobalState> reproducer;

    public GeneralQueryPartitioningWhere(GeneralGlobalState state) {
        super(state);
        GeneralErrors.addExpressionErrors(errors);
    }

    private class GeneralQueryPartitioningWhereReproducer implements Reproducer<GeneralGlobalState> {
        final String firstQueryString;
        final String secondQueryString;
        final String thirdQueryString;
        final String originalQueryString;
        final boolean orderBy;
        private String errorMessage;

        GeneralQueryPartitioningWhereReproducer(String firstQueryString, String secondQueryString,
                String thirdQueryString, String originalQueryString, boolean orderBy, String errorMessage) {
            this.firstQueryString = firstQueryString;
            this.secondQueryString = secondQueryString;
            this.thirdQueryString = thirdQueryString;
            this.originalQueryString = originalQueryString;
            this.orderBy = orderBy;
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
                List<String> combinedString1 = new ArrayList<>();
                List<String> secondResultSet1 = ComparatorHelper.getCombinedResultSet(firstQueryString,
                        secondQueryString, thirdQueryString, combinedString1, !orderBy, globalState, errors);
                ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet1, originalQueryString,
                        combinedString1, globalState, ComparatorHelper::canonicalizeResultValue);
            } catch (AssertionError triggeredError) {
                this.errorMessage = triggeredError.getMessage();
                return true;
            } catch (SQLException ignored) {
            }
            return false;
        }
    }

    @Override
    public void check() throws SQLException {
        reproducer = null;
        super.check();
        select.setWhereClause(null);
        String originalQueryString = GeneralToStringVisitor.asString(select);
        List<String> resultSet;
        try {
            resultSet = ComparatorHelper.getResultSetFirstColumnAsString(originalQueryString, errors, state);
        } catch (Exception e) {
            if (select.getJoinList().size() == 0 && select.getFromList().size() <= 2) {
                e.printStackTrace();
                throw new AssertionError(e.getMessage()
                        + "\n You probably triggered an error in the DBMS by the previous query, as the query is a simple select that could not easily have issue. Check the *-cur.log");
            }
            // Noticed that, we would still add some extra information to the generator table. Since the UNION ALL query
            // would not be actually executed but fail due to the previous JOIN query.
            // I think it is fine. We could do dependency analysis later.
            state.getHandler().appendScoreToTable(false, true, originalQueryString, e.getMessage());
            throw e;
        }

        boolean orderBy = Randomly.getBooleanWithRatherLowProbability();
        if (orderBy) {
            select.setOrderByExpressions(gen.generateOrderBys());
        }
        select.setWhereClause(predicate);
        String firstQueryString = GeneralToStringVisitor.asString(select);
        select.setWhereClause(negatedPredicate);
        String secondQueryString = GeneralToStringVisitor.asString(select);
        select.setWhereClause(isNullPredicate);
        String thirdQueryString = GeneralToStringVisitor.asString(select);
        List<String> combinedString = new ArrayList<>();

        List<String> secondResultSet;
        try {
            secondResultSet = ComparatorHelper.getCombinedResultSet(firstQueryString, secondQueryString,
                    thirdQueryString, combinedString, !orderBy, state, errors);
        } catch (Exception e) {
            state.getHandler().appendScoreToTable(false, true, firstQueryString, e.getMessage());
            throw e;
        }
        try {
            ComparatorHelper.assumeResultSetsAreEqual(resultSet, secondResultSet, originalQueryString, combinedString,
                    state, ComparatorHelper::canonicalizeResultValue);
        } catch (AssertionError e) {
            // TODO we need to give some information to the handler here
            // state.getHandler().printStatistics();
            state.getHandler().appendScoreToTable(true, true, firstQueryString);
            reproducer = new GeneralQueryPartitioningWhereReproducer(firstQueryString, secondQueryString,
                    thirdQueryString, originalQueryString, orderBy, e.getMessage());
            throw e;
        }
        state.getHandler().appendScoreToTable(true, true, firstQueryString);
    }

    @Override
    public Reproducer<GeneralGlobalState> getLastReproducer() {
        return reproducer;
    }

}
