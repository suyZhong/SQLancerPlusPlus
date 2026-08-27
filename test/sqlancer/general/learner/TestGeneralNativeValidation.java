package sqlancer.general.learner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

import sqlancer.MainOptions;
import sqlancer.general.GeneralLearningManager.SQLFeature;
import sqlancer.general.GeneralOptions;
import sqlancer.general.GeneralOptions.GeneralDatabaseEngineFactory;
import sqlancer.general.GeneralProvider.GeneralGlobalState;

public class TestGeneralNativeValidation {

    private static final class ValidationState extends GeneralGlobalState {
        private final boolean validationResult;
        private int validationCalls;

        private ValidationState(boolean validationResult) {
            this.validationResult = validationResult;
        }

        @Override
        public boolean checkIfQueriesAreValid(GeneralGlobalState globalState, List<String> queries,
                String databaseName) {
            validationCalls++;
            return validationResult;
        }
    }

    private static final class TestFragments extends GeneralFragments {

        @Override
        public String getConfigName() {
            return "test.txt";
        }

        @Override
        public String getStatementType() {
            return "TEST";
        }

        @Override
        public SQLFeature getFeature() {
            return SQLFeature.CLAUSE;
        }

        @Override
        public String genLearnStatement(GeneralGlobalState globalState) {
            return "";
        }

        @Override
        public List<String> genValStatements(GeneralGlobalState globalState, String key, String choice,
                String databaseName) {
            return List.of("SELECT " + choice);
        }
    }

    @Test
    public void testInvalidNewFragmentIsRemoved() {
        TestFragments fragments = new TestFragments();
        fragments.addFragment("0", "invalid", List.of());
        ValidationState state = new ValidationState(false);

        fragments.validateNewFragments(state);

        assertEquals(1, state.validationCalls);
        assertEquals(List.of(), fragments.getFragments().get("0"));
    }

    @Test
    public void testValidationOnlyCoversCurrentLearningBatch() {
        TestFragments fragments = new TestFragments();
        fragments.addFragment("0", "existing", List.of());
        fragments.beginLearningBatch();
        fragments.addFragment("0", "candidate", List.of());
        ValidationState state = new ValidationState(false);

        fragments.validateNewFragments(state);

        assertEquals(1, state.validationCalls);
        assertEquals(1, fragments.getFragments().get("0").size());
        assertEquals("existing", fragments.getFragments().get("0").get(0).getFragmentName());
    }

    @Test
    public void testSupportedNewFragmentIsRetained() {
        TestFragments fragments = new TestFragments();
        fragments.addFragment("0", "supported", List.of());
        ValidationState state = new ValidationState(true);

        fragments.validateNewFragments(state);

        assertEquals(1, state.validationCalls);
        assertEquals(1, fragments.getFragments().get("0").size());
    }

    @Test
    public void testValidationUsesTargetJdbcDriver() {
        GeneralGlobalState state = new GeneralGlobalState();
        GeneralOptions options = new GeneralOptions();
        options.databaseEngine = GeneralDatabaseEngineFactory.DUCKDB;
        state.setDbmsSpecificOptions(options);
        state.setMainOptions(new MainOptions());

        assertTrue(state.checkIfQueriesAreValid(state, List.of("SELECT 1"), "TEST_FEATURE"));
        assertFalse(state.checkIfQueriesAreValid(state, List.of("THIS IS NOT SQL"), "TEST_FEATURE"));
    }
}
