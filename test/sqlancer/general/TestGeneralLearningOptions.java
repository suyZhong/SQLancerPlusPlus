package sqlancer.general;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import sqlancer.general.GeneralLearningManager.SQLFeature;

public class TestGeneralLearningOptions {

    @Test
    public void testAllLearningKindsEnabledByDefault() {
        GeneralOptions options = new GeneralOptions();

        for (SQLFeature feature : SQLFeature.values()) {
            assertTrue(options.isLearningEnabled(feature), feature.toString());
        }
    }

    @Test
    public void testLearningKindsCanBeDisabledIndependently() {
        GeneralOptions options = new GeneralOptions();
        options.enableStatementLearning = false;
        options.enableDatatypeLearning = false;
        options.enableExpressionLearning = false;
        options.enableClauseLearning = false;

        assertFalse(options.isLearningEnabled(SQLFeature.COMMAND));
        assertFalse(options.isLearningEnabled(SQLFeature.DATATYPE));
        assertFalse(options.isLearningEnabled(SQLFeature.FUNCTION));
        assertFalse(options.isLearningEnabled(SQLFeature.OPERATOR));
        assertFalse(options.isLearningEnabled(SQLFeature.CLAUSE));
    }
}
