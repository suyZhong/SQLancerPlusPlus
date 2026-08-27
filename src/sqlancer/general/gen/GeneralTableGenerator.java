package sqlancer.general.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.general.GeneralErrorHandler.GeneratorNode;
import sqlancer.general.GeneralLearningManager.SQLFeature;
import sqlancer.general.GeneralProvider.GeneralGlobalState;
import sqlancer.general.GeneralSchema.GeneralColumn;
import sqlancer.general.GeneralSchema.GeneralCompositeDataType;
import sqlancer.general.GeneralSchema.GeneralTable;
import sqlancer.general.learner.GeneralFragments;
import sqlancer.general.learner.GeneralStringBuilder;

public final class GeneralTableGenerator {

    private static GeneralTableFragments fragments = new GeneralTableFragments();
    private static final String CONFIG_NAME = "tablegenerator.txt";
    private static final String STATEMENT = "CREATE_TABLE";
    private static final SQLFeature FEATURE = SQLFeature.CLAUSE;

    private GeneralTableGenerator() {
    }

    private static final class GeneralTableFragments extends GeneralFragments {
        GeneralTableFragments() {
            super();
        }

        @Override
        public synchronized String genLearnStatement(GeneralGlobalState globalState) {
            setLearn(true);
            String stmt = getQuery(globalState).getQueryString();
            setLearn(false);
            if (globalState.getOptions().debugLogs()) {
                System.out.println(stmt);
            }
            return stmt;
        }

        @Override
        public List<String> genValStatements(GeneralGlobalState globalState, String key, String choice,
                String databaseName) {
            List<String> queries = new ArrayList<>();
            String sketchTable;
            if (currentSketch != null) {
                sketchTable = currentSketch;
            } else {
                sketchTable = genLearnStatement(globalState);
            }
            Matcher matcher = Pattern.compile("\\{(\\d+)\\}").matcher(sketchTable);
            StringBuffer result = new StringBuffer();

            while (matcher.find()) {
                String index = matcher.group(1);
                String replacement = "";
                if (index.equals(key)) {
                    replacement = choice;
                }
                matcher.appendReplacement(result, replacement);
            }
            matcher.appendTail(result);
            queries.add(result.toString().replace("TEST_TABLE", databaseName));
            return queries;
        }

        @Override
        public String getConfigName() {
            return CONFIG_NAME;
        }

        @Override
        public String getStatementType() {
            return STATEMENT;
        }

        @Override
        public SQLFeature getFeature() {
            return FEATURE;
        }
    }

    public static SQLQueryAdapter getQuery(GeneralGlobalState globalState) {
        ExpectedErrors errors = new ExpectedErrors();
        String tableName;
        globalState.setCreatingDatabase(true);
        // TODO check if this is correct
        if (globalState.getHandler().getOption(GeneratorNode.CREATE_DATABASE)) {
            tableName = globalState.getSchema().getFreeTableName();
        } else {
            tableName = String.format("%s%s%s", globalState.getDatabaseName(),
                    globalState.getDbmsSpecificOptions().dbTableDelim, globalState.getSchema().getFreeTableName());
        }
        // if the current getQuery is for learning:
        if (fragments.getLearn()) {
            tableName = "TEST_TABLE";
        } else {
            globalState.setTestObject("TEST_TABLE", tableName);
        }
        GeneralStringBuilder<GeneralTableFragments> sb = new GeneralStringBuilder<>(globalState, fragments,
                globalState.getDbmsSpecificOptions().randomClauseFragments);
        sb.append("CREATE ", 0);
        sb.append(" TABLE ");
        sb.append(" ");
        sb.append(tableName);
        sb.append("(");
        List<GeneralColumn> columns = getNewColumns(globalState);
        // this is for rand column generation
        globalState.setUpdateTable(new GeneralTable(tableName, columns, false));
        for (int i = 0; i < columns.size(); i++) {
            globalState.setTestObject("TEST_COLUMN" + i, columns.get(i).getName());
        }
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            String colName = columns.get(i).getName();
            if (fragments.getLearn()) {
                colName = "TEST_COLUMN" + i;
            }
            sb.append(colName);
            sb.append(" ");
            sb.append(String.format("%s ", columns.get(i).getType()), 1);
        }
        List<GeneralColumn> columnsToAdd = new ArrayList<>();
        if (globalState.getDbmsSpecificOptions().testIndexes && !Randomly.getBooleanWithRatherLowProbability()) {
            List<GeneralColumn> primaryKeyColumns = Randomly
                    .nonEmptySubset(new ArrayList<>(columns.subList(0, columns.size() - 1)));
            globalState.getHandler().addScore(GeneratorNode.PRIMARY_KEY);
            sb.append(", PRIMARY KEY(");
            String pkCols = primaryKeyColumns.stream().map(c -> c.getName()).collect(Collectors.joining(", "));
            if (fragments.getLearn()) {
                pkCols = primaryKeyColumns.stream().map(c -> "TEST_COLUMN" + columns.indexOf(c))
                        .collect(Collectors.joining(", "));
            }
            sb.append(pkCols);
            sb.append(")", 2);
            // operate on the columns: if the column name is in primaryKeyColumns, then it
            // is a primary key
            for (GeneralColumn c : columns) {
                columnsToAdd.add(new GeneralColumn(c.getName(), c.getType(), primaryKeyColumns.contains(c), false));
            }
        } else {
            columnsToAdd = columns;
        }
        sb.append(")", 3);
        errors.addRegex(Pattern.compile(".*", Pattern.DOTALL));
        GeneralTable newTable = new GeneralTable(tableName, columnsToAdd, false);
        newTable.getColumns().forEach(c -> c.setTable(newTable));
        globalState.setUpdateTable(newTable);
        globalState.setCreatingDatabase(false);
        globalState.cleanTestObject();
        return new SQLQueryAdapter(sb.toString(), errors, true, false);
    }

    public static String getRandomCollate() {
        return Randomly.fromOptions("NOCASE", "NOACCENT", "NOACCENT.NOCASE", "C", "POSIX");
    }

    private static List<GeneralColumn> getNewColumns(GeneralGlobalState globalState) {
        List<GeneralColumn> columns = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber() + 2; i++) {
            String columnName = String.format("c%d", i);
            globalState.getHandler().addScore(GeneratorNode.COLUMN_NUM);
            GeneralCompositeDataType columnType = globalState.getLearningManager().getTopicType();
            if (Randomly.getBoolean() || columnType == null) {
                columnType = GeneralCompositeDataType.getRandomWithoutNull();
            }
            // TODO: make this as a feedback for the learner
            globalState.getHandler().addScore("COLUMN-" + columnType.toString());
            columns.add(new GeneralColumn(columnName, columnType, false, false));
        }
        return columns;
    }

    public static GeneralFragments getFragments() {
        return fragments;
    }

}
