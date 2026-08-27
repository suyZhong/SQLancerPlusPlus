package sqlancer.general;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import sqlancer.Randomly;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.schema.AbstractRelationalTable;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractTables;
import sqlancer.common.schema.TableIndex;
import sqlancer.general.GeneralLearningManager.SQLFeature;
import sqlancer.general.GeneralProvider.GeneralGlobalState;
import sqlancer.general.GeneralSchema.GeneralTable;
import sqlancer.general.ast.GeneralBinaryOperator;
import sqlancer.general.ast.GeneralFunction;
import sqlancer.general.learner.GeneralFragments;
import sqlancer.general.learner.GeneralStringBuilder;
import sqlancer.general.learner.GeneralTemplateLearner;

public class GeneralSchema extends AbstractSchema<GeneralGlobalState, GeneralTable> {
    private static GeneralTypeFragments fragments = new GeneralTypeFragments();
    private static final String CONFIG_NAME = "typegenerator.txt";
    private static final String STATEMENT = "DATATYPE";
    private static final SQLFeature FEATURE = SQLFeature.DATATYPE;

    private static int typeCounter;
    private static volatile Map<Integer, String> typeMap = new HashMap<>();
    private static Map<String, Boolean> typeAvailabilityMap = new HashMap<>();
    private static Map<String, List<String>> typeToFunction = new HashMap<>();

    private static final class GeneralTypeFragments extends GeneralFragments {
        GeneralTypeFragments() {
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
            queries.add(String.format("CREATE TABLE %s (c0 %s);", databaseName, key));
            queries.add(String.format("INSERT INTO %s VALUES (%s);", databaseName, choice));
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

        @Override
        // rare keywords
        // use options
        protected String getSystemPrompt() {
            return "This GPT is an expert in SQL dialects. It helps users generate correct SQL statements for different DBMSs based on the reference user provided. Users specify a DBMS, provide a SQL template with SQL keywords and holes, and give random variable generators. The GPT fills holes with data types ({0}) and the format of the data types ({1}), consists of concrete strings or random variable generators user provided. The response is a CSV file with two columns separated by semicolon \";\": one for data type names and one for the format, without a header. Provide at least 3 example values or syntax for one data type. Each data type is split into separate rows. Provide as many different answers. Be rare and complex. Avoid explanations. Avoid random functions in DBMS. Avoid functions let the server sleep.";
        }

        @Override
        protected String getExamples() {
            return "INT,<RANDOM_INT>\n" + "VARCHAR,'<RANDOM_STRING>'\n" + "BOOLEAN,TRUE\n" + "DATE,'<RANDOM_DATE>'\n"
                    + "DATE,'2021-01-01'\n";
        }

        @Override
        protected void parseFragments(String... s) {
            String key = s[0];

            Pattern pattern = Pattern.compile("<([^>]*)>");
            Matcher matcher = pattern.matcher(s[1]);

            String content = "";
            StringBuffer output = new StringBuffer();

            List<GeneralFragmentVariable> vars = new ArrayList<>();

            while (matcher.find()) {
                content = matcher.group(1);
                matcher.appendReplacement(output, "%s");
                vars.add(GeneralFragmentVariable.valueOf(content.toUpperCase()));
            }
            matcher.appendTail(output);

            addFragment(key, output.toString(), vars);

            // if key is not in the typeMap, add it
            if (!typeMap.containsValue(key)) {
                typeMap.put(typeCounter, key);
                // TODO change uppercase
                updateTypeToFunction(key.toUpperCase(), new ArrayList<>(), false);
                typeAvailabilityMap.put(key, true);
                typeCounter++;
            }
        }

        @Override
        public String get(int index, GeneralGlobalState state) {
            if (getLearn()) {
                return super.get(index, state);
            }
            String key = typeMap.get(index);
            return get(key, state);
        }

        public String get(String key, GeneralGlobalState state) {
            // actually, if typeMap contains the key, then fragments must contain the key
            if (getFragments().containsKey(key) && typeAvailabilityMap.get(key)) {
                GeneralFragmentChoice choice = Randomly.fromList(getFragments().get(key));
                if (state.getCreatingDatabase()) {
                    // only consider feedback when creating the database
                    // otherwise any constant generated will be considered
                    // can't match data format with type
                    state.getHandler().addScore(choice);
                }
                return choice.toString(state);
            } else {
                return "NULL";
            }
        }

        @Override
        public synchronized void updateFragmentByFeedback(GeneralErrorHandler handler) {
            super.updateFragmentByFeedback(handler);
            // iterate over the fragments, if the choices is empty
            // then typeAvailabilityMap will be updated
            for (String key : getFragments().keySet()) {
                if (getFragments().get(key).isEmpty()) {
                    typeAvailabilityMap.put(key, false);
                } else {
                    typeAvailabilityMap.put(key, true);
                }
            }

        }

        @Override
        public void learnSpecificTopicFromLearner(GeneralGlobalState globalState, String type) {
            GeneralFragments typeFragments = GeneralSchema.getFragments();
            GeneralFragments functionFragments = GeneralFunction.getFragments();
            GeneralFragments operatorFragments = GeneralBinaryOperator.getFragments();
            typeFragments.beginLearningBatch();
            functionFragments.beginLearningBatch();
            operatorFragments.beginLearningBatch();

            StringBuilder templateBuilder = new StringBuilder();
            templateBuilder.append(String.format("CREATE TABLE TEST_TABLE (COL %s);\n", type));
            templateBuilder
                    .append(String.format("INSERT INTO TEST_TABLE VALUES (%s);\n", fragments.get(type, globalState)));
            templateBuilder.append(String.format(
                    "INSERT INTO TEST_TABLE VALUES ({0}); -- Placeholder {0}: %s value or expression to insert\n",
                    type));
            templateBuilder.append(String.format(
                    "INSERT INTO TEST_TABLE VALUES ({1}()); -- Placeholder {1}: Deterministic function that returns a %s value with no parameters\n",
                    type));
            templateBuilder.append(String.format(
                    "INSERT INTO TEST_TABLE VALUES ({2}(NULL)); -- Placeholder {2}: Deterministic function with one parameter that returns a %s value\n",
                    type));
            templateBuilder.append(String.format(
                    "INSERT INTO TEST_TABLE VALUES ({3}(NULL, NULL)); -- Placeholder {3}: Deterministic function with two parameters that returns a %s value\n",
                    type));
            templateBuilder.append(String.format(
                    "INSERT INTO TEST_TABLE VALUES (NULL {4} NULL); -- Placeholder {4}: Deterministic operator whose return value is %s\n",
                    type));
            templateBuilder.append(String.format(
                    "SELECT * FROM TEST_TABLE WHERE COL {5} COL; -- Placeholder {5}: Deterministic operator whose operands are %s value\n",
                    type));
            String template = templateBuilder.toString();
            String variables = getVariables();
            // get the prompt of the general fragments but not the schema one
            String systemPrompt = super.getSystemPrompt();
            GeneralTemplateLearner learner = new GeneralTemplateLearner(globalState, FEATURE, template, variables,
                    systemPrompt, String.format("%s function", type));
            StringBuilder exampleBuilder = new StringBuilder();
            exampleBuilder.append(String.format("0,%s\n", fragments.get(type, globalState)));
            exampleBuilder.append(String.format("1,EXAMPLE_FUNCTION_NAME\n"));
            exampleBuilder.append(String.format("2,EXAMPLE_FUNCTION_NAME\n"));
            exampleBuilder.append(String.format("3,EXAMPLE_FUNCTION_NAME\n"));
            exampleBuilder.append(String.format("4,EXAMPLE_OPERATOR\n"));
            String examples = exampleBuilder.toString();
            learner.setExamples(examples);
            System.out.println("Updating fragments from learner for type " + type);
            learner.learn();
            System.out.println("Processing and loading fragments from learner for type " + type);
            String fragmentsString = learner.getFragments();

            if (fragmentsString.isEmpty()) {
                System.out.println("No fragments learned for type " + type);
                return;
            }

            loadFragmentsFromCSV(new StringReader(fragmentsString), globalState, true);
            if (globalState.getDbmsSpecificOptions().enableDirectValidation) {
                typeFragments.validateNewFragments(globalState);
                functionFragments.validateNewFragments(globalState);
                operatorFragments.validateNewFragments(globalState);
            }

        }

        @Override
        protected void parseSpecificFragments(String[] s, GeneralGlobalState globalState) {
            // TODO Auto-generated method stub
            String type = globalState.getLearningManager().getTopic();
            try {
                Integer.parseInt(s[0]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid fragment key");
                return;
            }
            String key = s[0];

            StringBuffer fmtString = new StringBuffer();
            List<GeneralFragmentVariable> vars = new ArrayList<>();
            Pattern pattern = Pattern.compile("<([^>]*)>");
            Matcher matcher = pattern.matcher(s[1]);

            String content = "";

            while (matcher.find()) {
                content = matcher.group(1);
                matcher.appendReplacement(fmtString, "%s");
                vars.add(GeneralFragmentVariable.valueOf(content.toUpperCase()));
            }
            matcher.appendTail(fmtString);
            GeneralFragments typeFragments = GeneralSchema.getFragments();
            GeneralFragments funcFragments = GeneralFunction.getFragments();
            GeneralFragments opFragments = GeneralBinaryOperator.getFragments();
            if (!key.equals("0") && !globalState.getDbmsSpecificOptions().enableExpressionLearning) {
                return;
            }

            switch (key) {
            case "0":
                typeFragments.addFragment(type, fmtString.toString(), vars);
                break;
            case "1":
                funcFragments.addFragment("0", fmtString.toString().replaceAll("^\"+|\"+$", ""), vars);
                // typeToFunction.get(type).add(fmtString.toString())
                try {
                    updateTypeToFunction(type, new ArrayList<>(List.of(fmtString.toString())), false);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                break;
            case "2":
                // add more cases here
                funcFragments.addFragment("1", fmtString.toString().replaceAll("^\"+|\"+$", ""), vars);
                try {
                    updateTypeToFunction(type, new ArrayList<>(List.of(fmtString.toString())), false);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                break;
            case "3":
                funcFragments.addFragment("2", fmtString.toString().replaceAll("^\"+|\"+$", ""), vars);
                break;
            case "4":
                opFragments.addFragment("BOOLEAN", fmtString.toString().replaceAll("^\"+|\"+$", ""), vars);
                break;
            case "5":
                opFragments.addFragment(type, fmtString.toString().replaceAll("^\"+|\"+$", ""), vars);
                break;
            default:
                break;
            }
        }

    }

    public static SQLQueryAdapter getQuery(GeneralGlobalState globalState) {
        GeneralStringBuilder<GeneralTypeFragments> sb = new GeneralStringBuilder<>(globalState, fragments);
        sb.append("CREATE TABLE test (c0 ", 0);
        sb.append(");\n");

        sb.append("INSERT INTO test VALUES (", 1);
        sb.append(")");

        return new SQLQueryAdapter(sb.toString());
    }

    public static List<String> getAvailFunctions(String type) {
        return typeToFunction.get(type);
    }

    public static void updateTypeToFunction(String type, List<String> functions, boolean overwrite) {
        if (!typeToFunction.containsKey(type) || overwrite) {
            typeToFunction.put(type, functions);
        } else {
            typeToFunction.get(type).addAll(functions);
        }
    }

    public enum GeneralDataType {

        // INT, VARCHAR, BOOLEAN, FLOAT, DATE, TIMESTAMP, NULL;
        INT, NULL, BOOLEAN, STRING, VARTYPE;

        public static GeneralDataType[] weightTypes = {};

        public static GeneralDataType getRandomWithoutNull() {
            GeneralDataType dt;
            do {
                dt = Randomly.fromOptions(values());
            } while (dt == GeneralDataType.NULL);
            return dt;
        }

        public static void calcWeight() {
            // iterate values to add them into an array, and update the weightTypes array
            // with the new values
            List<GeneralDataType> types = new ArrayList<>();
            for (GeneralDataType dt : GeneralDataType.values()) {
                if (dt == GeneralDataType.NULL) {
                    continue;
                }
                if (dt == GeneralDataType.VARTYPE) {
                    for (int i = 0; i < typeCounter; i++) {
                        types.add(dt);
                    }
                    continue;
                }
                types.add(dt);
            }
            weightTypes = types.toArray(new GeneralDataType[0]);
        }

        public static GeneralDataType getRandomWithProb() {
            return Randomly.fromOptions(weightTypes);
        }

        public GeneralCompositeDataType get() {
            return new GeneralCompositeDataType(this, 0);
        }

    }

    public static class GeneralCompositeDataType {

        private final GeneralDataType dataType;

        private final int id;

        public GeneralCompositeDataType(GeneralDataType dataType, int id) {
            this.dataType = dataType;
            this.id = id;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof GeneralCompositeDataType) {
                GeneralCompositeDataType other = (GeneralCompositeDataType) obj;
                return dataType == other.dataType && id == other.id;
            }
            return false;
        }

        @Override
        public int hashCode() {
            return Objects.hash(dataType, id);
        }

        public GeneralDataType getPrimitiveDataType() {
            return dataType;
        }

        public int getId() {
            if (id == -1) {
                throw new AssertionError(this);
            }
            return id;
        }

        public static GeneralCompositeDataType getByName(String type) {
            // match the name of the type to the typeMap
            if (type == null) {
                return null;
            }
            if (type.equals("BOOLEAN")) {
                return GeneralDataType.BOOLEAN.get();
            } else if (type.equals("VARCHAR")) {
                return GeneralDataType.STRING.get();
            } else if (type.equals("INT")) {
                return GeneralDataType.INT.get();
            }
            int firstKey = -1;
            for (int key : typeMap.keySet()) {
                if (typeMap.get(key).equals(type)) {
                    firstKey = key;
                    return new GeneralCompositeDataType(GeneralDataType.VARTYPE, firstKey);
                }
            }
            System.err.println("Type not found");
            return null;
        }

        public static List<GeneralCompositeDataType> getSupportedTypes() {
            List<GeneralCompositeDataType> types = new ArrayList<>();
            for (GeneralDataType dt : GeneralDataType.values()) {
                if (dt == GeneralDataType.NULL) {
                    continue;
                }
                if (dt == GeneralDataType.VARTYPE) {
                    for (int i = 0; i < typeCounter; i++) {
                        types.add(new GeneralCompositeDataType(dt, i));
                    }
                    continue;
                }
                types.add(new GeneralCompositeDataType(dt, 0));
            }
            return types;
        }

        public static GeneralCompositeDataType getRandomWithoutNull(GeneralGlobalState globalState) {
            String topic = globalState.getLearningManager().getTopic();
            if (Randomly.getBooleanWithRatherLowProbability()) {
                GeneralCompositeDataType type = getByName(topic);
                if (type != null) {
                    return type;
                }
            }
            return getRandomWithoutNull();
        }

        public static GeneralCompositeDataType getRandomWithoutNull() {
            GeneralDataType type = GeneralDataType.getRandomWithProb();
            int typeID = -1;
            switch (type) {
            case INT:
                typeID = Randomly.fromOptions(1, 2, 4, 8);
                break;
            // case FLOAT:
            // size = Randomly.fromOptions(4, 8);
            // break;
            case BOOLEAN:
            case STRING:
                // case DATE:
                // case TIMESTAMP:
                if (Randomly.getBoolean()) {
                    typeID = 500; // As MySQL Generator here is 500
                } else {
                    typeID = 0;
                }
                break;
            case VARTYPE:
                // pick a random type id from the typeMap
                // TODO an exception here
                typeID = Randomly.fromList(List.copyOf(typeMap.keySet()));
                break;
            default:
                throw new AssertionError(type);
            }

            return new GeneralCompositeDataType(type, typeID);
        }

        @Override
        public String toString() {
            switch (getPrimitiveDataType()) {
            case INT:
                return "INT";
            case STRING:
                if (id == 0) {
                    return "VARCHAR";
                } else {
                    return "VARCHAR(" + id + ")";
                }
            case BOOLEAN:
                return "BOOLEAN";
            case NULL:
                return "NULL";
            case VARTYPE:
                // TODO catch exception here
                return typeMap.get(id).toUpperCase();
            default:
                throw new AssertionError(getPrimitiveDataType());
            }
        }

    }

    public static void setTypeAvailability(String type, boolean availability) {
        typeAvailabilityMap.put(type, availability);
    }

    public static class GeneralColumn extends AbstractTableColumn<GeneralTable, GeneralCompositeDataType> {

        private final boolean isPrimaryKey;
        private final boolean isNullable;

        public GeneralColumn(String name, GeneralCompositeDataType columnType, boolean isPrimaryKey,
                boolean isNullable) {
            super(name, null, columnType);
            this.isPrimaryKey = isPrimaryKey;
            this.isNullable = isNullable;
        }

        public boolean isPrimaryKey() {
            return isPrimaryKey;
        }

        public boolean isNullable() {
            return isNullable;
        }

    }

    public static class GeneralTables extends AbstractTables<GeneralTable, GeneralColumn> {

        public GeneralTables(List<GeneralTable> tables) {
            super(tables);
        }

    }

    public GeneralSchema(List<GeneralTable> databaseTables) {
        super(databaseTables);
    }

    public GeneralTables getRandomTableNonEmptyTables() {
        return new GeneralTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }

    public static class GeneralTable extends AbstractRelationalTable<GeneralColumn, TableIndex, GeneralGlobalState> {

        public GeneralTable(String tableName, List<GeneralColumn> columns, boolean isView) {
            super(tableName, columns, Collections.emptyList(), isView);
        }

        public GeneralTable(String tableName, List<GeneralColumn> columns, List<TableIndex> indexes, boolean isView) {
            super(tableName, columns, indexes, isView);
        }

    }

    @Override
    public String getFreeTableName() {
        // TODO Auto-generated method stub
        int i = 0;
        if (Randomly.getBooleanWithRatherLowProbability()) {
            i = (int) Randomly.getNotCachedInteger(0, 10);
        }
        do {
            String tableName = String.format("t%d", i++);
            if (super.getDatabaseTables().stream().noneMatch(t -> t.getName().endsWith(tableName))) {
                return tableName;
            }
        } while (true);
    }

    public void printTables() {
        for (GeneralTable t : getDatabaseTables()) {
            System.out.println(t.getName() + " -------");
            for (GeneralColumn c : t.getColumns()) {
                System.out.println(c.getName() + " " + c.getType());
            }
        }
    }

    public static GeneralFragments getFragments() {
        return fragments;
    }
}
