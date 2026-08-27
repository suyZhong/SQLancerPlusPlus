package sqlancer.general.learner;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.gen.ExpressionGenerator;
import sqlancer.general.GeneralErrorHandler;
import sqlancer.general.GeneralLearningManager.SQLFeature;
import sqlancer.general.GeneralProvider.GeneralGlobalState;
import sqlancer.general.GeneralSchema.GeneralColumn;
import sqlancer.general.GeneralSchema.GeneralTable;
import sqlancer.general.GeneralToStringVisitor;
import sqlancer.general.ast.GeneralConstant;
import sqlancer.general.ast.GeneralExpression;
import sqlancer.general.gen.GeneralRandomQuerySynthesizer;

public abstract class GeneralFragments {

    private boolean learnFlag;
    public static final String PLACEHOLDER = "{%d}";
    private Map<String, List<GeneralFragmentChoice>> fragments = new HashMap<>();
    private final Map<String, List<GeneralFragmentChoice>> newFragments = new HashMap<>();
    protected String currentSketch = "";

    protected enum GeneralFragmentVariable {
        RANDOM_INT((g) -> {
            return GeneralConstant.createIntConstant(g.getRandomly().getInteger());
        }, "Get a random integer. e.g., 42"), RANDOM_STRING((g) -> {
            return GeneralConstant.createVartypeConstant(g.getRandomly().getString());
        }, "Get a random string without quotes. e.g., hello"), RANDOM_COLUMN((g) -> {
            if (g.getUpdateTable() != null) {
                return new ColumnReferenceNode<GeneralExpression, GeneralColumn>(g.getUpdateTable().getRandomColumn());
            } else {
                if (g.getSchema().getDatabaseTables().isEmpty()) {
                    return GeneralConstant.createVartypeConstant("c0");
                }
                GeneralTable table = g.getSchema().getRandomTable(t -> !t.isView());
                return new ColumnReferenceNode<GeneralExpression, GeneralColumn>(table.getRandomColumn());
            }
        }, "Random column"), RANDOM_TABLE((g) -> {
            if (g.getUpdateTable() != null) {
                return GeneralConstant.createVartypeConstant(g.getUpdateTable().getName());
            } else {
                if (g.getSchema().getDatabaseTables().isEmpty()) {
                    return GeneralConstant.createVartypeConstant("t0");
                }
                GeneralTable table = g.getSchema().getRandomTable(t -> !t.isView());
                return GeneralConstant.createVartypeConstant(table.getName());
            }
        }, "Random table"), RANDOM_EXPRESSION((g) -> {
            ExpressionGenerator<Node<GeneralExpression>> gen;
            List<GeneralColumn> columns;
            if (g.getUpdateTable() != null) {
                columns = g.getUpdateTable().getColumns();
                gen = GeneralRandomQuerySynthesizer.getExpressionGenerator(g, columns);
            } else {
                GeneralTable table = g.getSchema().getRandomTable();
                columns = table.getColumns();
                gen = GeneralRandomQuerySynthesizer.getExpressionGenerator(g, columns);
            }
            return gen.generateExpression();
        }, "Random expression based on current or random table"), RANDOM_POSITIVE_INT((g) -> {
            return GeneralConstant.createIntConstant(g.getRandomly().getPositiveIntegerInt());
        }, "Random positive integer"), RANDOM_DATE((g) -> {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            Timestamp timestamp = new Timestamp(g.getRandomly().getInteger());
            return GeneralConstant.createVartypeConstant(dateFormat.format(timestamp));
        }, "Get a random date. e.g., 2021-01-01"), RANDOM_TIMESTAMP((g) -> {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Timestamp timestamp = new Timestamp(g.getRandomly().getInteger());
            return GeneralConstant.createVartypeConstant(dateFormat.format(timestamp));
        }, "Get a random timestamp. e.g., 2021-01-01 00:00:00"), NULL((g) -> {
            return null;
        }) {
            @Override
            public String toString() {
                return "";
            }
        };

        private Node<GeneralExpression> node;
        private GeneralVariableGenerator<GeneralGlobalState> generator;
        private String description = "";

        GeneralFragmentVariable(GeneralVariableGenerator<GeneralGlobalState> generator) {
            this.generator = generator;
        }

        GeneralFragmentVariable(GeneralVariableGenerator<GeneralGlobalState> generator, String description) {
            this.generator = generator;
            this.description = description;
        }

        public void genVariable(GeneralGlobalState state) {
            node = generator.generate(state);
        }

        @Override
        public String toString() {
            return GeneralToStringVisitor.asString(node);
        }

        public String getDescription() {
            return description;
        }
    }

    public class GeneralFragmentChoice {

        private final String fmtString;
        private final List<GeneralFragmentVariable> vars;
        private final String key;

        public GeneralFragmentChoice(String fmtString, List<GeneralFragmentVariable> vars, String key) {
            this.fmtString = fmtString;
            this.vars = vars;
            this.key = key;
        }

        public String toString(GeneralGlobalState state) {
            for (GeneralFragmentVariable var : vars) {
                var.genVariable(state);
            }
            return String.format(fmtString, vars.stream().map(var -> var.toString()).toArray());
        }

        @Override
        public String toString() {
            return String.format("%s-%s-%s", getStatementType(), key, getFragmentName());
        }

        public String getType() {
            return getStatementType();
        }

        public String getKey() {
            return key;
        }

        public String getFragmentName() {
            return String.format(fmtString, vars.stream().map(var -> String.format("<%s>", var.name())).toArray());
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            GeneralFragmentChoice other = (GeneralFragmentChoice) obj;
            return fmtString.equals(other.fmtString) && vars.equals(other.vars) && key.equals(other.key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fmtString, vars, key);
        }

    }

    public GeneralFragments() {
        this.fragments = new HashMap<>();
    }

    public void setLearn(boolean learnFlag) {
        this.learnFlag = learnFlag;
    }

    public boolean getLearn() {
        return learnFlag;
    }

    public Map<String, List<GeneralFragmentChoice>> getFragments() {
        return fragments;
    }

    public void addFragment(String key, String fmtString, List<GeneralFragmentVariable> vars) {
        if (!fragments.containsKey(key)) {
            fragments.put(key, new ArrayList<>());
        }
        if (!newFragments.containsKey(key)) {
            newFragments.put(key, new ArrayList<>());
        }
        // remove trailing spaces
        String trimmedFmtString = fmtString.trim();
        // avoid duplicate:
        for (GeneralFragmentChoice choice : fragments.get(key)) {
            if (choice.fmtString.equals(trimmedFmtString)) {
                // System.out.println("Duplicate fragment");
                return;
            }
        }
        try {
            validateFragment(trimmedFmtString, vars);
        } catch (Exception e) {
            System.err.println(String.format("Invalid format string %s", trimmedFmtString));
            System.err.println(e.getMessage());
            throw e;
        }
        GeneralFragmentChoice choice = new GeneralFragmentChoice(trimmedFmtString, vars, key);
        // check if it's disabled by the handler
        if (!GeneralErrorHandler.checkFragmentAvailability(choice)) {
            // System.out.println(String.format("Fragment %s is disabled", fmtString));
            return;
        }
        // System.out.println(String.format("Adding fragment %s", fmtString));
        fragments.get(key).add(choice);
        newFragments.get(key).add(choice);
    }

    public String get(int index, GeneralGlobalState state) {
        String key = String.valueOf(index);
        if (learnFlag) {
            return getPlaceHolder(index);
        }
        if (fragments.containsKey(key)) {
            List<GeneralFragmentChoice> choices = fragments.get(key);
            if (choices.isEmpty()) {
                return "";
            }
            GeneralFragmentChoice choice = Randomly.fromList(choices);
            state.getHandler().addScore(choice);
            return choice.toString(state);
        } else {
            return "";
        }
    }

    public String getPlaceHolder(int index) {
        return String.format(PLACEHOLDER, index);
    }

    public void loadFragmentsFromFile(GeneralGlobalState globalState) {
        File configFile = new File(globalState.getConfigDirectory(), getConfigName());
        if (configFile.exists()) {
            // read from file
            FileReader fileReader;
            System.out.println(String.format("Loading fragments from file %s.", getConfigName()));
            try {
                fileReader = new FileReader(configFile);
                loadFragmentsFromCSV(fileReader, globalState, false);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            System.err.println(String.format("File %s does not exist", getConfigName()));
        }
        // printFragments();
    }

    protected String preprocessingLine(String line) {
        return line;
    }

    protected void loadFragmentsFromCSV(Reader configReader, GeneralGlobalState globalState, boolean isSpecific) {
        // get file lines by the reader
        try (BufferedReader reader = new BufferedReader(configReader)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.contains(";")) {
                    continue;
                }
                line = preprocessingLine(line);
                String fragmentKey = line.substring(0, line.indexOf(';'));
                String fragmentContent = line.substring(line.indexOf(';') + 1);
                String[] s = { fragmentKey, fragmentContent };
                try {
                    if (isSpecific) {
                        parseSpecificFragments(s, globalState);
                    } else {
                        parseFragments(s);
                    }
                } catch (Exception e) {
                    // System.out.println(String.format("Error parsing %s for statement %s", String.join(" ", s),
                    // getStatementType()));
                    // System.err.println(e.getMessage());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected void validateFragment(String fmtString, List<GeneralFragmentVariable> vars) {
        // Validate that the format string is compatible with the variables; throws IllegalFormatException if invalid
        String formatted = String.format(fmtString, vars.stream().map(var -> var.name()).toArray());
        if (formatted == null) {
            throw new AssertionError("Unexpected null result from String.format");
        }
    }

    protected void parseSpecificFragments(String[] s, GeneralGlobalState globalState) {
        parseFragments(s);
    }

    protected void parseFragments(String... s) {
        // assume all the rows are in the format "integer index, <content>"

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

        addFragment(key, fmtString.toString(), vars);

    }

    protected String parseVariable(String s, List<GeneralFragmentVariable> vars) {
        StringBuffer fmtString = new StringBuffer();
        Pattern pattern = Pattern.compile("<([^>]*)>");
        Matcher matcher = pattern.matcher(s);

        String content = "";

        while (matcher.find()) {
            content = matcher.group(1);
            matcher.appendReplacement(fmtString, "%s");
            vars.add(GeneralFragmentVariable.valueOf(content.toUpperCase()));
        }
        matcher.appendTail(fmtString);

        return fmtString.toString();
    }

    public synchronized void updateFragmentByFeedback(GeneralErrorHandler handler) {
        // Iterate the fragments and remove the ones that are not useful
        for (String key : fragments.keySet()) {
            List<GeneralFragmentChoice> choices = fragments.get(key);
            choices.removeIf(choice -> !handler.getFragmentOption(choice));
        }
    }

    public void beginLearningBatch() {
        newFragments.clear();
    }

    public void updateFragmentsFromLearner(GeneralGlobalState globalState) {
        beginLearningBatch();
        String template = genLearnStatement(globalState);
        currentSketch = template;
        String variables = getVariables();
        String systemPrompt = getSystemPrompt();
        String examples = getExamples();
        String topic = getFeature().isSubFeature() ? getStatementType() : "overview";
        GeneralTemplateLearner learner = new GeneralTemplateLearner(globalState, getFeature(), template, variables,
                systemPrompt, topic);
        learner.setExamples(examples);
        System.out.println("Updating fragments from learner");
        learner.learn();
        System.out.println("Processing and loading fragments from learner");
        String fragments = learner.getFragments();
        if (fragments != "") {
            loadFragmentsFromCSV(new StringReader(fragments), globalState, false);
        } else {
            System.err.println("No fragments returned from learner");
        }
        if (globalState.getDbmsSpecificOptions().enableDirectValidation) {
            validateNewFragments(globalState);
        }
        // printFragments();
    }

    public void validateNewFragments(GeneralGlobalState globalState) {
        for (String key : newFragments.keySet()) {
            List<GeneralFragmentChoice> choices = newFragments.get(key);
            List<GeneralFragmentChoice> toRemove = new ArrayList<>();
            for (GeneralFragmentChoice choice : choices) {
                // TODO here we should check if the fragment is valid
                String databaseName = "TEST_FEATURE";
                String concreteFragment = "";
                try {
                    concreteFragment = choice.toString(globalState);
                } catch (Exception e) {
                    System.out.println(String.format("No need to test %s", choice.toString()));
                    // toRemove.add(choice);
                    continue;
                }
                List<String> valStatements = genValStatements(globalState, key, concreteFragment, databaseName);
                if (valStatements.isEmpty()) {
                    System.out.println(String.format("No need to test %s", choice.toString()));
                    // remove the fragment
                    // choices.remove(choice);
                    continue;
                }
                if (globalState.checkIfQueriesAreValid(globalState, valStatements, databaseName)) {
                    // TODO here we should check if the fragment is valid
                    System.out.println(String.format("Fragment %s is supported", choice.toString()));
                    // print the statements
                    for (String stmt : valStatements) {
                        System.out.println(String.format("%s", stmt));
                    }
                } else {
                    System.out.println(String.format("Fragment %s is invalid", choice.toString()));
                    for (String stmt : valStatements) {
                        System.out.println(String.format("%s", stmt));
                    }
                    // remove the fragment
                    toRemove.add(choice);
                }
            }
            for (GeneralFragmentChoice choice : toRemove) {
                boolean removed = fragments.get(key).remove(choice);
                if (removed) {
                    System.out.println(String.format("Removed fragment %s", choice.toString()));
                } else {
                    System.out.println(String.format("Fragment %s not found", choice.toString()));
                }
            }
        }
    }

    protected String getSystemPrompt() {
        return "This GPT is an expert in SQL dialects. It helps users generate correct SQL statements for different DBMSs based on the reference user provided. Users specify a DBMS and provide a SQL sketch with SQL keywords and holes. The GPT fills holes with concrete string alternatives unless the user specifies variables. The response is a CSV file with two columns separated by semicolon \";\": one for holes (without brackets) and one for alternatives, without a header. Each alternative is split into separate rows. Provide as many and detailed answers as possible for each placeholder. Be rare and complex. Avoid explanations. Avoid random functions in DBMS. Function calls should be deterministic. Avoid commands writing to OS. ";
    }

    public void printFragments() {
        System.out.println(String.format("Fragments for %s", getStatementType()));
        for (String key : fragments.keySet()) {
            System.out.println(String.format("Fragment %s", key));
            for (GeneralFragmentChoice choice : fragments.get(key)) {
                System.out.println(choice.toString());
            }
        }
    }

    public void dumpFragments(GeneralGlobalState globalState) {
        if (globalState.getOptions().debugLogs()) {
            System.out.println(String.format("Dumping fragments for %s", getStatementType()));
        }
        File dir = globalState.getLogger().getLearnerFileDir();
        try (FileWriter writer = new FileWriter(
                new File(dir, String.format("%s-%s-config.txt", globalState.getDatabaseName(), getStatementType())))) {
            for (String key : fragments.keySet()) {
                for (GeneralFragmentChoice choice : fragments.get(key)) {
                    writer.write(String.format("%s,%s\n", key, choice.getFragmentName()));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected String getVariables() {
        StringBuilder sb = new StringBuilder();
        for (GeneralFragmentVariable var : GeneralFragmentVariable.values()) {
            if (var == GeneralFragmentVariable.NULL) {
                continue;
            }
            sb.append(String.format("<%s>: %s\n", var.name(), var.getDescription()));
        }
        sb.append("Note: Please DO NOT include other variables or identifiers.\n");
        return sb.toString();
    }

    // TODO: load the examples from the configs
    protected String getExamples() {
        return "";
    }

    public abstract String getConfigName();

    public abstract String getStatementType();

    public abstract SQLFeature getFeature();

    public abstract String genLearnStatement(GeneralGlobalState globalState);

    public abstract List<String> genValStatements(GeneralGlobalState globalState, String key, String choice,
            String databaseName);

    public void learnSpecificTopicFromLearner(GeneralGlobalState globalState, String topic) {
        throw new UnsupportedOperationException();
    };

}
