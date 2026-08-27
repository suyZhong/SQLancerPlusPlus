package sqlancer.general;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.general.GeneralProvider.GeneralGlobalState;
import sqlancer.general.GeneralSchema.GeneralCompositeDataType;
import sqlancer.general.ast.GeneralBinaryOperator;
import sqlancer.general.ast.GeneralFunction;
import sqlancer.general.learner.GeneralFragments;

public class GeneralLearningManager {

    private static final GeneralLearningScheduler LEARNING_SCHEDULER = new GeneralLearningScheduler();

    private String curTopic;
    private int learnCount;
    // if True, then the topic is learned
    private static Map<String, String> topics = new HashMap<>();
    private static volatile Map<String, Boolean> topicPool = new HashMap<>();

    public enum SQLFeature {
        COMMAND("command", true), //
        DATATYPE("datatype", false), //
        FUNCTION("function", false), //
        CLAUSE("clause", true), //
        OPERATOR("operator", false);

        private final String name;
        private final boolean subFeature;

        SQLFeature(String name, boolean subFeature) {
            this.name = name;
            this.subFeature = subFeature;
        }

        @Override
        public String toString() {
            return name;
        }

        // TODO: change the variable name
        public boolean isSubFeature() {
            return subFeature;
        }
    }

    public GeneralLearningManager() {
    }

    public String getTopic() {
        return curTopic;
    }

    public GeneralCompositeDataType getTopicType() {
        return GeneralCompositeDataType.getByName(curTopic);
    }

    public void setTopic(String databaseName, String topic) {
        topics.put(databaseName, topic);
        topicPool.put(topic, true);
    }

    private void setCurTopic(GeneralGlobalState globalState) {
        curTopic = topics.get(globalState.getDatabaseName());
    }

    public int getLearnCount() {
        return learnCount;
    }

    public Map<String, Boolean> getTopicPool() {
        return topicPool;
    }

    private synchronized void initializeTopicPool(GeneralFragments fragments) {
        if (topicPool.isEmpty()) {
            for (String topic : fragments.getFragments().keySet()) {
                topicPool.put(topic, false);
            }
        } else {
            for (String topic : fragments.getFragments().keySet()) {
                topicPool.putIfAbsent(topic, false);
            }
            topicPool.replaceAll((topic, learned) -> false);
        }
    }

    public boolean tryAcquireLearningSlot(GeneralGlobalState globalState) {
        GeneralOptions options = globalState.getDbmsSpecificOptions();
        String databaseEngine = options.getDatabaseEngineFactory().toString();
        return LEARNING_SCHEDULER.tryAcquire(databaseEngine, options.learningIntervalSeconds);
    }

    public void learnTypeByTopic(GeneralGlobalState globalState) {
        GeneralFragments fragments = GeneralSchema.getFragments(); // TODO: change it to feature
        // update topicPool HashMap
        if (topicPool.isEmpty()) {
            initializeTopicPool(fragments);
        }

        if (globalState.getOptions().debugLogs()) {
            System.out.println("Topic pool: " + topicPool);
        }
        // System.out.println(topicPool);
        // update the fragments if fragments is empty
        if (fragments.getFragments().isEmpty()) {
            fragments.updateFragmentsFromLearner(globalState);
            for (String topic : fragments.getFragments().keySet()) {
                if (!topicPool.containsKey(topic)) {
                    topicPool.put(topic, false);
                    GeneralSchema.setTypeAvailability(topic, false);
                }
            }
        }
        // randomly pick a topic to learn
        if (Randomly.getBooleanWithRatherLowProbability() || globalState.getHandler().getExecDatabaseNum() == 0) {
            // pick one topic that is in the topicPool with false value
            List<String> topics = topicPool.entrySet().stream().filter(entry -> !entry.getValue())
                    .map(Map.Entry::getKey).collect(Collectors.toList());
            if (topics.isEmpty()) {
                curTopic = null;
                System.out.println("All topics are learned. Refresh");
                initializeTopicPool(fragments);
                return;
            }
            String topic = Randomly.fromList(topics);
            if (fragments.getFragments().get(topic).size() != 0) {
                GeneralSchema.setTypeAvailability(topic, true);
            }
            setTopic(globalState.getDatabaseName(), topic);
            curTopic = topic;
            // globalState.getHandler().setCompositeOption(topic, true);
            // learn the topic
            fragments.learnSpecificTopicFromLearner(globalState, topic);
            GeneralFunction.loadFunctionsFromFragments(globalState);
            GeneralBinaryOperator.loadOperatorsFromFragments(globalState);
            // System.out.println(GeneralFunction.getFuncNames());
            // System.out.println(GeneralBinaryOperator.getOperators());
            // globalState.getHandler().setCurDepth(globalState.getDatabaseName(), 2);
            // topicPool.put(topic, true);
        }

        setCurTopic(globalState);
        if (globalState.getOptions().debugLogs()) {
            System.out.println("Current topic for " + globalState.getDatabaseName() + " is: " + curTopic);
        }
        // System.out.println("Current topic for " + globalState.getDatabaseName() + "is: " + curTopic);

    }

}
