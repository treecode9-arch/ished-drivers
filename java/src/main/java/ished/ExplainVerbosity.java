package ished;

/** As the driver's ExplainVerbosity: how much explain() tells (the plan only, or what running it examined). */
public enum ExplainVerbosity {
    QUERY_PLANNER("queryPlanner"),
    EXECUTION_STATS("executionStats"),
    ALL_PLANS_EXECUTION("allPlansExecution");

    private final String value;

    ExplainVerbosity(String value) { this.value = value; }

    String value() { return value; }
}
