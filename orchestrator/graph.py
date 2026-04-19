from langgraph.graph import StateGraph, END
from orchestrator.state import PipelineDebugState
from agents.log_analysis_agent import run_log_analysis_agent
from agents.schema_validation_agent import run_schema_validation_agent
from agents.data_quality_agent import run_data_quality_agent
from agents.root_cause_agent import run_root_cause_agent
from agents.fix_recommendation_agent import run_fix_recommendation_agent


# ─── Node Functions ───────────────────────────────────────────────────────────

def log_analysis_node(state: PipelineDebugState) -> PipelineDebugState:
    print("  [1/5] Running Log Analysis Agent...")
    result = run_log_analysis_agent(state["incident"])
    state["log_analysis"] = result
    return state


def routing_node(state: PipelineDebugState) -> PipelineDebugState:
    print("  [2/5] Routing based on log analysis findings...")
    category = state["log_analysis"].issue_category.value

    schema_primary = {"type_cast_failure", "missing_column", "schema_mismatch"}
    dq_primary = {"null_spike", "duplicate_records", "join_explosion",
                  "late_arriving_data", "partition_mismatch"}

    if category in schema_primary:
        state["needs_schema_deep_dive"] = True
        state["needs_dq_deep_dive"] = False
        state["is_unknown_error"] = False
        print(f"     → Schema deep dive triggered (category: {category})")
    elif category in dq_primary:
        state["needs_schema_deep_dive"] = False
        state["needs_dq_deep_dive"] = True
        state["is_unknown_error"] = False
        print(f"     → DQ deep dive triggered (category: {category})")
    else:
        state["needs_schema_deep_dive"] = True
        state["needs_dq_deep_dive"] = True
        state["is_unknown_error"] = True
        print(f"     → Unknown error — both deep dives triggered")

    return state


def schema_validation_primary_node(state: PipelineDebugState) -> PipelineDebugState:
    print("  [3/5] Running Schema Validation Agent (deep dive)...")
    result = run_schema_validation_agent(state["incident"])
    state["schema_validation"] = result
    return state


def schema_validation_secondary_node(state: PipelineDebugState) -> PipelineDebugState:
    print("  [4/5] Running Schema Validation Agent (secondary)...")
    result = run_schema_validation_agent(state["incident"])
    state["schema_validation"] = result
    return state


def data_quality_primary_node(state: PipelineDebugState) -> PipelineDebugState:
    print("  [3/5] Running Data Quality Agent (deep dive)...")
    result = run_data_quality_agent(state["incident"])
    state["data_quality"] = result
    return state


def data_quality_secondary_node(state: PipelineDebugState) -> PipelineDebugState:
    print("  [4/5] Running Data Quality Agent (secondary)...")
    result = run_data_quality_agent(state["incident"])
    state["data_quality"] = result
    return state


def root_cause_node(state: PipelineDebugState) -> PipelineDebugState:
    print("  [5a/5] Running Root Cause Agent...")
    result = run_root_cause_agent(
        state["incident"],
        state["log_analysis"],
        state["schema_validation"],
        state["data_quality"]
    )
    state["root_cause"] = result
    return state


def fix_recommendation_node(state: PipelineDebugState) -> PipelineDebugState:
    print("  [5b/5] Running Fix Recommendation Agent...")
    result = run_fix_recommendation_agent(
        state["incident"],
        state["root_cause"]
    )
    state["fix_recommendation"] = result
    return state


# ─── Conditional Edge ─────────────────────────────────────────────────────────

def route_after_log_analysis(state: PipelineDebugState) -> str:
    category = state["log_analysis"].issue_category.value
    schema_primary = {"type_cast_failure", "missing_column", "schema_mismatch"}

    if category in schema_primary:
        return "schema_first"
    else:
        return "dq_first"


# ─── Build Graph ──────────────────────────────────────────────────────────────

def build_graph() -> StateGraph:
    graph = StateGraph(PipelineDebugState)

    # Add nodes
    graph.add_node("log_analysis", log_analysis_node)
    graph.add_node("routing", routing_node)
    graph.add_node("schema_validation_primary", schema_validation_primary_node)
    graph.add_node("schema_validation_secondary", schema_validation_secondary_node)
    graph.add_node("data_quality_primary", data_quality_primary_node)
    graph.add_node("data_quality_secondary", data_quality_secondary_node)
    graph.add_node("root_cause", root_cause_node)
    graph.add_node("fix_recommendation", fix_recommendation_node)

    # Entry point
    graph.set_entry_point("log_analysis")

    # Log analysis → routing
    graph.add_edge("log_analysis", "routing")

    # Conditional routing
    graph.add_conditional_edges(
        "routing",
        route_after_log_analysis,
        {
            "schema_first": "schema_validation_primary",
            "dq_first": "data_quality_primary"
        }
    )

    # Schema first path: schema (primary) → dq (secondary) → root cause
    graph.add_edge("schema_validation_primary", "data_quality_secondary")
    graph.add_edge("data_quality_secondary", "root_cause")

    # DQ first path: dq (primary) → schema (secondary) → root cause
    graph.add_edge("data_quality_primary", "schema_validation_secondary")
    graph.add_edge("schema_validation_secondary", "root_cause")

    # Root cause → fix recommendation → end
    graph.add_edge("root_cause", "fix_recommendation")
    graph.add_edge("fix_recommendation", END)

    return graph.compile()


# Export compiled graph
pipeline_debug_graph = build_graph()