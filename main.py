import json
import os
import glob
from orchestrator.graph import pipeline_debug_graph


def run_single_incident(scenario_path: str):
    with open(scenario_path) as f:
        incident = json.load(f)

    print(f"\n{'='*60}")
    print(f"Pipeline: {incident['pipeline_name']}")
    print(f"Run ID:   {incident['run_id']}")
    print(f"{'='*60}")

    result = pipeline_debug_graph.invoke({
        "incident": incident,
        "log_analysis": None,
        "schema_validation": None,
        "data_quality": None,
        "root_cause": None,
        "fix_recommendation": None,
        "needs_schema_deep_dive": False,
        "needs_dq_deep_dive": False,
        "is_unknown_error": False,
        "final_report": None
    })

    root_cause = result["root_cause"]
    fix = result["fix_recommendation"]

    print(f"\n--- DIAGNOSIS COMPLETE ---")
    print(f"Root Cause:      {root_cause.root_cause}")
    print(f"Category:        {root_cause.issue_category.value}")
    print(f"Severity:        {root_cause.severity.value}")
    print(f"Confidence:      {root_cause.confidence}")
    print(f"\nPriority Fix:    {fix.priority_fix}")
    print(f"Impact:          {fix.estimated_impact}")
    print(f"\nGround Truth:    {incident['ground_truth_root_cause']}")

    predicted = root_cause.issue_category.value
    actual = incident["ground_truth_root_cause"]
    match = "✅ CORRECT" if predicted == actual else "❌ INCORRECT"
    print(f"Prediction:      {match}")

    # Generate and save report
    from utils.report_generator import generate_report, save_report
    report = generate_report(
        incident,
        result["log_analysis"],
        result["schema_validation"],
        result["data_quality"],
        root_cause,
        fix
    )
    filepath = save_report(report, incident["pipeline_name"], incident["run_id"])
    print(f"Report saved:    {filepath}")

    return result


def run_all_scenarios():
    scenarios = sorted(glob.glob("scenarios/scenario_*.json"))
    print(f"\nFound {len(scenarios)} scenarios to process...")

    correct = 0
    total = len(scenarios)

    for path in scenarios:
        result = run_single_incident(path)
        predicted = result["root_cause"].issue_category.value
        with open(path) as f:
            actual = json.load(f)["ground_truth_root_cause"]
        if predicted == actual:
            correct += 1

    print(f"\n{'='*60}")
    print(f"FINAL ACCURACY: {correct}/{total} ({round(correct/total*100, 1)}%)")
    print(f"{'='*60}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        # Run single scenario: python main.py scenarios/scenario_01.json
        run_single_incident(sys.argv[1])
    else:
        # Run all scenarios
        run_all_scenarios()