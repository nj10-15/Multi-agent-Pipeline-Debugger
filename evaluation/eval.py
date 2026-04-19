import json
import glob
import time
from datetime import datetime
from evaluation.baseline_agent import run_baseline_agent
from orchestrator.graph import pipeline_debug_graph


def run_evaluation():
    scenarios = sorted(glob.glob("scenarios/scenario_*.json"))
    total = len(scenarios)

    print(f"\n{'='*60}")
    print(f"PIPELINE DEBUGGER EVALUATION")
    print(f"Running {total} scenarios — baseline vs multi-agent")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")

    baseline_results = []
    multiagent_results = []

    for i, path in enumerate(scenarios, 1):
        with open(path) as f:
            incident = json.load(f)

        ground_truth = incident["ground_truth_root_cause"]
        pipeline = incident["pipeline_name"]

        print(f"[{i:02d}/{total}] {pipeline}")

        # --- Baseline ---
        try:
            t0 = time.time()
            baseline_out = run_baseline_agent(incident)
            baseline_time = round(time.time() - t0, 2)
            baseline_pred = baseline_out.issue_category.value
            baseline_correct = baseline_pred == ground_truth
            baseline_confidence = baseline_out.confidence
        except Exception as e:
            baseline_pred = "error"
            baseline_correct = False
            baseline_confidence = 0.0
            baseline_time = 0.0
            print(f"  Baseline error: {e}")

        # --- Multi-Agent ---
        try:
            t0 = time.time()
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
            multiagent_time = round(time.time() - t0, 2)
            multiagent_pred = result["root_cause"].issue_category.value
            multiagent_correct = multiagent_pred == ground_truth
            multiagent_confidence = result["root_cause"].confidence
        except Exception as e:
            multiagent_pred = "error"
            multiagent_correct = False
            multiagent_confidence = 0.0
            multiagent_time = 0.0
            print(f"  Multi-agent error: {e}")

        # --- Print row result ---
        b_mark = "✅" if baseline_correct else "❌"
        m_mark = "✅" if multiagent_correct else "❌"
        print(f"  Ground truth:  {ground_truth}")
        print(f"  Baseline:      {baseline_pred} {b_mark} ({baseline_time}s)")
        print(f"  Multi-agent:   {multiagent_pred} {m_mark} ({multiagent_time}s)")
        print()

        baseline_results.append({
            "scenario": path,
            "pipeline": pipeline,
            "ground_truth": ground_truth,
            "predicted": baseline_pred,
            "correct": baseline_correct,
            "confidence": baseline_confidence,
            "time_seconds": baseline_time
        })

        multiagent_results.append({
            "scenario": path,
            "pipeline": pipeline,
            "ground_truth": ground_truth,
            "predicted": multiagent_pred,
            "correct": multiagent_correct,
            "confidence": multiagent_confidence,
            "time_seconds": multiagent_time
        })

    # --- Summary ---
    baseline_correct_count = sum(r["correct"] for r in baseline_results)
    multiagent_correct_count = sum(r["correct"] for r in multiagent_results)

    baseline_acc = round(baseline_correct_count / total * 100, 1)
    multiagent_acc = round(multiagent_correct_count / total * 100, 1)

    baseline_avg_conf = round(
        sum(r["confidence"] for r in baseline_results) / total, 3)
    multiagent_avg_conf = round(
        sum(r["confidence"] for r in multiagent_results) / total, 3)

    baseline_avg_time = round(
        sum(r["time_seconds"] for r in baseline_results) / total, 2)
    multiagent_avg_time = round(
        sum(r["time_seconds"] for r in multiagent_results) / total, 2)

    improvement = round(multiagent_acc - baseline_acc, 1)

    print(f"\n{'='*60}")
    print(f"EVALUATION RESULTS")
    print(f"{'='*60}")
    print(f"{'Metric':<30} {'Baseline':>12} {'Multi-Agent':>12}")
    print(f"{'-'*54}")
    print(f"{'Accuracy':<30} {str(baseline_acc)+'%':>12} {str(multiagent_acc)+'%':>12}")
    print(f"{'Correct / Total':<30} {str(baseline_correct_count)+'/'+str(total):>12} {str(multiagent_correct_count)+'/'+str(total):>12}")
    print(f"{'Avg Confidence':<30} {str(baseline_avg_conf):>12} {str(multiagent_avg_conf):>12}")
    print(f"{'Avg Time (seconds)':<30} {str(baseline_avg_time):>12} {str(multiagent_avg_time):>12}")
    print(f"{'-'*54}")
    print(f"{'Accuracy Improvement':<30} {'+'+str(improvement)+'%':>12}")
    print(f"{'='*60}")

    # --- Save results to JSON ---
    output = {
        "evaluation_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_scenarios": total,
        "baseline": {
            "accuracy": baseline_acc,
            "correct": baseline_correct_count,
            "avg_confidence": baseline_avg_conf,
            "avg_time_seconds": baseline_avg_time,
            "results": baseline_results
        },
        "multi_agent": {
            "accuracy": multiagent_acc,
            "correct": multiagent_correct_count,
            "avg_confidence": multiagent_avg_conf,
            "avg_time_seconds": multiagent_avg_time,
            "results": multiagent_results
        },
        "improvement": improvement
    }

    with open("evaluation/eval_results.json", "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nFull results saved to evaluation/eval_results.json")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    run_evaluation()