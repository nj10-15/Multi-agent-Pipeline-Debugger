from datetime import datetime
from models.schemas import (
    LogAnalysisOutput,
    SchemaValidationOutput,
    DataQualityOutput,
    RootCauseOutput,
    FixRecommendationOutput
)


def generate_report(
    incident: dict,
    log_output: LogAnalysisOutput,
    schema_output: SchemaValidationOutput,
    dq_output: DataQualityOutput,
    root_cause_output: RootCauseOutput,
    fix_output: FixRecommendationOutput
) -> str:

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    severity = root_cause_output.severity.value.upper()
    category = root_cause_output.issue_category.value.replace("_", " ").title()
    confidence_pct = round(root_cause_output.confidence * 100)

    # Severity border line
    border = "=" * 60

    report = f"""
{border}
PIPELINE INCIDENT REPORT
{border}
Generated:     {now}
Pipeline:      {incident.get('pipeline_name', 'unknown')}
Run ID:        {incident.get('run_id', 'unknown')}
Severity:      {severity}
Confidence:    {confidence_pct}%
{border}

INCIDENT SUMMARY
----------------
{root_cause_output.root_cause}

ISSUE CATEGORY
--------------
{category}

EVIDENCE COLLECTED
------------------
Log Analysis:
  - Issue detected: {log_output.issue_detected}
  - Error message:  {log_output.error_message}
  - Evidence:       {log_output.evidence}
  - Confidence:     {round(log_output.confidence * 100)}%

Schema Validation:
  - Mismatch detected:  {schema_output.schema_mismatch_detected}
  - Missing columns:    {schema_output.missing_columns if schema_output.missing_columns else 'None'}
  - Type changes:       {schema_output.type_changes if schema_output.type_changes else 'None'}
  - Extra columns:      {schema_output.extra_columns if schema_output.extra_columns else 'None'}
  - Evidence:           {schema_output.evidence}
  - Confidence:         {round(schema_output.confidence * 100)}%

Data Quality:
  - Quality issue detected:  {dq_output.quality_issue_detected}
  - Null spike columns:      {dq_output.null_spike_columns if dq_output.null_spike_columns else 'None'}
  - Duplicate count:         {dq_output.duplicate_count}
  - Row count mismatch:      {dq_output.row_count_mismatch}
  - Evidence:                {dq_output.evidence}
  - Confidence:              {round(dq_output.confidence * 100)}%

SUPPORTING EVIDENCE
-------------------
{chr(10).join(f'  - {e}' for e in root_cause_output.supporting_evidence)}

IMPACT ASSESSMENT
-----------------
Source rows:   {incident.get('row_counts', {}).get('source', 'unknown')}
Target rows:   {incident.get('row_counts', {}).get('target', 'unknown')}
Failed rules:  {incident.get('dq_metrics', {}).get('failed_rules', [])}

RECOMMENDED FIXES
-----------------
Priority fix:
  {fix_output.priority_fix}

All recommended fixes:
{chr(10).join(f'  {i+1}. {fix}' for i, fix in enumerate(fix_output.recommended_fixes))}

Estimated impact:
  {fix_output.estimated_impact}

{border}
END OF REPORT
{border}
"""
    return report.strip()


def save_report(report: str, pipeline_name: str, run_id: str) -> str:
    import os
    os.makedirs("reports", exist_ok=True)
    safe_name = pipeline_name.replace(" ", "_").replace("/", "_")
    filename = f"reports/{safe_name}_{run_id}.txt"
    with open(filename, "w") as f:
        f.write(report)
    return filename
