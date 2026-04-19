from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from models.schemas import RootCauseOutput
from dotenv import load_dotenv
import json

load_dotenv()

llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
structured_llm = llm.with_structured_output(RootCauseOutput)

prompt = ChatPromptTemplate.from_messages([
    ("system", """You are a data engineer diagnosing ETL pipeline failures.
    
    Analyze the provided pipeline information and identify:
    - The root cause of the failure
    - The issue category
    - Severity level: low, medium, high, or critical
    - Your confidence from 0.0 to 1.0
    - Supporting evidence
    
    Issue categories:
    schema_mismatch, null_spike, duplicate_records, join_explosion,
    type_cast_failure, late_arriving_data, partition_mismatch,
    missing_column, unknown"""),
    ("human", """Diagnose this pipeline failure:

Pipeline: {pipeline_name}
Run ID: {run_id}

Logs:
{logs}

Expected Schema:
{expected_schema}

Actual Schema:
{actual_schema}

Row Counts:
{row_counts}

Data Quality Metrics:
{dq_metrics}

SQL Snippet:
{sql_snippet}
""")
])

chain = prompt | structured_llm


def run_baseline_agent(incident: dict) -> RootCauseOutput:
    result = chain.invoke({
        "pipeline_name": incident.get("pipeline_name", "unknown"),
        "run_id": incident.get("run_id", "unknown"),
        "logs": "\n".join(incident.get("logs", [])),
        "expected_schema": json.dumps(incident.get("expected_schema", {}), indent=2),
        "actual_schema": json.dumps(incident.get("actual_schema", {}), indent=2),
        "row_counts": json.dumps(incident.get("row_counts", {}), indent=2),
        "dq_metrics": json.dumps(incident.get("dq_metrics", {}), indent=2),
        "sql_snippet": incident.get("sql_snippet", "N/A")
    })

    return RootCauseOutput.model_validate(result)