from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from models.schemas import RootCauseOutput, LogAnalysisOutput, SchemaValidationOutput, DataQualityOutput
from dotenv import load_dotenv

load_dotenv()

llm = ChatOpenAI(model="gpt-4o", temperature=0)
structured_llm = llm.with_structured_output(RootCauseOutput)

prompt = ChatPromptTemplate.from_messages([
    ("system", """You are a principal data engineer specialized in root cause 
    analysis for ETL pipeline failures.
    
    You will receive findings from three specialized agents:
    - Log Analysis Agent: extracted failure patterns from logs
    - Schema Validation Agent: identified schema mismatches
    - Data Quality Agent: detected data anomalies
    
    Your job is to:
    - Synthesize all three agents findings into one coherent root cause
    - Identify which evidence is most conclusive
    - Classify the root cause into the most specific category possible
    - List the key pieces of supporting evidence
    - Assign a severity level: low, medium, high, or critical
    - Rate your confidence from 0.0 to 1.0
    
    Think step by step. Cross-reference findings across agents before concluding."""),
    ("human", """Synthesize these agent findings and determine the root cause:

Pipeline: {pipeline_name}
Run ID: {run_id}

--- LOG ANALYSIS FINDINGS ---
Issue Detected: {log_issue_detected}
Issue Category: {log_issue_category}
Error Message: {log_error_message}
Evidence: {log_evidence}
Confidence: {log_confidence}

--- SCHEMA VALIDATION FINDINGS ---
Mismatch Detected: {schema_mismatch_detected}
Missing Columns: {schema_missing_columns}
Type Changes: {schema_type_changes}
Extra Columns: {schema_extra_columns}
Evidence: {schema_evidence}
Confidence: {schema_confidence}

--- DATA QUALITY FINDINGS ---
Quality Issue Detected: {dq_issue_detected}
Null Spike Columns: {dq_null_spike_columns}
Duplicate Count: {dq_duplicate_count}
Row Count Mismatch: {dq_row_count_mismatch}
Evidence: {dq_evidence}
Confidence: {dq_confidence}
""")
])

chain = prompt | structured_llm


def run_root_cause_agent(
    incident: dict,
    log_output: LogAnalysisOutput,
    schema_output: SchemaValidationOutput,
    dq_output: DataQualityOutput
) -> RootCauseOutput:

    result = chain.invoke({
        "pipeline_name": incident.get("pipeline_name", "unknown"),
        "run_id": incident.get("run_id", "unknown"),
        "log_issue_detected": log_output.issue_detected,
        "log_issue_category": log_output.issue_category,
        "log_error_message": log_output.error_message,
        "log_evidence": log_output.evidence,
        "log_confidence": log_output.confidence,
        "schema_mismatch_detected": schema_output.schema_mismatch_detected,
        "schema_missing_columns": schema_output.missing_columns,
        "schema_type_changes": schema_output.type_changes,
        "schema_extra_columns": schema_output.extra_columns,
        "schema_evidence": schema_output.evidence,
        "schema_confidence": schema_output.confidence,
        "dq_issue_detected": dq_output.quality_issue_detected,
        "dq_null_spike_columns": dq_output.null_spike_columns,
        "dq_duplicate_count": dq_output.duplicate_count,
        "dq_row_count_mismatch": dq_output.row_count_mismatch,
        "dq_evidence": dq_output.evidence,
        "dq_confidence": dq_output.confidence
    })

    return RootCauseOutput.model_validate(result)