from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from models.schemas import LogAnalysisOutput, IssueCategory
from dotenv import load_dotenv

load_dotenv()

llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
structured_llm = llm.with_structured_output(LogAnalysisOutput)

prompt = ChatPromptTemplate.from_messages([
    ("system", """You are a senior data engineer specialized in diagnosing 
    ETL pipeline failures by analyzing error logs.
    
    Your job is to:
    - Read the pipeline logs carefully
    - Identify the failure pattern
    - Classify the issue into one of these categories:
      schema_mismatch, null_spike, duplicate_records, join_explosion,
      type_cast_failure, late_arriving_data, partition_mismatch, 
      missing_column, unknown
    - Extract the key error message
    - Summarize the evidence
    - Rate your confidence from 0.0 to 1.0
    
    Be precise and technical. Base your analysis only on what the logs show."""),
    ("human", """Analyze these pipeline logs and identify the failure:

Pipeline: {pipeline_name}
Run ID: {run_id}

Logs:
{logs}

SQL Snippet:
{sql_snippet}
""")
])

chain = prompt | structured_llm


def run_log_analysis_agent(incident: dict) -> LogAnalysisOutput:
    logs_formatted = "\n".join(incident.get("logs", []))
    
    result = chain.invoke({
        "pipeline_name": incident.get("pipeline_name", "unknown"),
        "run_id": incident.get("run_id", "unknown"),
        "logs": logs_formatted,
        "sql_snippet": incident.get("sql_snippet", "N/A")
    })
    
    return LogAnalysisOutput.model_validate(result)