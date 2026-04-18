from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from models.schemas import DataQualityOutput
from dotenv import load_dotenv
import json

load_dotenv()

llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
structured_llm = llm.with_structured_output(DataQualityOutput)

prompt = ChatPromptTemplate.from_messages([
    ("system", """You are a senior data quality engineer specialized in 
    detecting data anomalies in ETL pipelines.
    
    Your job is to:
    - Inspect null ratios for each column and flag any above 0.05 (5%)
    - Check for duplicate primary keys and flag if count is above 0
    - Check for row count mismatches between source and target
    - Identify which columns have null spikes
    - Summarize the evidence clearly
    - Rate your confidence from 0.0 to 1.0
    
    Be precise. Only report what you can directly observe from the metrics."""),
    ("human", """Analyze these data quality metrics and identify any issues:

Pipeline: {pipeline_name}
Run ID: {run_id}

Row Counts:
{row_counts}

Data Quality Metrics:
{dq_metrics}

Failed Rules:
{failed_rules}
""")
])

chain = prompt | structured_llm


def run_data_quality_agent(incident: dict) -> DataQualityOutput:
    dq = incident.get("dq_metrics", {})
    row_counts = incident.get("row_counts", {})

    result = chain.invoke({
        "pipeline_name": incident.get("pipeline_name", "unknown"),
        "run_id": incident.get("run_id", "unknown"),
        "row_counts": json.dumps(row_counts, indent=2),
        "dq_metrics": json.dumps(dq, indent=2),
        "failed_rules": json.dumps(incident.get("dq_metrics", {}).get("failed_rules", []), indent=2)
    })

    return DataQualityOutput.model_validate(result)