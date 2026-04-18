from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from models.schemas import SchemaValidationOutput
from dotenv import load_dotenv
import json

load_dotenv()

llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
structured_llm = llm.with_structured_output(SchemaValidationOutput)

prompt = ChatPromptTemplate.from_messages([
    ("system", """You are a senior data engineer specialized in schema validation 
    for ETL pipelines.
    
    Your job is to:
    - Compare the expected schema against the actual schema
    - Identify missing columns that exist in expected but not in actual
    - Identify type changes where a column exists in both but with different types
    - Identify extra columns that exist in actual but not in expected
    - Summarize the evidence clearly
    - Rate your confidence from 0.0 to 1.0
    
    Be precise. Only report what you can directly observe from the schemas."""),
    ("human", """Compare these schemas and identify any mismatches:

Pipeline: {pipeline_name}
Run ID: {run_id}

Expected Schema:
{expected_schema}

Actual Schema:
{actual_schema}
""")
])

chain = prompt | structured_llm


def run_schema_validation_agent(incident: dict) -> SchemaValidationOutput:
    result = chain.invoke({
        "pipeline_name": incident.get("pipeline_name", "unknown"),
        "run_id": incident.get("run_id", "unknown"),
        "expected_schema": json.dumps(incident.get("expected_schema", {}), indent=2),
        "actual_schema": json.dumps(incident.get("actual_schema", {}), indent=2)
    })

    return SchemaValidationOutput.model_validate(result)