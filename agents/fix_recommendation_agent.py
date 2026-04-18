from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from models.schemas import FixRecommendationOutput, RootCauseOutput
from dotenv import load_dotenv

load_dotenv()

llm = ChatOpenAI(model="gpt-4o", temperature=0)
structured_llm = llm.with_structured_output(FixRecommendationOutput)

prompt = ChatPromptTemplate.from_messages([
    ("system", """You are a principal data engineer specialized in recommending 
    practical fixes for ETL pipeline failures.
    
    Your job is to:
    - Read the confirmed root cause and severity
    - Recommend 2-4 concrete, actionable fixes ordered by priority
    - Identify the single most important fix to apply first
    - Estimate the business impact of applying the fixes
    - Assign a severity level: low, medium, high, or critical
    
    Be specific and technical. Fixes should be directly implementable by 
    a data engineer. Avoid vague advice like 'check the pipeline'."""),
    ("human", """Recommend fixes for this confirmed pipeline failure:

Pipeline: {pipeline_name}
Run ID: {run_id}

Root Cause: {root_cause}
Issue Category: {issue_category}
Severity: {severity}
Confidence: {confidence}

Supporting Evidence:
{supporting_evidence}
""")
])

chain = prompt | structured_llm


def run_fix_recommendation_agent(
    incident: dict,
    root_cause_output: RootCauseOutput
) -> FixRecommendationOutput:

    result = chain.invoke({
        "pipeline_name": incident.get("pipeline_name", "unknown"),
        "run_id": incident.get("run_id", "unknown"),
        "root_cause": root_cause_output.root_cause,
        "issue_category": root_cause_output.issue_category,
        "severity": root_cause_output.severity,
        "confidence": root_cause_output.confidence,
        "supporting_evidence": "\n".join(root_cause_output.supporting_evidence)
    })

    return FixRecommendationOutput.model_validate(result)