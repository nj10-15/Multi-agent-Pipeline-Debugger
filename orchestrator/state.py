from typing import Optional
from typing_extensions import TypedDict
from models.schemas import (
    LogAnalysisOutput,
    SchemaValidationOutput,
    DataQualityOutput,
    RootCauseOutput,
    FixRecommendationOutput
)


class PipelineDebugState(TypedDict):
    # Input
    incident: dict

    # Agent outputs
    log_analysis: Optional[LogAnalysisOutput]
    schema_validation: Optional[SchemaValidationOutput]
    data_quality: Optional[DataQualityOutput]
    root_cause: Optional[RootCauseOutput]
    fix_recommendation: Optional[FixRecommendationOutput]

    # Routing flags — explicitly set based on issue category
    needs_schema_deep_dive: bool
    needs_dq_deep_dive: bool
    is_unknown_error: bool  # forces both deep dives

    # Final output
    final_report: Optional[str]