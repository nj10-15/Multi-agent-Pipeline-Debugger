from pydantic import BaseModel
from typing import Optional
from enum import Enum


class Severity(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class IssueCategory(str, Enum):
    SCHEMA_MISMATCH = "schema_mismatch"
    NULL_SPIKE = "null_spike"
    DUPLICATE_RECORDS = "duplicate_records"
    JOIN_EXPLOSION = "join_explosion"
    TYPE_CAST_FAILURE = "type_cast_failure"
    LATE_ARRIVING_DATA = "late_arriving_data"
    PARTITION_MISMATCH = "partition_mismatch"
    MISSING_COLUMN = "missing_column"
    UNKNOWN = "unknown"


class LogAnalysisOutput(BaseModel):
    issue_detected: bool
    issue_category: IssueCategory
    error_message: str
    evidence: str
    confidence: float  # 0.0 to 1.0


class SchemaValidationOutput(BaseModel):
    schema_mismatch_detected: bool
    missing_columns: list[str]
    type_changes: list[str]
    extra_columns: list[str]
    evidence: str
    confidence: float


class DataQualityOutput(BaseModel):
    quality_issue_detected: bool
    null_spike_columns: list[str]
    duplicate_count: int
    row_count_mismatch: bool
    evidence: str
    confidence: float


class RootCauseOutput(BaseModel):
    root_cause: str
    issue_category: IssueCategory
    supporting_evidence: list[str]
    confidence: float
    severity: Severity


class FixRecommendationOutput(BaseModel):
    recommended_fixes: list[str]
    priority_fix: str
    estimated_impact: str
    severity: Severity


class FinalReport(BaseModel):
    pipeline_name: str
    run_id: str
    issue_category: IssueCategory
    root_cause: str
    impact_summary: str
    recommended_fixes: list[str]
    severity: Severity
    confidence: float
    incident_summary: str