from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


ActionType = Literal[
    "insert_formula",
    "fill_formula_down",
    "explain_formula",
    "fix_formula",
    "generate_formula",
    "batch",
    "create_table",
    "freeze_header",
    "auto_fit_columns",
    "format_header",
    "format_number",
    "insert_rows",
    "delete_rows",
    "insert_columns",
    "delete_columns",
    "clear_cells",
    "merge_cells",
    "unmerge_cells",
    "rename_sheet",
    "hide_rows",
    "unhide_rows",
    "hide_columns",
    "unhide_columns",
    "add_comment",
    "add_hyperlink",
    "add_validation",
    "conditional_format_range",
    "sort",
    "delete_duplicates",
    "find_replace",
    "highlight_threshold",
    "apply_filter",
    "clear_filter",
    "create_chart",
    "convert_column_type",
    "create_pivot",
    "analyze_workbook",
    "recommend_chart",
    "add_sheet",
    "join_sheets",
    "noop",
]

RiskLevel = Literal["low", "medium", "high"]


class ActionImpact(BaseModel):
    summary: str
    estimated_rows: int = 0
    estimated_cells: int = 0
    warnings: list[str] = Field(default_factory=list)


class CommandTemplate(BaseModel):
    title: str
    prompt: str
    description: str
    category: str


class TaskStep(BaseModel):
    title: str
    action: str
    target_sheet: str
    explanation: str
    risk_level: RiskLevel = "low"
    requires_confirmation: bool = False
    target_cell: str | None = None
    target_column: str | None = None
    formula: str | None = None
    parameters: dict[str, Any] = Field(default_factory=dict)


class ConversationMemory(BaseModel):
    last_command: str | None = None
    last_action: str | None = None
    last_target_sheet: str | None = None
    last_target_column: str | None = None
    last_formula: str | None = None
    last_preview_title: str | None = None
    recent_commands: list[str] = Field(default_factory=list)
    recent_actions: list[str] = Field(default_factory=list)
    recent_steps: list[TaskStep] = Field(default_factory=list)
    follow_up_prompts: list[str] = Field(default_factory=list)


class WorkbookInsight(BaseModel):
    title: str
    detail: str
    severity: Literal["info", "warning", "critical"] = "info"
    sheet_name: str | None = None


class ChartRecommendation(BaseModel):
    title: str
    detail: str
    sheet_name: str
    chart_type: str
    category_column: str | None = None
    value_column: str | None = None
    confidence: Literal["low", "medium", "high"] = "medium"


class WorkbookAnomaly(BaseModel):
    title: str
    detail: str
    sheet_name: str
    column: str | None = None
    count: int = 0
    sample: list[str] = Field(default_factory=list)


class CommandRecord(BaseModel):
    record_id: str
    user_command: str
    action: str
    explanation: str
    target_sheet: str
    status: str
    created_at: str
    risk_level: RiskLevel


class WorkbookStats(BaseModel):
    sheet_count: int
    total_rows: int
    total_columns: int
    total_formula_cells: int
    total_hidden_rows: int
    total_non_empty_cells: int
    numeric_column_count: int
    text_column_count: int


class SheetSummary(BaseModel):
    name: str
    headers: list[str]
    rows: list[list[Any]]
    row_numbers: list[int]
    max_row: int
    max_column: int
    hidden_row_count: int
    formula_cell_count: int
    visible_data_row_count: int
    preview_truncated: bool


class SheetContext(BaseModel):
    name: str
    row_count: int
    column_count: int
    headers: list[str]
    data_types: dict[str, str]
    numeric_headers: list[str]
    text_headers: list[str]
    date_headers: list[str]
    sample_rows: list[dict[str, Any]]


class WorkbookSnapshot(BaseModel):
    session_id: str
    file_path: str
    active_sheet: str
    dirty: bool
    can_undo: bool
    can_redo: bool
    sheets: list[SheetSummary]
    context: list[SheetContext]
    stats: WorkbookStats
    history: list[CommandRecord]
    templates: list[CommandTemplate]
    suggested_prompts: list[str]
    memory: ConversationMemory
    insights: list[WorkbookInsight]
    chart_recommendations: list[ChartRecommendation]
    anomalies: list[WorkbookAnomaly]


class WorkbookOpenRequest(BaseModel):
    file_path: str


class WorkbookSessionRequest(BaseModel):
    session_id: str


class WorkbookSheetRequest(BaseModel):
    session_id: str
    sheet_name: str


class UpdateCellRequest(BaseModel):
    session_id: str
    sheet_name: str
    cell: str
    value: Any = None


class SaveWorkbookRequest(BaseModel):
    session_id: str
    destination_path: str | None = None


class ActionPlan(BaseModel):
    action: ActionType
    target_sheet: str
    target_cell: str | None = None
    target_column: str | None = None
    formula: str | None = None
    explanation: str
    preview_title: str
    risk_level: RiskLevel = "low"
    requires_confirmation: bool = False
    impacted_range: str | None = None
    parameters: dict[str, Any] = Field(default_factory=dict)
    impact: ActionImpact = Field(default_factory=lambda: ActionImpact(summary="No changes will be made."))


class CommandPreviewRequest(BaseModel):
    session_id: str
    command: str = Field(min_length=2)
    selected_cell: str | None = None
    selected_value: Any = None


class CommandPreviewResponse(BaseModel):
    plan: ActionPlan
    snapshot: WorkbookSnapshot


class CommandExecuteRequest(BaseModel):
    session_id: str
    plan: ActionPlan


class SaveWorkbookResponse(BaseModel):
    file_path: str
    snapshot: WorkbookSnapshot
