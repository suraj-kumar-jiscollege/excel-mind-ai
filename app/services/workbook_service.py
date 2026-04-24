from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any
from uuid import uuid4

from fastapi import HTTPException
from openpyxl import load_workbook
from openpyxl.chart import BarChart, LineChart, PieChart, Reference
from openpyxl.formula.translate import Translator
from openpyxl.formatting.rule import CellIsRule
from openpyxl.styles import PatternFill
from openpyxl.utils import column_index_from_string, get_column_letter
from openpyxl.workbook import Workbook
from openpyxl.worksheet.worksheet import Worksheet

from app.config import settings
from app.models import (
    ActionPlan,
    CommandRecord,
    CommandTemplate,
    RiskLevel,
    SheetContext,
    SheetSummary,
    WorkbookSnapshot,
    WorkbookStats,
)


@dataclass(slots=True)
class WorkbookSession:
    session_id: str
    file_path: Path
    workbook: Workbook
    active_sheet: str
    dirty: bool = False
    command_history: list[CommandRecord] = field(default_factory=list)
    undo_stack: list[bytes] = field(default_factory=list)
    redo_stack: list[bytes] = field(default_factory=list)


class WorkbookService:
    def __init__(self) -> None:
        self._sessions: dict[str, WorkbookSession] = {}

    def open_workbook(self, file_path: str) -> WorkbookSnapshot:
        path = Path(file_path).expanduser().resolve()
        self._validate_path(path)
        if not path.exists():
            raise HTTPException(status_code=404, detail="Workbook file not found.")
        if path.suffix.lower() != ".xlsx":
            raise HTTPException(status_code=400, detail="Only .xlsx files are supported.")

        workbook = load_workbook(path)
        session_id = str(uuid4())
        session = WorkbookSession(
            session_id=session_id,
            file_path=path,
            workbook=workbook,
            active_sheet=workbook.active.title,
        )
        self._sessions[session_id] = session
        return self.get_snapshot(session_id)

    def get_session(self, session_id: str) -> WorkbookSession:
        session = self._sessions.get(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Workbook session not found.")
        return session

    def get_snapshot(self, session_id: str) -> WorkbookSnapshot:
        session = self.get_session(session_id)
        workbook = session.workbook
        sheets = [self._sheet_summary(workbook[sheet_name]) for sheet_name in workbook.sheetnames]
        context = [self._sheet_context(workbook[sheet_name]) for sheet_name in workbook.sheetnames]
        stats = self._build_stats(workbook, sheets, context)
        return WorkbookSnapshot(
            session_id=session.session_id,
            file_path=str(session.file_path),
            active_sheet=session.active_sheet,
            dirty=session.dirty,
            can_undo=bool(session.undo_stack),
            can_redo=bool(session.redo_stack),
            sheets=sheets,
            context=context,
            stats=stats,
            history=session.command_history,
            templates=self._build_templates(context, session.active_sheet, workbook.sheetnames),
            suggested_prompts=self._build_suggested_prompts(context, session.active_sheet, workbook.sheetnames),
        )

    def set_active_sheet(self, session_id: str, sheet_name: str) -> WorkbookSnapshot:
        session = self.get_session(session_id)
        self._get_sheet(session, sheet_name)
        session.active_sheet = sheet_name
        return self.get_snapshot(session_id)

    def update_cell(self, session_id: str, sheet_name: str, cell: str, value: Any) -> WorkbookSnapshot:
        session = self.get_session(session_id)
        worksheet = self._get_sheet(session, sheet_name)
        self._checkpoint(session)
        previous_value = worksheet[cell].value
        worksheet[cell] = value
        session.dirty = True
        self._record_history(
            session,
            user_command=f"Manual edit {sheet_name}!{cell}",
            action="manual_edit",
            explanation=f"Updated {sheet_name}!{cell} from {self._stringify(previous_value)} to {self._stringify(value)}.",
            target_sheet=sheet_name,
            risk_level="low",
        )
        return self.get_snapshot(session_id)

    def save_workbook(self, session_id: str, destination_path: str | None = None) -> tuple[str, WorkbookSnapshot]:
        session = self.get_session(session_id)
        target = Path(destination_path).expanduser().resolve() if destination_path else session.file_path
        self._validate_path(target)
        target.parent.mkdir(parents=True, exist_ok=True)
        session.workbook.save(target)
        session.file_path = target
        session.dirty = False
        return str(target), self.get_snapshot(session_id)

    def undo(self, session_id: str) -> WorkbookSnapshot:
        session = self.get_session(session_id)
        if not session.undo_stack:
            raise HTTPException(status_code=400, detail="Nothing to undo.")
        session.redo_stack.append(self._serialize_workbook(session.workbook))
        previous_state = session.undo_stack.pop()
        session.workbook = self._restore_workbook(previous_state)
        if session.active_sheet not in session.workbook.sheetnames:
            session.active_sheet = session.workbook.active.title
        session.dirty = True
        self._record_history(
            session,
            user_command="Undo",
            action="undo",
            explanation="Reverted the latest workbook change.",
            target_sheet=session.active_sheet,
            risk_level="low",
            status="system",
        )
        return self.get_snapshot(session_id)

    def redo(self, session_id: str) -> WorkbookSnapshot:
        session = self.get_session(session_id)
        if not session.redo_stack:
            raise HTTPException(status_code=400, detail="Nothing to redo.")
        session.undo_stack.append(self._serialize_workbook(session.workbook))
        next_state = session.redo_stack.pop()
        session.workbook = self._restore_workbook(next_state)
        if session.active_sheet not in session.workbook.sheetnames:
            session.active_sheet = session.workbook.active.title
        session.dirty = True
        self._record_history(
            session,
            user_command="Redo",
            action="redo",
            explanation="Re-applied the latest reverted workbook change.",
            target_sheet=session.active_sheet,
            risk_level="low",
            status="system",
        )
        return self.get_snapshot(session_id)

    def execute_action(self, session_id: str, plan: ActionPlan) -> WorkbookSnapshot:
        session = self.get_session(session_id)
        if plan.action == "noop":
            return self.get_snapshot(session_id)

        worksheet = self._get_sheet(session, plan.target_sheet)
        self._checkpoint(session)

        if plan.action == "insert_formula":
            if not plan.target_cell or not plan.formula:
                raise HTTPException(status_code=400, detail="Formula insertion requires target_cell and formula.")
            worksheet[plan.target_cell] = plan.formula
        elif plan.action == "fill_formula_down":
            self._fill_formula_down(worksheet, plan)
        elif plan.action == "sort":
            self._sort_sheet(worksheet, plan)
        elif plan.action == "delete_duplicates":
            self._delete_duplicates(worksheet, plan)
        elif plan.action == "find_replace":
            self._find_replace(worksheet, plan)
        elif plan.action == "highlight_threshold":
            self._highlight_threshold(worksheet, plan)
        elif plan.action == "apply_filter":
            self._apply_filter(worksheet, plan)
        elif plan.action == "clear_filter":
            self._clear_filter(worksheet)
        elif plan.action == "create_chart":
            self._create_chart(session, worksheet, plan)
        elif plan.action == "convert_column_type":
            self._convert_column_type(worksheet, plan)
        elif plan.action == "create_pivot":
            self._create_pivot(session, worksheet, plan)
        elif plan.action == "add_sheet":
            new_sheet_name = plan.parameters.get("new_sheet_name", "NewSheet")
            if new_sheet_name not in session.workbook.sheetnames:
                session.workbook.create_sheet(new_sheet_name)
        elif plan.action == "join_sheets":
            self._join_sheets(session, worksheet, plan)
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported action '{plan.action}'.")

        session.dirty = True
        self._record_history(
            session,
            user_command=plan.preview_title,
            action=plan.action,
            explanation=plan.explanation,
            target_sheet=plan.target_sheet,
            risk_level=plan.risk_level,
        )
        return self.get_snapshot(session_id)

    def find_first_empty_cell(self, session_id: str, sheet_name: str, column_letter: str) -> str:
        worksheet = self._get_sheet(self.get_session(session_id), sheet_name)
        column_index = column_index_from_string(column_letter)
        for row_idx in range(2, worksheet.max_row + 2):
            cell = worksheet.cell(row=row_idx, column=column_index)
            if cell.value in (None, ""):
                return cell.coordinate
        return f"{column_letter}{worksheet.max_row + 1}"

    def next_available_column_letter(self, session_id: str, sheet_name: str, offset: int = 1) -> str:
        worksheet = self._get_sheet(self.get_session(session_id), sheet_name)
        return get_column_letter(max(1, worksheet.max_column + offset))

    def header_to_column_letter(self, session_id: str, sheet_name: str, header_name: str | None) -> str | None:
        worksheet = self._get_sheet(self.get_session(session_id), sheet_name)
        return self._resolve_column_letter(worksheet, header_name)

    def count_duplicate_rows(self, session_id: str, sheet_name: str, columns: list[str] | None = None) -> int:
        worksheet = self._get_sheet(self.get_session(session_id), sheet_name)
        indices = self._header_indices(worksheet, columns)
        seen = set()
        duplicates = 0
        for row_idx in range(2, worksheet.max_row + 1):
            row = [worksheet.cell(row=row_idx, column=col_idx).value for col_idx in range(1, worksheet.max_column + 1)]
            key = tuple(row[index] for index in indices)
            if key in seen:
                duplicates += 1
            else:
                seen.add(key)
        return duplicates

    def count_find_replace_hits(
        self,
        session_id: str,
        sheet_name: str,
        find_text: str,
        target_column: str | None = None,
    ) -> int:
        worksheet = self._get_sheet(self.get_session(session_id), sheet_name)
        if not find_text:
            return 0
        column_letter = self._resolve_column_letter(worksheet, target_column)
        columns = [column_index_from_string(column_letter)] if column_letter else list(range(1, worksheet.max_column + 1))
        hits = 0
        for row_idx in range(1, worksheet.max_row + 1):
            for column_index in columns:
                value = worksheet.cell(row=row_idx, column=column_index).value
                if isinstance(value, str) and find_text in value:
                    hits += value.count(find_text)
        return hits

    def count_threshold_hits(self, session_id: str, sheet_name: str, target_column: str | None, threshold: float) -> int:
        worksheet = self._get_sheet(self.get_session(session_id), sheet_name)
        column_letter = self._resolve_column_letter(worksheet, target_column)
        if not column_letter:
            return 0
        column_index = column_index_from_string(column_letter)
        hits = 0
        for row_idx in range(2, worksheet.max_row + 1):
            value = worksheet.cell(row=row_idx, column=column_index).value
            number = self._coerce_number(value)
            if number is not None and number > threshold:
                hits += 1
        return hits

    def count_filter_matches(
        self,
        session_id: str,
        sheet_name: str,
        target_column: str | None,
        operator: str,
        criterion: Any,
    ) -> tuple[int, int]:
        worksheet = self._get_sheet(self.get_session(session_id), sheet_name)
        column_letter = self._resolve_column_letter(worksheet, target_column)
        if not column_letter:
            return 0, max(0, worksheet.max_row - 1)
        column_index = column_index_from_string(column_letter)
        matched = 0
        total = max(0, worksheet.max_row - 1)
        for row_idx in range(2, worksheet.max_row + 1):
            value = worksheet.cell(row=row_idx, column=column_index).value
            if self._matches_condition(value, operator, criterion):
                matched += 1
        return matched, total

    def profile_column_conversion(
        self,
        session_id: str,
        sheet_name: str,
        target_column: str | None,
        target_type: str,
    ) -> tuple[int, int]:
        worksheet = self._get_sheet(self.get_session(session_id), sheet_name)
        column_letter = self._resolve_column_letter(worksheet, target_column)
        if not column_letter:
            return 0, max(0, worksheet.max_row - 1)

        column_index = column_index_from_string(column_letter)
        convertible = 0
        skipped = 0
        date_format = "DD-MMM-YYYY" if target_type == "date" else None
        for row_idx in range(2, worksheet.max_row + 1):
            value = worksheet.cell(row=row_idx, column=column_index).value
            if value in (None, ""):
                continue
            success, _ = self._convert_value(value, target_type, date_format)
            if success:
                convertible += 1
            else:
                skipped += 1
        return convertible, skipped

    def find_header_column(self, worksheet: Worksheet, header_name: str | None) -> str | None:
        if not header_name:
            return None
        target = self._normalize(header_name)
        for idx in range(1, worksheet.max_column + 1):
            value = worksheet.cell(row=1, column=idx).value
            if self._normalize(str(value or "")) == target:
                return get_column_letter(idx)
        return None

    def _get_sheet(self, session: WorkbookSession, sheet_name: str) -> Worksheet:
        if sheet_name not in session.workbook.sheetnames:
            raise HTTPException(status_code=404, detail=f"Sheet '{sheet_name}' not found.")
        return session.workbook[sheet_name]

    def _sheet_summary(self, worksheet: Worksheet) -> SheetSummary:
        max_row = worksheet.max_row or 1
        max_column = worksheet.max_column or 1
        headers = [self._stringify(worksheet.cell(row=1, column=idx).value) for idx in range(1, max_column + 1)]
        rows: list[list[Any]] = []
        row_numbers: list[int] = []
        hidden_row_count = 0
        formula_cell_count = 0
        visible_data_row_count = 0
        preview_truncated = False

        for row in worksheet.iter_rows(min_row=1, max_row=max_row, min_col=1, max_col=max_column):
            for cell in row:
                if isinstance(cell.value, str) and cell.value.startswith("="):
                    formula_cell_count += 1

        preview_limit = 10_000
        for row_idx in range(1, max_row + 1):
            if row_idx == 1:
                row_values = [
                    worksheet.cell(row=row_idx, column=col_idx).value for col_idx in range(1, max_column + 1)
                ]
                rows.append(row_values)
                row_numbers.append(row_idx)
                continue
            if worksheet.row_dimensions[row_idx].hidden:
                hidden_row_count += 1
                continue
            visible_data_row_count += 1
            if len(rows) - 1 >= preview_limit:
                preview_truncated = True
                continue
            row_values = [
                worksheet.cell(row=row_idx, column=col_idx).value for col_idx in range(1, max_column + 1)
            ]
            rows.append(row_values)
            row_numbers.append(row_idx)

        return SheetSummary(
            name=worksheet.title,
            headers=headers,
            rows=rows,
            row_numbers=row_numbers,
            max_row=max_row,
            max_column=max_column,
            hidden_row_count=hidden_row_count,
            formula_cell_count=formula_cell_count,
            visible_data_row_count=visible_data_row_count,
            preview_truncated=preview_truncated,
        )

    def _sheet_context(self, worksheet: Worksheet) -> SheetContext:
        max_row = worksheet.max_row or 1
        max_column = worksheet.max_column or 1
        headers = [self._stringify(worksheet.cell(row=1, column=idx).value) for idx in range(1, max_column + 1)]
        data_types: dict[str, str] = {}
        numeric_headers: list[str] = []
        text_headers: list[str] = []
        date_headers: list[str] = []
        sample_rows: list[dict[str, Any]] = []

        for col_idx in range(1, max_column + 1):
            header = headers[col_idx - 1] or get_column_letter(col_idx)
            values = []
            for row_idx in range(2, max_row + 1):
                value = worksheet.cell(row=row_idx, column=col_idx).value
                if value is not None:
                    values.append(value)
                if len(values) >= 10:
                    break
            inferred = self._infer_type(values)
            data_types[header] = inferred
            if inferred == "number":
                numeric_headers.append(header)
            elif inferred == "date":
                date_headers.append(header)
            else:
                text_headers.append(header)

        for row_idx in range(2, max_row + 1):
            if worksheet.row_dimensions[row_idx].hidden:
                continue
            row_data: dict[str, Any] = {}
            for col_idx in range(1, max_column + 1):
                header = headers[col_idx - 1] or get_column_letter(col_idx)
                row_data[header] = worksheet.cell(row=row_idx, column=col_idx).value
            sample_rows.append(row_data)
            if len(sample_rows) >= 5:
                break

        return SheetContext(
            name=worksheet.title,
            row_count=max_row,
            column_count=max_column,
            headers=headers,
            data_types=data_types,
            numeric_headers=numeric_headers,
            text_headers=text_headers,
            date_headers=date_headers,
            sample_rows=sample_rows,
        )

    def _build_stats(
        self,
        workbook: Workbook,
        sheets: list[SheetSummary],
        context: list[SheetContext],
    ) -> WorkbookStats:
        total_non_empty_cells = 0
        for sheet_name in workbook.sheetnames:
            worksheet = workbook[sheet_name]
            for row in worksheet.iter_rows():
                for cell in row:
                    if cell.value not in (None, ""):
                        total_non_empty_cells += 1

        return WorkbookStats(
            sheet_count=len(sheets),
            total_rows=sum(max(0, sheet.max_row - 1) for sheet in sheets),
            total_columns=sum(sheet.max_column for sheet in sheets),
            total_formula_cells=sum(sheet.formula_cell_count for sheet in sheets),
            total_hidden_rows=sum(sheet.hidden_row_count for sheet in sheets),
            total_non_empty_cells=total_non_empty_cells,
            numeric_column_count=sum(len(item.numeric_headers) for item in context),
            text_column_count=sum(len(item.text_headers) for item in context),
        )

    def _build_templates(
        self,
        context: list[SheetContext],
        active_sheet: str,
        sheet_names: list[str],
    ) -> list[CommandTemplate]:
        current = next((item for item in context if item.name == active_sheet), context[0] if context else None)
        numeric_header = current.numeric_headers[0] if current and current.numeric_headers else "Sales"
        text_header = current.text_headers[0] if current and current.text_headers else "Status"
        templates = [
            CommandTemplate(
                title="Quick Total",
                prompt=f"{numeric_header} column ka total nikalo",
                description="Insert a total formula in the next available column.",
                category="Formula",
            ),
            CommandTemplate(
                title="Trend Chart",
                prompt=f"{numeric_header} ka bar chart banao",
                description="Create a chart directly from the active sheet data.",
                category="Visualization",
            ),
            CommandTemplate(
                title="Clean Duplicates",
                prompt=f"{text_header} ke basis par duplicate rows remove karo",
                description="Preview and remove duplicate records safely.",
                category="Cleanup",
            ),
            CommandTemplate(
                title="Filter Focus",
                prompt=f"{numeric_header} > 5000 rows filter karo",
                description="Hide rows that do not match the current focus criteria.",
                category="Filter",
            ),
            CommandTemplate(
                title="Convert Type",
                prompt=f"{text_header} ko number me convert karo",
                description="Normalize a column into a cleaner data type.",
                category="Cleanup",
            ),
        ]
        if len(sheet_names) > 1:
            templates.append(
                CommandTemplate(
                    title="Cross Sheet Lookup",
                    prompt=f"{sheet_names[1]} se VLOOKUP karke data lao",
                    description="Fill a lookup formula using another sheet as the source.",
                    category="Lookup",
                )
            )
        return templates

    def _build_suggested_prompts(
        self,
        context: list[SheetContext],
        active_sheet: str,
        sheet_names: list[str],
    ) -> list[str]:
        current = next((item for item in context if item.name == active_sheet), context[0] if context else None)
        if current is None:
            return []
        numeric_header = current.numeric_headers[0] if current.numeric_headers else "Sales"
        text_header = current.text_headers[0] if current.text_headers else "Name"
        prompts = [
            f"{numeric_header} column ka total nikalo",
            f"{numeric_header} > 5000 ko green highlight karo",
            f"{text_header} me replace old with new karo",
            f"{numeric_header} descending sort karo",
            f"{numeric_header} > 5000 rows filter karo",
            f"{numeric_header} ka chart banao",
            f"{text_header} contains urgent rows filter karo",
            f"{text_header} ko number me convert karo",
            f"{numeric_header} ka total where {text_header} is closed nikalo",
        ]
        if len(sheet_names) > 1:
            prompts.append(f"{sheet_names[1]} se VLOOKUP karke match karo")
        return prompts

    def _fill_formula_down(self, worksheet: Worksheet, plan: ActionPlan) -> None:
        target_column = self._resolve_column_letter(worksheet, plan.target_column) or plan.parameters.get("target_column_letter")
        formula_template = plan.formula
        anchor_cell = str(plan.parameters.get("anchor_cell", "") or plan.parameters.get("source_cell", "")).strip().upper()
        source_value = formula_template
        if not source_value and anchor_cell:
            source_value = worksheet[anchor_cell].value
        if not target_column and anchor_cell:
            target_column = self._coordinate_parts(anchor_cell)[0]
        if not target_column or source_value in (None, ""):
            raise HTTPException(status_code=400, detail="Fill formula requires a target column and a formula template.")

        start_row = int(plan.parameters.get("start_row", 2))
        end_row = int(plan.parameters.get("end_row", worksheet.max_row))
        for row_idx in range(start_row, end_row + 1):
            target_cell = f"{target_column}{row_idx}"
            if isinstance(source_value, str) and source_value.startswith("="):
                if "{row}" in source_value:
                    worksheet[target_cell] = source_value.replace("{row}", str(row_idx))
                elif anchor_cell:
                    worksheet[target_cell] = Translator(source_value, origin=anchor_cell).translate_formula(target_cell)
                else:
                    worksheet[target_cell] = source_value
            else:
                worksheet[target_cell] = source_value

    def _sort_sheet(self, worksheet: Worksheet, plan: ActionPlan) -> None:
        target_column = self._resolve_column_letter(worksheet, plan.target_column)
        if not target_column:
            raise HTTPException(status_code=400, detail="Sort action needs a valid target column.")

        sort_index = column_index_from_string(target_column) - 1
        descending = bool(plan.parameters.get("descending", False))
        data_rows = []
        for row_idx in range(2, worksheet.max_row + 1):
            values = [worksheet.cell(row=row_idx, column=col_idx).value for col_idx in range(1, worksheet.max_column + 1)]
            data_rows.append(values)

        data_rows.sort(
            key=lambda row: (row[sort_index] is None, str(row[sort_index]).lower() if row[sort_index] is not None else ""),
            reverse=descending,
        )

        for row_offset, row_values in enumerate(data_rows, start=2):
            worksheet.row_dimensions[row_offset].hidden = False
            for col_idx, value in enumerate(row_values, start=1):
                worksheet.cell(row=row_offset, column=col_idx).value = value

    def _delete_duplicates(self, worksheet: Worksheet, plan: ActionPlan) -> None:
        indices = self._header_indices(worksheet, plan.parameters.get("columns") or ([plan.target_column] if plan.target_column else []))
        seen = set()
        unique_rows: list[list[Any]] = []
        for row_idx in range(2, worksheet.max_row + 1):
            row = [worksheet.cell(row=row_idx, column=col_idx).value for col_idx in range(1, worksheet.max_column + 1)]
            key = tuple(row[index] for index in indices)
            if key in seen:
                continue
            seen.add(key)
            unique_rows.append(row)

        original_max_row = worksheet.max_row
        for row_idx in range(2, original_max_row + 1):
            worksheet.row_dimensions[row_idx].hidden = False
            for col_idx in range(1, worksheet.max_column + 1):
                worksheet.cell(row=row_idx, column=col_idx).value = None

        for row_offset, row_values in enumerate(unique_rows, start=2):
            for col_idx, value in enumerate(row_values, start=1):
                worksheet.cell(row=row_offset, column=col_idx).value = value

    def _find_replace(self, worksheet: Worksheet, plan: ActionPlan) -> None:
        find_text = str(plan.parameters.get("find", ""))
        replace_text = str(plan.parameters.get("replace", ""))
        if not find_text:
            raise HTTPException(status_code=400, detail="Find/replace requires a non-empty find string.")

        column_letter = self._resolve_column_letter(worksheet, plan.target_column)
        columns = [column_index_from_string(column_letter)] if column_letter else list(range(1, worksheet.max_column + 1))
        for row_idx in range(1, worksheet.max_row + 1):
            for column_index in columns:
                cell = worksheet.cell(row=row_idx, column=column_index)
                if isinstance(cell.value, str) and find_text in cell.value:
                    cell.value = cell.value.replace(find_text, replace_text)

    def _highlight_threshold(self, worksheet: Worksheet, plan: ActionPlan) -> None:
        target_column = self._resolve_column_letter(worksheet, plan.target_column)
        threshold = plan.parameters.get("threshold")
        color = str(plan.parameters.get("color", "C7F9CC")).replace("#", "")
        if not target_column or threshold is None:
            raise HTTPException(status_code=400, detail="Highlight action needs a target column and threshold.")

        fill = PatternFill(start_color=color, end_color=color, fill_type="solid")
        cell_range = f"{target_column}2:{target_column}{worksheet.max_row}"
        worksheet.conditional_formatting.add(
            cell_range,
            CellIsRule(operator="greaterThan", formula=[str(threshold)], fill=fill),
        )

    def _apply_filter(self, worksheet: Worksheet, plan: ActionPlan) -> None:
        target_column = self._resolve_column_letter(worksheet, plan.target_column)
        operator = str(plan.parameters.get("operator", "greater_than"))
        criterion = plan.parameters.get("value")
        if not target_column:
            raise HTTPException(status_code=400, detail="Filter action needs a target column.")

        column_index = column_index_from_string(target_column)
        worksheet.auto_filter.ref = f"A1:{get_column_letter(worksheet.max_column)}{worksheet.max_row}"
        for row_idx in range(2, worksheet.max_row + 1):
            value = worksheet.cell(row=row_idx, column=column_index).value
            worksheet.row_dimensions[row_idx].hidden = not self._matches_condition(value, operator, criterion)

    def _clear_filter(self, worksheet: Worksheet) -> None:
        worksheet.auto_filter.ref = ""
        for row_idx in range(2, worksheet.max_row + 1):
            worksheet.row_dimensions[row_idx].hidden = False

    def _create_chart(self, session: WorkbookSession, worksheet: Worksheet, plan: ActionPlan) -> None:
        value_column = self._resolve_column_letter(worksheet, plan.target_column)
        if not value_column:
            raise HTTPException(status_code=400, detail="Chart creation needs a numeric target column.")

        category_column = self._resolve_column_letter(worksheet, plan.parameters.get("category_column")) or "A"
        chart_type = str(plan.parameters.get("chart_type", "bar")).lower()
        target_cell = plan.target_cell or f"{get_column_letter(worksheet.max_column + 2)}2"
        output_sheet_name = str(plan.parameters.get("output_sheet", worksheet.title))
        chart_sheet = session.workbook[output_sheet_name] if output_sheet_name in session.workbook.sheetnames else session.workbook.create_sheet(output_sheet_name)

        if chart_type == "line":
            chart = LineChart()
        elif chart_type == "pie":
            chart = PieChart()
        else:
            chart = BarChart()

        chart.title = str(plan.parameters.get("title", f"{plan.target_column or value_column} Analysis"))
        chart.y_axis.title = str(plan.parameters.get("y_title", plan.target_column or value_column))
        chart.x_axis.title = str(plan.parameters.get("x_title", category_column))

        data = Reference(
            worksheet,
            min_col=column_index_from_string(value_column),
            min_row=1,
            max_row=max(2, worksheet.max_row),
        )
        categories = Reference(
            worksheet,
            min_col=column_index_from_string(category_column),
            min_row=2,
            max_row=max(2, worksheet.max_row),
        )
        chart.add_data(data, titles_from_data=True)
        chart.set_categories(categories)
        chart.height = 8
        chart.width = 16
        chart_sheet.add_chart(chart, target_cell)

    def _convert_column_type(self, worksheet: Worksheet, plan: ActionPlan) -> None:
        target_column = self._resolve_column_letter(worksheet, plan.target_column)
        target_type = str(plan.parameters.get("target_type", "")).strip().lower()
        date_format = str(plan.parameters.get("date_format", "DD-MMM-YYYY")).strip() or "DD-MMM-YYYY"
        clear_invalid = bool(plan.parameters.get("clear_invalid", False))
        if not target_column or target_type not in {"number", "text", "date"}:
            raise HTTPException(status_code=400, detail="Column conversion needs a valid target column and target type.")

        column_index = column_index_from_string(target_column)
        for row_idx in range(2, worksheet.max_row + 1):
            cell = worksheet.cell(row=row_idx, column=column_index)
            if cell.value in (None, ""):
                continue
            success, converted = self._convert_value(cell.value, target_type, date_format)
            if success:
                cell.value = converted
                if target_type == "date":
                    cell.number_format = date_format
            elif clear_invalid:
                cell.value = None

    def _create_pivot(self, session: WorkbookSession, worksheet: Worksheet, plan: ActionPlan) -> None:
        group_col_name = plan.parameters.get("group_by")
        val_col_name = plan.parameters.get("value_col")
        group_letter = self._resolve_column_letter(worksheet, group_col_name)
        val_letter = self._resolve_column_letter(worksheet, val_col_name)

        if not group_letter or not val_letter:
            raise HTTPException(status_code=400, detail="Pivot table needs group_by and value columns.")

        group_idx = column_index_from_string(group_letter)
        val_idx = column_index_from_string(val_letter)

        summary = {}
        for row_idx in range(2, worksheet.max_row + 1):
            if worksheet.row_dimensions[row_idx].hidden:
                continue
            group_val = worksheet.cell(row=row_idx, column=group_idx).value
            val_cell = worksheet.cell(row=row_idx, column=val_idx).value

            group_key = str(group_val) if group_val is not None else "Unknown"
            num_val = self._coerce_number(val_cell) or 0.0

            summary[group_key] = summary.get(group_key, 0.0) + num_val

        pivot_sheet_name = f"Pivot_{worksheet.title}"[:31]  # Excel limits sheet names to 31 chars
        if pivot_sheet_name in session.workbook.sheetnames:
            pivot_sheet = session.workbook[pivot_sheet_name]
            pivot_sheet.delete_rows(1, pivot_sheet.max_row)
        else:
            pivot_sheet = session.workbook.create_sheet(pivot_sheet_name)

        pivot_sheet.cell(row=1, column=1).value = group_col_name or "Group"
        pivot_sheet.cell(row=1, column=2).value = f"Sum of {val_col_name or 'Value'}"

        row_offset = 2
        for k, v in summary.items():
            pivot_sheet.cell(row=row_offset, column=1).value = k
            pivot_sheet.cell(row=row_offset, column=2).value = v
            row_offset += 1

    def _join_sheets(self, session: WorkbookSession, primary_sheet: Worksheet, plan: ActionPlan) -> None:
        secondary_sheet_name = plan.parameters.get("secondary_sheet")
        join_column_name = plan.parameters.get("join_column")
        if not secondary_sheet_name or secondary_sheet_name not in session.workbook.sheetnames:
            raise HTTPException(status_code=400, detail="Secondary sheet not found for join.")

        secondary_sheet = session.workbook[secondary_sheet_name]
        primary_col_letter = self._resolve_column_letter(primary_sheet, join_column_name)
        secondary_col_letter = self._resolve_column_letter(secondary_sheet, join_column_name)

        if not primary_col_letter or not secondary_col_letter:
            raise HTTPException(status_code=400, detail=f"Join column '{join_column_name}' not found in both sheets.")

        primary_col_idx = column_index_from_string(primary_col_letter)
        secondary_col_idx = column_index_from_string(secondary_col_letter)

        secondary_map = {}
        sec_max_col = secondary_sheet.max_column
        for row_idx in range(2, secondary_sheet.max_row + 1):
            key_val = secondary_sheet.cell(row=row_idx, column=secondary_col_idx).value
            if key_val is None:
                continue
            key_str = str(key_val).strip().lower()
            row_data = [secondary_sheet.cell(row=row_idx, column=c).value for c in range(1, sec_max_col + 1)]
            secondary_map[key_str] = row_data

        start_new_col = primary_sheet.max_column + 1
        current_new_col = start_new_col
        for c in range(1, sec_max_col + 1):
            if c != secondary_col_idx:
                header_val = secondary_sheet.cell(row=1, column=c).value
                primary_sheet.cell(row=1, column=current_new_col).value = f"{secondary_sheet_name}_{header_val}"
                current_new_col += 1

        for row_idx in range(2, primary_sheet.max_row + 1):
            key_val = primary_sheet.cell(row=row_idx, column=primary_col_idx).value
            if key_val is None:
                continue
            key_str = str(key_val).strip().lower()
            if key_str in secondary_map:
                row_data = secondary_map[key_str]
                current_new_col = start_new_col
                for c in range(1, sec_max_col + 1):
                    if c != secondary_col_idx:
                        primary_sheet.cell(row=row_idx, column=current_new_col).value = row_data[c-1]
                        current_new_col += 1

    def _checkpoint(self, session: WorkbookSession) -> None:
        session.undo_stack.append(self._serialize_workbook(session.workbook))
        if len(session.undo_stack) > 30:
            session.undo_stack.pop(0)
        session.redo_stack.clear()

    def _record_history(
        self,
        session: WorkbookSession,
        user_command: str,
        action: str,
        explanation: str,
        target_sheet: str,
        risk_level: RiskLevel,
        status: str = "completed",
    ) -> None:
        session.command_history.insert(
            0,
            CommandRecord(
                record_id=str(uuid4())[:8],
                user_command=user_command,
                action=action,
                explanation=explanation,
                target_sheet=target_sheet,
                status=status,
                created_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
                risk_level=risk_level,
            ),
        )
        del session.command_history[30:]

    def _serialize_workbook(self, workbook: Workbook) -> bytes:
        stream = BytesIO()
        workbook.save(stream)
        return stream.getvalue()

    def _restore_workbook(self, payload: bytes) -> Workbook:
        return load_workbook(BytesIO(payload))

    def _header_indices(self, worksheet: Worksheet, headers: list[str] | None) -> list[int]:
        if not headers:
            return list(range(worksheet.max_column))
        indices: list[int] = []
        for header in headers:
            column_letter = self._resolve_column_letter(worksheet, header)
            if column_letter:
                indices.append(column_index_from_string(column_letter) - 1)
        return indices or list(range(worksheet.max_column))

    def _resolve_column_letter(self, worksheet: Worksheet, header_or_letter: str | None) -> str | None:
        if not header_or_letter:
            return None
        candidate = header_or_letter.strip()
        if re.fullmatch(r"[A-Za-z]{1,3}", candidate):
            return candidate.upper()
        return self.find_header_column(worksheet, candidate)

    @staticmethod
    def _coordinate_parts(cell_reference: str) -> tuple[str | None, int | None]:
        match = re.fullmatch(r"([A-Za-z]{1,3})(\d+)", cell_reference.strip())
        if not match:
            return None, None
        return match.group(1).upper(), int(match.group(2))

    def _matches_condition(self, value: Any, operator: str, criterion: Any) -> bool:
        normalized_operator = operator.lower()
        if normalized_operator in {"contains", "includes"}:
            return criterion is not None and criterion != "" and str(criterion).lower() in str(value or "").lower()

        value_number = self._coerce_number(value)
        criterion_number = self._coerce_number(criterion)
        if value_number is not None and criterion_number is not None:
            if normalized_operator in {"gt", "greater_than", ">"}:
                return value_number > criterion_number
            if normalized_operator in {"gte", "greater_or_equal", ">="}:
                return value_number >= criterion_number
            if normalized_operator in {"lt", "less_than", "<"}:
                return value_number < criterion_number
            if normalized_operator in {"lte", "less_or_equal", "<="}:
                return value_number <= criterion_number
            if normalized_operator in {"eq", "equals", "="}:
                return value_number == criterion_number
            if normalized_operator in {"neq", "not_equals", "!="}:
                return value_number != criterion_number

        # Handle Date Comparison
        if hasattr(value, "strftime") or (isinstance(criterion, str) and re.match(r"\d{4}-\d{2}-\d{2}", criterion)):
            value_str = value.strftime("%Y-%m-%d") if hasattr(value, "strftime") else str(value or "")
            if normalized_operator in {"eq", "equals", "="}:
                return value_str == str(criterion)
            if normalized_operator in {"neq", "not_equals", "!="}:
                return value_str != str(criterion)

        value_text = str(value or "").strip().lower()
        criterion_text = str(criterion or "").strip().lower()
        if normalized_operator in {"neq", "not_equals", "!="}:
            return value_text != criterion_text
        return value_text == criterion_text

    def _validate_path(self, path: Path) -> None:
        allowed_root = settings.allowed_root_path
        if allowed_root and allowed_root not in path.parents and path != allowed_root:
            raise HTTPException(status_code=403, detail="Workbook path is outside the allowed root.")

    @staticmethod
    def _coerce_number(value: Any) -> float | None:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return float(value)
        if isinstance(value, str):
            try:
                return float(value.replace(",", "").strip())
            except ValueError:
                return None
        return None

    @staticmethod
    def _infer_type(values: list[Any]) -> str:
        if not values:
            return "empty"
        if all(isinstance(value, (int, float)) and not isinstance(value, bool) for value in values):
            return "number"
        if all(hasattr(value, "year") and hasattr(value, "month") for value in values):
            return "date"
        return "text"

    @classmethod
    def _convert_value(cls, value: Any, target_type: str, date_format: str | None = None) -> tuple[bool, Any]:
        normalized_target = target_type.strip().lower()
        if normalized_target == "text":
            return True, cls._stringify(value)
        if normalized_target == "number":
            number = cls._coerce_number(value)
            if number is None:
                return False, value
            if number.is_integer():
                return True, int(number)
            return True, number
        if normalized_target == "date":
            parsed = cls._parse_date_value(value)
            if parsed is None:
                return False, value
            return True, parsed
        return False, value

    @staticmethod
    def _parse_date_value(value: Any) -> datetime | date | None:
        if isinstance(value, datetime):
            return value
        if isinstance(value, date):
            return value
        if isinstance(value, str):
            candidate = value.strip()
            if not candidate:
                return None
            for pattern in (
                "%Y-%m-%d",
                "%d-%m-%Y",
                "%d/%m/%Y",
                "%m/%d/%Y",
                "%d %b %Y",
                "%d %B %Y",
                "%Y/%m/%d",
            ):
                try:
                    return datetime.strptime(candidate, pattern)
                except ValueError:
                    continue
            try:
                return datetime.fromisoformat(candidate)
            except ValueError:
                return None
        return None

    @staticmethod
    def _stringify(value: Any) -> str:
        return "" if value is None else str(value)

    @staticmethod
    def _normalize(text: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", text.lower())


workbook_service = WorkbookService()
