from __future__ import annotations

import json
import re
from typing import Any

import httpx
from fastapi import HTTPException

from app.config import settings
from app.models import ActionImpact, ActionPlan, WorkbookSnapshot
from app.services.workbook_service import workbook_service


class AIService:
    async def preview_command(self, session_id: str, command: str) -> ActionPlan:
        snapshot = workbook_service.get_snapshot(session_id)
        heuristic_plan = self._preview_with_heuristics(command, snapshot)
        if settings.gemini_api_key:
            try:
                gemini_plan = await self._preview_with_gemini(command, snapshot)
                if gemini_plan.action == "noop" and heuristic_plan.action != "noop":
                    return heuristic_plan
                return gemini_plan
            except Exception as e:
                import sys
                import traceback
                print(f"Gemini API Error: {repr(e)}", file=sys.stderr)
                traceback.print_exc(file=sys.stderr)
                if heuristic_plan.action == "noop":
                    heuristic_plan.explanation = f"API Error: {repr(e)}. Original heuristic message: {heuristic_plan.explanation}"
                return heuristic_plan
        return heuristic_plan

    async def _preview_with_gemini(self, command: str, snapshot: WorkbookSnapshot) -> ActionPlan:
        prompt = self._build_prompt(command, snapshot)
        url = (
            "https://generativelanguage.googleapis.com/v1beta/models/"
            f"{settings.gemini_model}:generateContent?key={settings.gemini_api_key}"
        )
        payload = {
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": 0.1, "responseMimeType": "application/json"},
        }
        async with httpx.AsyncClient(timeout=25.0) as client:
            response = await client.post(url, json=payload)
        response.raise_for_status()
        data = response.json()
        candidates = data.get("candidates") or []
        if not candidates:
            raise HTTPException(status_code=502, detail="Gemini returned no candidates.")
        text = candidates[0]["content"]["parts"][0].get("text", "")
        text = text.strip()
        if text.startswith("```json"):
            text = text[7:]
        if text.startswith("```"):
            text = text[3:]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()
        return ActionPlan.model_validate(json.loads(text))

    def _preview_with_heuristics(self, command: str, snapshot: WorkbookSnapshot) -> ActionPlan:
        command_lower = command.lower().strip()
        target_sheet = self._find_target_sheet(command_lower, snapshot)
        sheet = self._sheet_by_name(snapshot, target_sheet)
        matched_headers = self._find_matching_headers(command_lower, sheet["headers"])
        matched_header = self._find_header_in_command(command_lower, sheet["headers"])
        numeric_header = self._pick_numeric_header(sheet, matched_header)
        text_header = self._pick_text_header(sheet, matched_header)

        if any(token in command_lower for token in ["clear filter", "reset filter", "show all"]):
            return ActionPlan(
                action="clear_filter",
                target_sheet=target_sheet,
                preview_title="Clear active filter",
                explanation=f"I will unhide all rows and clear the active filter on {target_sheet}.",
                risk_level="low",
                requires_confirmation=False,
                impacted_range=f"A1:{self._col_letter(sheet['max_column'])}{sheet['max_row']}",
                parameters={},
                impact=ActionImpact(
                    summary=f"All {max(0, sheet['max_row'] - 1)} data rows will become visible again.",
                    estimated_rows=max(0, sheet["max_row"] - 1),
                    estimated_cells=max(0, sheet["max_row"] - 1) * max(1, sheet["max_column"]),
                ),
            )

        if any(
            token in command_lower
            for token in ["convert", "as number", "as text", "as date", "date format", "number format", "numeric"]
        ):
            target_type = self._extract_target_type(command_lower)
            conversion_header = matched_header or text_header or numeric_header
            if target_type and conversion_header:
                convertible, skipped = workbook_service.profile_column_conversion(
                    snapshot.session_id,
                    target_sheet,
                    conversion_header,
                    target_type,
                )
                return ActionPlan(
                    action="convert_column_type",
                    target_sheet=target_sheet,
                    target_column=conversion_header,
                    preview_title="Convert column type",
                    explanation=f"I will convert {conversion_header} on {target_sheet} into {target_type} values where possible.",
                    risk_level="medium" if skipped else "low",
                    requires_confirmation=bool(skipped),
                    impacted_range=f"{workbook_service.header_to_column_letter(snapshot.session_id, target_sheet, conversion_header)}2:{workbook_service.header_to_column_letter(snapshot.session_id, target_sheet, conversion_header)}{sheet['max_row']}",
                    parameters={
                        "target_type": target_type,
                        "date_format": "DD-MMM-YYYY" if target_type == "date" else "",
                    },
                    impact=ActionImpact(
                        summary=f"{convertible} cells can be converted and {skipped} may stay unchanged.",
                        estimated_rows=convertible,
                        estimated_cells=convertible,
                        warnings=(
                            ["Some cells do not match the requested type and will stay as-is."]
                            if skipped
                            else []
                        ),
                    ),
                )

        if any(token in command_lower for token in ["duplicate", "dedup"]):
            duplicate_count = workbook_service.count_duplicate_rows(
                snapshot.session_id,
                target_sheet,
                [matched_header] if matched_header else None,
            )
            return ActionPlan(
                action="delete_duplicates",
                target_sheet=target_sheet,
                target_column=matched_header,
                preview_title="Remove duplicate rows",
                explanation=f"I will remove duplicate rows from {target_sheet} using {matched_header or 'the full row'} as the uniqueness key.",
                risk_level="high",
                requires_confirmation=True,
                impacted_range=f"A2:{self._col_letter(sheet['max_column'])}{sheet['max_row']}",
                parameters={"columns": [matched_header] if matched_header else []},
                impact=ActionImpact(
                    summary=f"{duplicate_count} duplicate rows are likely to be removed.",
                    estimated_rows=duplicate_count,
                    estimated_cells=duplicate_count * max(1, sheet["max_column"]),
                    warnings=["Duplicate removal rewrites the visible table and should be confirmed before applying."],
                ),
            )

        if "replace" in command_lower and "with" in command_lower:
            match = re.search(r"replace\s+(.+?)\s+with\s+(.+)", command_lower)
            if match:
                find_text = match.group(1).strip()
                replace_text = match.group(2).strip()
                hits = workbook_service.count_find_replace_hits(snapshot.session_id, target_sheet, find_text, matched_header)
                return ActionPlan(
                    action="find_replace",
                    target_sheet=target_sheet,
                    target_column=matched_header,
                    preview_title="Find and replace values",
                    explanation=f"I will replace '{find_text}' with '{replace_text}' on {target_sheet}.",
                    risk_level="medium",
                    requires_confirmation=True,
                    impacted_range=f"A1:{self._col_letter(sheet['max_column'])}{sheet['max_row']}",
                    parameters={"find": find_text, "replace": replace_text},
                    impact=ActionImpact(
                        summary=f"About {hits} text matches will be updated.",
                        estimated_rows=hits,
                        estimated_cells=hits,
                        warnings=["Text replacements are applied directly to matching cells."],
                    ),
                )

        if any(token in command_lower for token in ["sort", "ascending", "descending"]):
            descending = "desc" in command_lower or "descending" in command_lower or "z to a" in command_lower
            sort_header = matched_header or sheet["headers"][0]
            return ActionPlan(
                action="sort",
                target_sheet=target_sheet,
                target_column=sort_header,
                preview_title="Sort sheet",
                explanation=f"I will sort {target_sheet} by {sort_header} in {'descending' if descending else 'ascending'} order.",
                risk_level="medium",
                requires_confirmation=False,
                impacted_range=f"A2:{self._col_letter(sheet['max_column'])}{sheet['max_row']}",
                parameters={"descending": descending},
                impact=ActionImpact(
                    summary=f"{max(0, sheet['max_row'] - 1)} rows will be reordered.",
                    estimated_rows=max(0, sheet["max_row"] - 1),
                    estimated_cells=max(0, sheet["max_row"] - 1) * max(1, sheet["max_column"]),
                ),
            )

        if any(token in command_lower for token in ["filter", "show only"]) or (
            "where" in command_lower
            and not any(token in command_lower for token in ["sum", "total", "average", "avg", "count", "sumif", "averageif", "avgif", "countif"])
        ):
            filter_header = numeric_header or matched_header or text_header
            operator, criterion = self._extract_filter_criterion(command_lower)
            matched, total = workbook_service.count_filter_matches(snapshot.session_id, target_sheet, filter_header, operator, criterion)
            return ActionPlan(
                action="apply_filter",
                target_sheet=target_sheet,
                target_column=filter_header,
                preview_title="Apply smart filter",
                explanation=f"I will filter {target_sheet} so only rows matching {filter_header} {operator} {criterion} remain visible.",
                risk_level="medium",
                requires_confirmation=False,
                impacted_range=f"A1:{self._col_letter(sheet['max_column'])}{sheet['max_row']}",
                parameters={"operator": operator, "value": criterion},
                impact=ActionImpact(
                    summary=f"{matched} of {total} rows will remain visible after filtering.",
                    estimated_rows=matched,
                    estimated_cells=matched * max(1, sheet["max_column"]),
                    warnings=["Filtered rows are hidden, not deleted."],
                ),
            )

        if any(token in command_lower for token in ["chart", "graph"]):
            chart_header = numeric_header
            if chart_header is None:
                return self._unsupported_plan(target_sheet)
            chart_type = "line" if "line" in command_lower else "pie" if "pie" in command_lower else "bar"
            target_cell = f"{workbook_service.next_available_column_letter(snapshot.session_id, target_sheet, 2)}2"
            category_header = text_header or sheet["headers"][0]
            return ActionPlan(
                action="create_chart",
                target_sheet=target_sheet,
                target_cell=target_cell,
                target_column=chart_header,
                preview_title="Create chart",
                explanation=f"I will create a {chart_type} chart for {chart_header} on {target_sheet}.",
                risk_level="low",
                requires_confirmation=False,
                impacted_range=target_cell,
                parameters={
                    "chart_type": chart_type,
                    "category_column": category_header,
                    "output_sheet": f"{target_sheet}_Insights",
                    "title": f"{chart_header} {chart_type.title()} Chart",
                },
                impact=ActionImpact(
                    summary=f"A new chart will be added using {max(0, sheet['max_row'] - 1)} data rows.",
                    estimated_rows=max(0, sheet["max_row"] - 1),
                    estimated_cells=max(0, sheet["max_row"] - 1),
                ),
            )

        threshold_match = re.search(r"(?:>=|>|above|greater than)\s*(\d+(?:\.\d+)?)", command_lower)
        if any(token in command_lower for token in ["highlight", "green", "format"]) and threshold_match and numeric_header:
            threshold = float(threshold_match.group(1))
            hits = workbook_service.count_threshold_hits(snapshot.session_id, target_sheet, numeric_header, threshold)
            column_letter = workbook_service.header_to_column_letter(snapshot.session_id, target_sheet, numeric_header)
            return ActionPlan(
                action="highlight_threshold",
                target_sheet=target_sheet,
                target_column=numeric_header,
                preview_title="Highlight important values",
                explanation=f"I will highlight {numeric_header} values greater than {threshold} on {target_sheet}.",
                risk_level="low",
                requires_confirmation=False,
                impacted_range=f"{column_letter}2:{column_letter}{sheet['max_row']}",
                parameters={"threshold": threshold, "color": "C7F9CC"},
                impact=ActionImpact(
                    summary=f"{hits} cells are expected to match the threshold rule.",
                    estimated_rows=hits,
                    estimated_cells=hits,
                ),
            )

        if any(token in command_lower for token in ["profit", "net profit", "difference", "minus", "subtract", "revenue minus", "income minus", "collection minus"]):
            difference_plan = self._build_difference_plan(command_lower, snapshot, target_sheet, sheet, matched_headers)
            if difference_plan is not None:
                return difference_plan

        if (
            any(token in command_lower for token in ["sumif", "averageif", "avgif", "countif"])
            or any(token in command_lower for token in [" where ", " jahan ", " when ", " contains ", " includes "])
        ):
            operator, criterion = self._extract_filter_criterion(command_lower)
            formula_kind = "count" if any(token in command_lower for token in ["countif", "count "]) else (
                "average" if any(token in command_lower for token in ["averageif", "avgif", "average", "avg"]) else "sum"
            )
            value_header = next(
                (header for header in matched_headers if header in sheet.get("numeric_headers", [])),
                numeric_header,
            )
            condition_header = self._pick_condition_header(sheet, matched_headers, value_header, operator)
            condition_letter = workbook_service.header_to_column_letter(snapshot.session_id, target_sheet, condition_header)
            if condition_letter is None:
                return self._unsupported_plan(target_sheet)
            value_letter = (
                workbook_service.header_to_column_letter(snapshot.session_id, target_sheet, value_header)
                if value_header
                else None
            )
            if formula_kind in {"sum", "average"} and value_letter is None:
                return self._unsupported_plan(target_sheet)

            target_column_letter = workbook_service.next_available_column_letter(snapshot.session_id, target_sheet)
            target_cell = workbook_service.find_first_empty_cell(snapshot.session_id, target_sheet, target_column_letter)
            criteria = self._excel_criteria(operator, criterion).replace('"', '""')
            condition_range = f"{condition_letter}2:{condition_letter}{sheet['max_row']}"
            if formula_kind == "count":
                formula = f'=COUNTIF({condition_range},"{criteria}")'
                preview_title = "Insert COUNTIF formula"
            elif formula_kind == "average":
                formula = f'=AVERAGEIF({condition_range},"{criteria}",{value_letter}2:{value_letter}{sheet["max_row"]})'
                preview_title = "Insert AVERAGEIF formula"
            else:
                formula = f'=SUMIF({condition_range},"{criteria}",{value_letter}2:{value_letter}{sheet["max_row"]})'
                preview_title = "Insert SUMIF formula"
            return ActionPlan(
                action="insert_formula",
                target_sheet=target_sheet,
                target_cell=target_cell,
                target_column=value_header or condition_header,
                formula=formula,
                preview_title=preview_title,
                explanation=(
                    f"I will insert a {formula_kind.upper()} formula for {target_sheet} using "
                    f"{condition_header} {operator} {criterion}."
                ),
                risk_level="low",
                requires_confirmation=False,
                impacted_range=target_cell,
                parameters={},
                impact=ActionImpact(summary="One conditional formula cell will be inserted.", estimated_rows=1, estimated_cells=1),
            )

        if any(token in command_lower for token in ["sum", "total"]):
            if numeric_header is None:
                return self._unsupported_plan(target_sheet)
            column_letter = workbook_service.header_to_column_letter(snapshot.session_id, target_sheet, numeric_header)
            target_column_letter = workbook_service.next_available_column_letter(snapshot.session_id, target_sheet)
            target_cell = workbook_service.find_first_empty_cell(snapshot.session_id, target_sheet, target_column_letter)
            return ActionPlan(
                action="insert_formula",
                target_sheet=target_sheet,
                target_cell=target_cell,
                target_column=numeric_header,
                formula=f"=SUM({column_letter}2:{column_letter}{sheet['max_row']})",
                preview_title="Insert total formula",
                explanation=f"I will insert a SUM formula for {numeric_header} into {target_cell}.",
                risk_level="low",
                requires_confirmation=False,
                impacted_range=target_cell,
                parameters={},
                impact=ActionImpact(summary="One formula cell will be inserted.", estimated_rows=1, estimated_cells=1),
            )

        if "average" in command_lower or "avg" in command_lower:
            if numeric_header is None:
                return self._unsupported_plan(target_sheet)
            column_letter = workbook_service.header_to_column_letter(snapshot.session_id, target_sheet, numeric_header)
            target_column_letter = workbook_service.next_available_column_letter(snapshot.session_id, target_sheet)
            target_cell = workbook_service.find_first_empty_cell(snapshot.session_id, target_sheet, target_column_letter)
            return ActionPlan(
                action="insert_formula",
                target_sheet=target_sheet,
                target_cell=target_cell,
                target_column=numeric_header,
                formula=f"=AVERAGE({column_letter}2:{column_letter}{sheet['max_row']})",
                preview_title="Insert average formula",
                explanation=f"I will insert an AVERAGE formula for {numeric_header} into {target_cell}.",
                risk_level="low",
                requires_confirmation=False,
                impacted_range=target_cell,
                parameters={},
                impact=ActionImpact(summary="One formula cell will be inserted.", estimated_rows=1, estimated_cells=1),
            )

        if "vlookup" in command_lower and len(snapshot.sheets) > 1:
            source_sheet = next(sheet_item for sheet_item in snapshot.sheets if sheet_item.name != target_sheet)
            target_column_letter = workbook_service.next_available_column_letter(snapshot.session_id, target_sheet)
            return ActionPlan(
                action="fill_formula_down",
                target_sheet=target_sheet,
                target_column=target_column_letter,
                formula=f'=IFERROR(VLOOKUP(A{{row}},\'{source_sheet.name}\'!A:B,2,FALSE),"")',
                preview_title="Fill VLOOKUP column",
                explanation=f"I will fill a VLOOKUP formula on {target_sheet} using {source_sheet.name} as the source table.",
                risk_level="medium",
                requires_confirmation=True,
                impacted_range=f"{target_column_letter}2:{target_column_letter}{sheet['max_row']}",
                parameters={
                    "target_column_letter": target_column_letter,
                    "start_row": 2,
                    "end_row": max(2, sheet["max_row"]),
                },
                impact=ActionImpact(
                    summary=f"A lookup formula will be filled across {max(0, sheet['max_row'] - 1)} rows.",
                    estimated_rows=max(0, sheet["max_row"] - 1),
                    estimated_cells=max(0, sheet["max_row"] - 1),
                    warnings=["Cross-sheet formulas should be reviewed before applying on production files."],
                ),
            )

        if any(token in command_lower for token in ["verify", "match"]) and len(snapshot.sheets) > 1:
            source_sheet = next(sheet_item for sheet_item in snapshot.sheets if sheet_item.name != target_sheet)
            target_column_letter = workbook_service.next_available_column_letter(snapshot.session_id, target_sheet)
            return ActionPlan(
                action="fill_formula_down",
                target_sheet=target_sheet,
                target_column=target_column_letter,
                formula=f'=IF(A{{row}}=\'{source_sheet.name}\'!A{{row}},"Match","No Match")',
                preview_title="Verify row matches",
                explanation=f"I will fill a match verification formula on {target_sheet} against {source_sheet.name}.",
                risk_level="medium",
                requires_confirmation=True,
                impacted_range=f"{target_column_letter}2:{target_column_letter}{sheet['max_row']}",
                parameters={
                    "target_column_letter": target_column_letter,
                    "start_row": 2,
                    "end_row": max(2, sheet["max_row"]),
                },
                impact=ActionImpact(
                    summary=f"A verification formula will be filled across {max(0, sheet['max_row'] - 1)} rows.",
                    estimated_rows=max(0, sheet["max_row"] - 1),
                    estimated_cells=max(0, sheet["max_row"] - 1),
                ),
            )

        if any(token in command_lower for token in ["pivot", "summary by", "group by", "summarize"]):
            group_header = text_header or sheet["headers"][0]
            val_header = numeric_header or (sheet["headers"][1] if len(sheet["headers"]) > 1 else sheet["headers"][0])
            return ActionPlan(
                action="create_pivot",
                target_sheet=target_sheet,
                target_column=group_header,
                preview_title="Create Pivot Table",
                explanation=f"I will create a pivot table summarizing {val_header} grouped by {group_header}.",
                risk_level="low",
                requires_confirmation=False,
                impacted_range="A1:B10",
                parameters={"group_by": group_header, "value_col": val_header, "agg": "sum"},
                impact=ActionImpact(
                    summary="A new sheet will be created with the summarized pivot data.",
                    estimated_rows=10,
                    estimated_cells=20,
                )
            )

        if any(token in command_lower for token in ["add sheet", "create sheet", "new sheet"]):
            new_sheet_name = "New_Sheet"
            match = re.search(r"(?:called|named|as) ([a-z0-9 _-]+)", command_lower)
            if match:
                new_sheet_name = match.group(1).title().strip()
            
            return ActionPlan(
                action="add_sheet",
                target_sheet=target_sheet,
                preview_title="Add new sheet",
                explanation=f"I will create a new sheet named {new_sheet_name}.",
                risk_level="low",
                requires_confirmation=False,
                parameters={"new_sheet_name": new_sheet_name},
                impact=ActionImpact(
                    summary=f"A new blank sheet '{new_sheet_name}' will be added.",
                )
            )

        if any(token in command_lower for token in ["join", "merge", "combine"]) and len(snapshot.sheets) > 1:
            source_sheet = next(sheet_item for sheet_item in snapshot.sheets if sheet_item.name != target_sheet)
            join_header = text_header or sheet["headers"][0]
            
            return ActionPlan(
                action="join_sheets",
                target_sheet=target_sheet,
                target_column=join_header,
                preview_title="Join Sheets",
                explanation=f"I will join data from {source_sheet.name} into {target_sheet} using {join_header}.",
                risk_level="medium",
                requires_confirmation=True,
                parameters={"secondary_sheet": source_sheet.name, "join_column": join_header},
                impact=ActionImpact(
                    summary=f"Data will be merged. This could add several new columns from {source_sheet.name}.",
                    estimated_rows=sheet["max_row"],
                    estimated_cells=sheet["max_row"] * 5,
                    warnings=["Merging sheets can be heavy. Review before applying."]
                )
            )

        return self._unsupported_plan(target_sheet)

    def _unsupported_plan(self, target_sheet: str) -> ActionPlan:
        return ActionPlan(
            action="noop",
            target_sheet=target_sheet,
            preview_title="Command not supported yet",
            explanation=(
                "Is advanced build me formulas, sorting, duplicate cleanup, replace, filters, highlights, charts, "
                "column conversion, aur cross-sheet lookup supported hain. Is specific command ko abhi aur training chahiye."
            ),
            risk_level="low",
            requires_confirmation=False,
            parameters={},
            impact=ActionImpact(summary="No workbook changes will be made."),
        )

    def _build_difference_plan(
        self,
        command_lower: str,
        snapshot: WorkbookSnapshot,
        target_sheet: str,
        sheet: dict[str, Any],
        matched_headers: list[str],
    ) -> ActionPlan | None:
        header_row = workbook_service.detect_header_row(snapshot.session_id, target_sheet)
        data_start_row = header_row + 1
        target_column_letter = workbook_service.next_available_column_letter(snapshot.session_id, target_sheet)

        explicit_match = re.search(
            r"\b([a-z0-9 _&()-]+?)\s*(?:minus|subtract(?:ed)? by|less)\s*([a-z0-9 _&()-]+)\b",
            command_lower,
        )
        left_column = None
        right_column = None
        if explicit_match:
            left_token = explicit_match.group(1).strip()
            right_token = explicit_match.group(2).strip()
            left_column = workbook_service.header_to_column_letter(snapshot.session_id, target_sheet, left_token)
            right_column = workbook_service.header_to_column_letter(snapshot.session_id, target_sheet, right_token)

        if left_column is None or right_column is None:
            grand_total_candidates = workbook_service.find_columns_with_keywords(
                snapshot.session_id,
                target_sheet,
                ["grand total"],
            )
            if len(grand_total_candidates) >= 2:
                left_column = grand_total_candidates[0]
                right_column = grand_total_candidates[-1]

        if left_column is None or right_column is None:
            income_keywords = [
                "grand total",
                "income",
                "collection",
                "received",
                "revenue",
                "sales",
                "earning",
                "total income",
                "total collection",
            ]
            expense_keywords = [
                "grand total",
                "expense",
                "spend",
                "spent",
                "payment",
                "cost",
                "outflow",
                "debit",
                "total expense",
            ]
            income_candidates = workbook_service.find_columns_with_keywords(snapshot.session_id, target_sheet, income_keywords)
            expense_candidates = workbook_service.find_columns_with_keywords(snapshot.session_id, target_sheet, expense_keywords)
            if income_candidates:
                left_column = income_candidates[0]
            if expense_candidates:
                right_column = expense_candidates[-1]

        if (left_column is None or right_column is None) and len(sheet.get("numeric_headers") or []) >= 2:
            left_header = next((header for header in matched_headers if header in sheet.get("numeric_headers", [])), None)
            numeric_headers = list(sheet.get("numeric_headers") or [])
            left_header = left_header or numeric_headers[0]
            right_header = next((header for header in reversed(numeric_headers) if header != left_header), None)
            left_column = left_column or workbook_service.header_to_column_letter(snapshot.session_id, target_sheet, left_header)
            right_column = right_column or workbook_service.header_to_column_letter(snapshot.session_id, target_sheet, right_header)

        if not left_column or not right_column or left_column == right_column:
            return None

        return ActionPlan(
            action="fill_formula_down",
            target_sheet=target_sheet,
            target_column=target_column_letter,
            preview_title="Insert profit formula",
            explanation=(
                f"I will calculate profit on {target_sheet} using {left_column} minus {right_column} "
                f"and fill it down from row {data_start_row}."
            ),
            risk_level="low",
            requires_confirmation=False,
            impacted_range=f"{target_column_letter}{header_row}:{target_column_letter}{sheet['max_row']}",
            formula=f"={left_column}{{row}}-{right_column}{{row}}",
            parameters={
                "start_row": data_start_row,
                "end_row": sheet["max_row"],
                "header_row": header_row,
                "column_header": "Profit",
            },
            impact=ActionImpact(
                summary=f"A Profit column will be created from {left_column} minus {right_column}.",
                estimated_rows=max(0, sheet["max_row"] - data_start_row + 1),
                estimated_cells=max(0, sheet["max_row"] - data_start_row + 1),
            ),
        )

    def _build_prompt(self, command: str, snapshot: WorkbookSnapshot) -> str:
        return (
            "You are an Excel AI agent. Read the workbook context and respond only as JSON.\n"
            "Allowed actions: insert_formula, fill_formula_down, sort, delete_duplicates, find_replace, "
            "highlight_threshold, apply_filter, clear_filter, create_chart, convert_column_type, create_pivot, add_sheet, join_sheets, noop.\n"
            "Return keys: action, target_sheet, target_cell, target_column, formula, preview_title, explanation, "
            "risk_level, requires_confirmation, impacted_range, parameters, impact.\n"
            "impact must contain: summary, estimated_rows, estimated_cells, warnings.\n\n"
            f"Workbook context:\n{json.dumps(snapshot.model_dump(), default=str, indent=2)}\n\n"
            f"User command:\n{command}\n"
        )

    def _sheet_by_name(self, snapshot: WorkbookSnapshot, sheet_name: str) -> dict[str, Any]:
        context_map = {sheet.name: sheet.model_dump() for sheet in snapshot.context}
        summary_map = {sheet.name: sheet.model_dump() for sheet in snapshot.sheets}
        if sheet_name not in summary_map:
            raise HTTPException(status_code=404, detail=f"Sheet '{sheet_name}' not found in snapshot.")
        return {**summary_map[sheet_name], **context_map.get(sheet_name, {})}

    def _find_target_sheet(self, command: str, snapshot: WorkbookSnapshot) -> str:
        for sheet in snapshot.sheets:
            if sheet.name.lower() in command:
                return sheet.name
        return snapshot.active_sheet

    def _find_header_in_command(self, command: str, headers: list[str]) -> str | None:
        normalized_command = self._normalize(command)
        for header in headers:
            normalized_header = self._normalize(header)
            if normalized_header and normalized_header in normalized_command:
                return header
        return headers[0] if headers else None

    def _find_matching_headers(self, command: str, headers: list[str]) -> list[str]:
        normalized_command = self._normalize(command)
        matches: list[str] = []
        for header in headers:
            normalized_header = self._normalize(header)
            if normalized_header and normalized_header in normalized_command:
                matches.append(header)
        return matches

    def _pick_numeric_header(self, sheet: dict[str, Any], preferred: str | None) -> str | None:
        numeric_headers = list(sheet.get("numeric_headers") or [])
        if preferred in numeric_headers:
            return preferred
        return numeric_headers[0] if numeric_headers else None

    def _pick_text_header(self, sheet: dict[str, Any], preferred: str | None) -> str | None:
        text_headers = list(sheet.get("text_headers") or [])
        if preferred in text_headers:
            return preferred
        return text_headers[0] if text_headers else (sheet["headers"][0] if sheet.get("headers") else None)

    def _pick_condition_header(
        self,
        sheet: dict[str, Any],
        matched_headers: list[str],
        value_header: str | None,
        operator: str,
    ) -> str | None:
        preferred_pool = (
            list(sheet.get("text_headers") or [])
            if operator in {"equals", "contains"}
            else list(sheet.get("numeric_headers") or [])
        )
        for header in matched_headers:
            if header != value_header:
                return header
        for header in preferred_pool:
            if header != value_header:
                return header
        for header in sheet.get("headers") or []:
            if header != value_header:
                return header
        return value_header

    def _extract_filter_criterion(self, command: str) -> tuple[str, Any]:
        command_lower = command.lower()
        
        # Special handling for relative dates
        if "today" in command_lower or "aaj" in command_lower:
            from datetime import date
            return "equals", date.today().strftime("%Y-%m-%d")
        if "yesterday" in command_lower or "kal" in command_lower:
            from datetime import date, timedelta
            return "equals", (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

        contains_match = re.search(r"(?:contains|includes)\s+([a-z0-9 _-]+)$", command)
        if contains_match:
            return "contains", self._clean_criterion_text(contains_match.group(1))
        numeric_match = re.search(r"(>=|<=|>|<|=)\s*(-?\d+(?:\.\d+)?)", command)
        if numeric_match:
            return self._map_operator(numeric_match.group(1)), float(numeric_match.group(2))
        greater_match = re.search(r"(above|greater than)\s*(-?\d+(?:\.\d+)?)", command)
        if greater_match:
            return "greater_than", float(greater_match.group(2))
        less_match = re.search(r"(below|less than)\s*(-?\d+(?:\.\d+)?)", command)
        if less_match:
            return "less_than", float(less_match.group(2))
        text_match = re.search(r"(?:is|equals|=)\s+([a-z0-9 _-]+)$", command)
        if text_match:
            return "equals", self._clean_criterion_text(text_match.group(1))
        return "greater_than", 0

    @staticmethod
    def _extract_target_type(command: str) -> str | None:
        if "date" in command:
            return "date"
        if any(token in command for token in ["number", "numeric"]):
            return "number"
        if "text" in command:
            return "text"
        return None

    @staticmethod
    def _excel_criteria(operator: str, criterion: Any) -> str:
        mapping = {
            "greater_than": ">",
            "greater_or_equal": ">=",
            "less_than": "<",
            "less_or_equal": "<=",
            "equals": "",
            "contains": "*",
        }
        normalized_operator = operator.lower()
        if normalized_operator == "contains":
            return f"*{criterion}*"
        prefix = mapping.get(normalized_operator, "")
        return f"{prefix}{criterion}"

    @staticmethod
    def _clean_criterion_text(text: str) -> str:
        cleaned = text.strip()
        cleaned = re.sub(r"\b(rows?|records?|cells?|values?)\b.*$", "", cleaned).strip()
        cleaned = re.sub(
            r"\b(filter|show|only|remain|nikalo|karo|banao|apply|insert|formula|highlight|convert|me|column|ka|ki)\b.*$",
            "",
            cleaned,
        ).strip()
        return cleaned

    @staticmethod
    def _map_operator(symbol: str) -> str:
        return {
            ">": "greater_than",
            ">=": "greater_or_equal",
            "<": "less_than",
            "<=": "less_or_equal",
            "=": "equals",
        }.get(symbol, "equals")

    @staticmethod
    def _col_letter(index: int) -> str:
        result = ""
        current = index
        while current:
            current, remainder = divmod(current - 1, 26)
            result = chr(65 + remainder) + result
        return result

    @staticmethod
    def _normalize(text: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", text.lower())


ai_service = AIService()
