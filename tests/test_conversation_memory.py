from __future__ import annotations

import asyncio
from io import BytesIO
from pathlib import Path
import sys
import unittest
from datetime import datetime

from openpyxl import Workbook

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.ai_service import ai_service
from app.services.workbook_service import workbook_service


def _make_workbook_bytes() -> bytes:
    workbook = Workbook()
    sheet1 = workbook.active
    sheet1.title = "Sheet1"
    sheet1.append(["Date", "Name", "Sales"])
    sheet1.append([datetime(2024, 1, 1), "Alpha", 10])
    sheet1.append([datetime(2024, 1, 2), "Beta", 20])
    sheet1.append([datetime(2024, 1, 3), "Gamma", 1000])

    sheet2 = workbook.create_sheet("Sheet2")
    sheet2.append(["Name", "Sales"])
    sheet2.append(["Gamma", 30])
    sheet2.append(["Delta", 40])

    stream = BytesIO()
    workbook.save(stream)
    return stream.getvalue()


class ConversationMemoryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        snapshot = workbook_service.open_workbook_from_bytes(_make_workbook_bytes(), "memory-test.xlsx")
        self.session_id = snapshot.session_id

    async def test_preview_updates_memory(self) -> None:
        plan = await ai_service.preview_command(self.session_id, "freeze top row")
        self.assertEqual(plan.action, "freeze_header")

        snapshot = workbook_service.get_snapshot(self.session_id)
        self.assertEqual(snapshot.memory.last_action, "freeze_header")
        self.assertIn("freeze top row", snapshot.memory.last_command.lower())
        self.assertTrue(snapshot.memory.follow_up_prompts)

    async def test_batch_command_creates_multi_step_plan(self) -> None:
        plan = await ai_service.preview_command(self.session_id, "freeze top row and auto fit columns")
        self.assertEqual(plan.action, "batch")
        self.assertIn("steps", plan.parameters)
        self.assertGreaterEqual(len(plan.parameters["steps"]), 2)

    async def test_follow_up_can_target_another_sheet(self) -> None:
        first_plan = await ai_service.preview_command(self.session_id, "freeze top row")
        self.assertEqual(first_plan.action, "freeze_header")

        follow_up = await ai_service.preview_command(self.session_id, "same for Sheet2")
        self.assertEqual(follow_up.action, "freeze_header")
        self.assertEqual(follow_up.target_sheet, "Sheet2")

    async def test_analysis_and_chart_recommendations_exist(self) -> None:
        snapshot = workbook_service.get_snapshot(self.session_id)
        self.assertTrue(snapshot.insights)
        self.assertTrue(snapshot.chart_recommendations)
        self.assertEqual(snapshot.chart_recommendations[0].chart_type, "line")

        plan = await ai_service.preview_command(self.session_id, "analyze workbook for anomalies and trends")
        self.assertEqual(plan.action, "analyze_workbook")
        self.assertIn("analysis", plan.preview_title.lower())

    async def test_hinglish_analysis_prompt_routes_to_workbook_analysis(self) -> None:
        plan = await ai_service.preview_command(self.session_id, "profit batao")
        self.assertEqual(plan.action, "analyze_workbook")
        self.assertIn("analysis", plan.preview_title.lower())


if __name__ == "__main__":
    unittest.main()
