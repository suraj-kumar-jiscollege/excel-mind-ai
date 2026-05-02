from __future__ import annotations

import sys
import unittest
from pathlib import Path

from openpyxl import Workbook
from openpyxl.workbook.defined_name import DefinedName
from openpyxl.worksheet.table import Table, TableStyleInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.services.formula_engine import FormulaEngine


class FormulaEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = FormulaEngine()
        self.workbook = Workbook()
        self.sheet = self.workbook.active
        self.sheet.title = "Sheet1"
        self.sheet["A1"] = 1
        self.sheet["A2"] = 2
        self.sheet["A3"] = 3
        self.sheet["B1"] = "North"
        self.sheet["B2"] = "South"
        self.sheet["B3"] = "North"
        self.sheet["C1"] = 10
        self.sheet["C2"] = 20
        self.sheet["C3"] = 30

    def test_basic_math_and_lazy_iferror(self) -> None:
        self.assertEqual(self.engine.evaluate_formula(self.workbook, "Sheet1", "=SUM(A1:A3)"), 6)
        self.assertEqual(self.engine.evaluate_formula(self.workbook, "Sheet1", '=IFERROR(1/0,"fallback")'), "fallback")
        self.assertEqual(self.engine.evaluate_formula(self.workbook, "Sheet1", '=IF(A1>0,"Yes","No")'), "Yes")

    def test_named_range_resolution(self) -> None:
        self.workbook.defined_names.add(DefinedName("Revenue", attr_text="'Sheet1'!$A$1:$A$3"))
        self.assertEqual(self.engine.evaluate_formula(self.workbook, "Sheet1", "=SUM(Revenue)"), 6)

    def test_structured_table_reference(self) -> None:
        self.sheet["A2"] = "East"
        self.sheet["A3"] = "West"
        self.sheet["B2"] = 15
        self.sheet["B3"] = 25
        self.sheet["C2"] = "Y"
        self.sheet["C3"] = "N"
        table = Table(displayName="SalesTable", ref="A1:C3")
        table.tableStyleInfo = TableStyleInfo(
            name="TableStyleMedium2",
            showFirstColumn=False,
            showLastColumn=False,
            showRowStripes=True,
            showColumnStripes=False,
        )
        self.sheet.add_table(table)
        self.sheet["A1"] = "Region"
        self.sheet["B1"] = "Amount"
        self.sheet["C1"] = "Flag"
        self.assertEqual(self.engine.evaluate_formula(self.workbook, "Sheet1", "=SUM(SalesTable[Amount])"), 40)
        self.assertEqual(self.engine.evaluate_formula(self.workbook, "Sheet1", "=SalesTable[#Headers]"), ["Region", "Amount", "Flag"])

    def test_lookup_and_dynamic_functions(self) -> None:
        self.assertEqual(self.engine.evaluate_formula(self.workbook, "Sheet1", '=CHOOSE(2,"a","b","c")'), "b")
        self.assertEqual(self.engine.evaluate_formula(self.workbook, "Sheet1", '=SWITCH("B","A",1,"B",2,9)'), 2)
        self.assertEqual(self.engine.evaluate_formula(self.workbook, "Sheet1", '=XLOOKUP("South",B1:B3,C1:C3,"")'), 20)
        self.assertEqual(self.engine.evaluate_formula(self.workbook, "Sheet1", '=INDEX(C1:C3,MATCH("South",B1:B3,0))'), 20)
        self.assertEqual(self.engine.evaluate_formula(self.workbook, "Sheet1", '=SUM(OFFSET(A1,1,0,2,1))'), 5)
        self.assertEqual(self.engine.evaluate_formula(self.workbook, "Sheet1", '=INDIRECT("A1:A3")'), [[1], [2], [3]])

    def test_filter_unique_sort_and_let(self) -> None:
        self.assertEqual(self.engine.evaluate_formula(self.workbook, "Sheet1", '=UNIQUE(B1:B3)'), ["North", "South"])
        self.assertEqual(self.engine.evaluate_formula(self.workbook, "Sheet1", '=SORT(C1:C3,1,TRUE)'), [[10], [20], [30]])
        self.assertEqual(self.engine.evaluate_formula(self.workbook, "Sheet1", '=LET(x,5,x+2)'), 7)

    def test_iferror_with_named_range(self) -> None:
        self.workbook.defined_names.add(DefinedName("Totals", attr_text="'Sheet1'!$C$1:$C$3"))
        self.assertEqual(self.engine.evaluate_formula(self.workbook, "Sheet1", '=SUM(Totals)'), 60)


if __name__ == "__main__":
    unittest.main()
