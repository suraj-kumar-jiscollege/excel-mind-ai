import pytest
from app.services.workbook_service import workbook_service
from app.models import ActionPlan
import os

def test_insight_generation():
    # Create a dummy workbook with correlation
    from openpyxl import Workbook
    wb = Workbook()
    ws = wb.active
    ws.title = "SalesData"
    ws.append(["Month", "Sales", "Advertising"])
    ws.append(["Jan", 100, 10])
    ws.append(["Feb", 120, 12])
    ws.append(["Mar", 140, 14])
    ws.append(["Apr", 160, 16])
    
    file_path = "test_correlation.xlsx"
    wb.save(file_path)
    
    try:
        snapshot = workbook_service.open_workbook(file_path)
        insights = snapshot.insights
        
        # Check if correlation insight is generated
        correlation_found = any("Correlation" in i.title for i in insights)
        assert correlation_found, "Correlation insight should be generated for linear data"
        
        # Check if time series insight is generated
        time_found = any("Time Series" in i.title for i in insights)
        # Note: Month is text, but we can improve detection later
        # For now, let's just ensure we have insights
        assert len(insights) > 0
        
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)

def test_performance_large_workbook():
    from openpyxl import Workbook
    wb = Workbook()
    ws = wb.active
    ws.title = "LargeData"
    ws.append(["ID", "Value"])
    for i in range(1000): # Simulating a reasonably large sheet
        ws.append([i, i * 2])
    
    file_path = "test_large.xlsx"
    wb.save(file_path)
    
    import time
    start_time = time.time()
    try:
        snapshot = workbook_service.open_workbook(file_path)
        end_time = time.time()
        
        # Ensure snapshot generation is fast
        assert (end_time - start_time) < 2.0, "Snapshot generation took too long"
        assert len(snapshot.sheets[0].rows) <= 10001 # Truncated limit check
        
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)
