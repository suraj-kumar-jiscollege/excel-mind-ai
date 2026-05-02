from __future__ import annotations
from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse, Response

from app.models import (
    CommandExecuteRequest,
    CommandPreviewRequest,
    CommandPreviewResponse,
    SaveWorkbookRequest,
    SaveWorkbookResponse,
    UpdateCellRequest,
    WorkbookSessionRequest,
    WorkbookSheetRequest,
    WorkbookSnapshot,
)
from app.config import settings
from app.services.ai_service import ai_service
from app.services.workbook_service import workbook_service


app = FastAPI(title="ExcelMind AI Backend", version="0.1.0")

allow_credentials = "*" not in settings.cors_origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=allow_credentials,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def read_root():
    return RedirectResponse(url="/docs")


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/api/workbooks/open", response_model=WorkbookSnapshot)
async def open_workbook(file: UploadFile = File(...)) -> WorkbookSnapshot:
    content = await file.read()
    return workbook_service.open_workbook_from_bytes(content, file.filename or "workbook.xlsx")

@app.get("/api/workbooks/{session_id}/download")
def download_workbook(session_id: str):
    session = workbook_service.get_session(session_id)
    content = workbook_service.get_workbook_bytes(session_id)
    return Response(
        content=content,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{session.file_path.name}"'}
    )


@app.get("/api/workbooks/{session_id}", response_model=WorkbookSnapshot)
def get_workbook(session_id: str) -> WorkbookSnapshot:
    return workbook_service.get_snapshot(session_id)


@app.post("/api/workbooks/active-sheet", response_model=WorkbookSnapshot)
def set_active_sheet(payload: WorkbookSheetRequest) -> WorkbookSnapshot:
    return workbook_service.set_active_sheet(payload.session_id, payload.sheet_name)


@app.post("/api/workbooks/cell", response_model=WorkbookSnapshot)
def update_cell(payload: UpdateCellRequest) -> WorkbookSnapshot:
    return workbook_service.update_cell(
        session_id=payload.session_id,
        sheet_name=payload.sheet_name,
        cell=payload.cell,
        value=payload.value,
    )


@app.post("/api/workbooks/save", response_model=SaveWorkbookResponse)
def save_workbook(payload: SaveWorkbookRequest) -> SaveWorkbookResponse:
    file_path, snapshot = workbook_service.save_workbook(payload.session_id, payload.destination_path)
    return SaveWorkbookResponse(file_path=file_path, snapshot=snapshot)


@app.post("/api/workbooks/undo", response_model=WorkbookSnapshot)
def undo_workbook(payload: WorkbookSessionRequest) -> WorkbookSnapshot:
    return workbook_service.undo(payload.session_id)


@app.post("/api/workbooks/redo", response_model=WorkbookSnapshot)
def redo_workbook(payload: WorkbookSessionRequest) -> WorkbookSnapshot:
    return workbook_service.redo(payload.session_id)


@app.post("/api/commands/preview", response_model=CommandPreviewResponse)
async def preview_command(payload: CommandPreviewRequest) -> CommandPreviewResponse:
    plan = await ai_service.preview_command(
.``        payload.session_id,
        payload.command,
        payload.selected_cell,
        payload.selected_value,
    )
    snapshot = workbook_service.get_snapshot(payload.session_id)
    return CommandPreviewResponse(plan=plan, snapshot=snapshot)


@app.post("/api/commands/execute", response_model=WorkbookSnapshot)
def execute_command(payload: CommandExecuteRequest) -> WorkbookSnapshot:
    return workbook_service.execute_action(payload.session_id, payload.plan)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host=settings.host, port=settings.port, reload=settings.reload)
