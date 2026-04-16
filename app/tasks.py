"""Celery tasks for background Excel processing."""

from __future__ import annotations

import tempfile
from pathlib import Path

from app.celery_app import app
from app.directions import get_direction
from app.excel_parser import extract_balances
from app.web import (
    _build_excel,
    _build_jbi_excel,
    _build_jbi_summary,
    _build_summary,
    _load_materials,
)


BASE_DIR = Path(__file__).resolve().parent.parent
JOBS_DIR = BASE_DIR / "jobs"


@app.task(bind=True)
def process_excel_task(
    self,
    file_bytes: bytes,
    filename: str,
    mode: str,
    scope: str,
    profile_name: str | None,
) -> dict:
    """
    Process Excel file in background.
    mode: 'summary' | 'excel'
    scope: 'beton' | 'jbi'
    Returns: {"summary": dict, "has_excel": bool}
    """
    task_id = self.request.id
    profile_name = profile_name if profile_name and profile_name != "__base__" else None

    with tempfile.NamedTemporaryFile(
        suffix=".xlsx", delete=False, prefix="beton_"
    ) as tmp:
        tmp.write(file_bytes)
        input_path = Path(tmp.name)

    try:
        direction = get_direction(scope)
        materials = _load_materials(scope=scope, profile_name=profile_name)
        beton_materials = (
            _load_materials(scope=direction.concrete_source, profile_name=None)
            if direction.concrete_source
            else []
        )
        balances = extract_balances(str(input_path), materials)
        beton_balances: dict[str, float] = {}
        if direction.concrete_source:
            try:
                beton_balances = extract_balances(str(input_path), beton_materials)
            except Exception:
                pass

        if direction.calc_type == "units":
            combined = {**balances, **beton_balances}
            summary = _build_jbi_summary(combined, profile_name=profile_name)
            if mode == "excel":
                excel_bytes = _build_jbi_excel(combined, profile_name=profile_name)
                JOBS_DIR.mkdir(parents=True, exist_ok=True)
                excel_path = JOBS_DIR / f"{task_id}.xlsx"
                excel_path.write_bytes(excel_bytes)
                return {"summary": summary, "has_excel": True}
            return {"summary": summary, "has_excel": False}

        summary = _build_summary(
            balances, scope=scope, profile_name=profile_name
        )

        if mode == "excel":
            excel_bytes = _build_excel(
                balances, scope=scope, profile_name=profile_name
            )
            JOBS_DIR.mkdir(parents=True, exist_ok=True)
            excel_path = JOBS_DIR / f"{task_id}.xlsx"
            excel_path.write_bytes(excel_bytes)
            return {"summary": summary, "has_excel": True}

        return {"summary": summary, "has_excel": False}
    finally:
        input_path.unlink(missing_ok=True)
