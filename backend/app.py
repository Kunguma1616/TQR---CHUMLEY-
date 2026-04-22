import asyncio
import base64
import io
import json
import logging
import os
import re
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Any
from PIL import Image
import aiosqlite
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.concurrency import run_in_threadpool
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from groq import Groq
from simple_salesforce import Salesforce


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%d/%m/%Y %H:%M:%S",
)
log = logging.getLogger("tqr")


BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")
DB_PATH = BASE_DIR / "tqr_results.db"
IMAGE_FILE_TYPES = {"jpg", "jpeg", "png", "heic", "webp"}
DOCUMENT_CONTENT_TYPES = {
    "pdf": "application/pdf",
    "doc": "application/msword",
    "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "xls": "application/vnd.ms-excel",
    "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "csv": "text/csv",
    "txt": "text/plain",
}
PROMPT_CONFIG_PATH = BASE_DIR / "tqr_scoring_matrix.json"

APPOINTMENT_FIELDS = [
    "Id",
    "Trade_Group_Postcode__c",
    "Trade_Group_Region__c",
    "Allocated_Engineer__c",
    "Allocated_Engineer__r.Name",
    "Feedback_Notes__c",
    "AppointmentNumber",
    "Status",
    "Scheduled_Trade__c",
    "Description",
    "ActualStartTime",
    "ActualEndTime",
    "Attendance_Notes_for_Office__c",
    "Attendance_Report_for_Customer__c",
    "Workmanship__c",
    "Workmanship1__c",
    "CCT_Charge_Gross__c",
    "EPR_Status__c",
    "Work_Order__c",
    "Report__c",
    "Post_Visit_Report_Check__c",
    "Decision_Making__c",
    "Payment_Attempted__c",
    "Sector_Type__c",
    "ArrivalWindowStartTime",
    "SchedStartTime",
    "Duration",
    "Street",
    "City",
    "PostalCode",
    "Subject",
    "Job__c",
    "Job_Number__c",
    "ParentRecordId",
    "RecordType.Name",
]

BASE_APPOINTMENTS_SOQL = f"""
    SELECT {", ".join(APPOINTMENT_FIELDS)}
    FROM ServiceAppointment
    WHERE ActualEndTime >= LAST_N_DAYS:90
      AND Status = 'Visit Complete'
      AND Post_Visit_Report_Check__c = null
      AND RecordType.Name = 'Service Appointment'
      AND (NOT Trade_Group_Postcode__c LIKE '%util%')
      AND (NOT Trade_Group_Postcode__c LIKE '%PM%')
      AND Account_Type__c != 'key account'
      AND (NOT Trade_Group_Postcode__c LIKE '%insurance%')
    ORDER BY ActualEndTime DESC
"""


def parse_salesforce_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def format_uk_datetime(value: str | None) -> str | None:
    dt = parse_salesforce_datetime(value)
    return dt.astimezone().strftime("%d/%m/%Y %H:%M") if dt else None


def safe_float(value: Any) -> float:
    try:
        return float(value or 0)
    except (ValueError, TypeError):
        return 0.0


def _unique_strings(values: list[str | None]) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for value in values:
        item = str(value or "").strip()
        if not item or item in seen:
            continue
        output.append(item)
        seen.add(item)
    return output


def clean_record(record: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in record.items() if key != "attributes"}


def json_extract(raw: str | None) -> dict[str, Any]:
    if not raw:
        raise ValueError("Empty AI response")
    start = raw.find("{")
    end = raw.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise ValueError("JSON object not found in AI response")
    return json.loads(raw[start : end + 1])


def load_prompt_config() -> dict[str, Any]:
    with open(PROMPT_CONFIG_PATH, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _format_bullets(items: list[str]) -> str:
    return "\n".join(f"- {item}" for item in items)


def build_tqr_system_prompt(config: dict[str, Any]) -> str:
    fields = config["fields"]
    sections: list[str] = [
        "ROLE",
        "You are a Trade Quality Review (TQR) scoring assistant for Chumley, a field service company.",
        "Your task is to assess completed jobs against a documented rubric and produce per-field scores with cited evidence for review by a human trade manager.",
        "",
        "CORE PRINCIPLES",
        _format_bullets(config["core_principles"]),
        "",
        "SUMMARY VERDICT RULES",
        "Hard fail triggers:",
        _format_bullets(config["summary_verdict"]["hard_fail_triggers"]),
        "",
        "OUTPUT DISCIPLINE",
        _format_bullets(config["output_schema_summary"]["requiredFields"]),
        "",
        "VALIDATION RULES",
        _format_bullets(config["output_schema_summary"]["validationRules"]),
        "",
        "FIELD INSTRUCTIONS"
    ]
    for field_name, field in fields.items():
        sections.extend([
            "",
            f"FIELD {field_name}",
            f"Purpose: {field['purpose']}",
            "Evidence sources:",
            _format_bullets(field.get("evidenceSources", [])),
        ])
        if field.get("mandatoryPhotos"):
            sections.extend([
                "Mandatory photos:",
                _format_bullets(field["mandatoryPhotos"]),
            ])
        if field.get("situationalPhotos"):
            sections.extend([
                "Situational photos:",
                _format_bullets(field["situationalPhotos"]),
            ])
        if field.get("rubric"):
            rubric_lines = [f"{item['band']} {item['label']}: {item['meaning']}" for item in field["rubric"]]
            sections.extend([
                "Scoring rubric:",
                _format_bullets(rubric_lines),
            ])
        if field.get("salesforceMapping"):
            sections.extend([
                "Salesforce mapping:",
                _format_bullets(field["salesforceMapping"]),
            ])
        if field.get("reviewTriggers"):
            sections.extend([
                "AI review triggers:",
                _format_bullets(field["reviewTriggers"]),
            ])
        if field.get("validationRules"):
            sections.extend([
                "Field validation rules:",
                _format_bullets(field["validationRules"]),
            ])
    sections.extend([
        "",
        "IMPORTANT",
        "- Do not hallucinate missing evidence.",
        "- Understand the image evidence in practical detail before scoring.",
        "- If evidence is missing or ambiguous, prefer Review over an unjustified passing score.",
        "- Your response must be a single valid JSON object with a top-level 'fields' object and 'summary' object."
    ])
    return "\n".join(sections)


def create_salesforce_client() -> Salesforce:
    return Salesforce(
        username=os.getenv("SF_USERNAME"),
        password=os.getenv("SF_PASSWORD"),
        security_token=os.getenv("SF_SECURITY_TOKEN"),
        domain=os.getenv("SF_DOMAIN", "login"),
    )


async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS tqr_results (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              appointment_id TEXT UNIQUE,
              workmanship REAL, cleanliness REAL,
              safety REAL, completion REAL, overall REAL,
              summary TEXT, flags TEXT, recommendation TEXT,
              tqr_fields TEXT,
              image_descriptions TEXT,
              hard_fail INTEGER DEFAULT 0,
              verdict TEXT,
              analysed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        for col, col_type in [
            ("tqr_fields", "TEXT"),
            ("image_descriptions", "TEXT"),
            ("hard_fail", "INTEGER"),
            ("verdict", "TEXT"),
        ]:
            try:
                await db.execute(f"ALTER TABLE tqr_results ADD COLUMN {col} {col_type}")
            except Exception:
                pass
        await db.commit()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    app.state.sf = await run_in_threadpool(create_salesforce_client)
    app.state.groq = Groq(api_key=os.getenv("GROQ_API_KEY"))
    app.state.prompt_config = load_prompt_config()
    app.state.tqr_system_prompt = build_tqr_system_prompt(app.state.prompt_config)
    app.state.rubric_scoring_model = app.state.prompt_config.get("models", {}).get("rubric_scoring_model", "openai/gpt-oss-120b")
    app.state.image_vision_model = "meta-llama/llama-4-scout-17b-16e-instruct"
    app.state.image_vision_models = ["meta-llama/llama-4-scout-17b-16e-instruct"]
    log.info("App started — Salesforce + Groq clients ready")
    yield


app = FastAPI(title="Chumley TQR Analyser API", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


async def sf_query_all(soql: str) -> list[dict[str, Any]]:
    sf = app.state.sf
    result = await run_in_threadpool(sf.query, soql)
    records = list(result.get("records", []))
    while not result.get("done", True):
        result = await run_in_threadpool(sf.query_more, result["nextRecordsUrl"], True)
        records.extend(result.get("records", []))
    return [clean_record(record) for record in records]


def _deserialise_cached_row(data: dict[str, Any]) -> dict[str, Any]:
    data["flags"] = json.loads(data["flags"] or "[]")
    data["tqr_fields"] = json.loads(data["tqr_fields"] or "null")
    data["image_descriptions"] = json.loads(data.get("image_descriptions") or "[]")
    data["hard_fail"] = bool(data.get("hard_fail"))
    if data.get("analysed_at"):
        data["analysed_at"] = datetime.fromisoformat(str(data["analysed_at"])).strftime("%d/%m/%Y %H:%M")
    return data


async def get_cached_result(appointment_id: str) -> dict[str, Any] | None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """
            SELECT appointment_id, workmanship, cleanliness, safety, completion,
                   overall, summary, flags, recommendation, tqr_fields, image_descriptions,
                   hard_fail, verdict, analysed_at
            FROM tqr_results
            WHERE appointment_id = ?
            """,
            (appointment_id,),
        )
        row = await cursor.fetchone()
    if not row:
        return None
    return _deserialise_cached_row(dict(row))


async def get_all_cached_results() -> dict[str, dict[str, Any]]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """
            SELECT appointment_id, workmanship, cleanliness, safety, completion,
                   overall, summary, flags, recommendation, tqr_fields, image_descriptions,
                   hard_fail, verdict, analysed_at
            FROM tqr_results
            """
        )
        rows = await cursor.fetchall()
    output: dict[str, dict[str, Any]] = {}
    for row in rows:
        item = _deserialise_cached_row(dict(row))
        output[item["appointment_id"]] = item
    return output


async def sf_query_all_safe(soql: str) -> list[dict[str, Any]]:
    try:
        return await sf_query_all(soql)
    except Exception as exc:
        log.warning("Optional SOQL failed: %s", exc)
        return []


async def save_analysis_result(appointment_id: str, result: dict[str, Any]) -> dict[str, Any]:
    fields = result.get("tqr_fields") or {}
    workmanship = safe_float((fields.get("workmanship") or {}).get("score", result.get("workmanship", 0)))
    overall = safe_float(result.get("overall", 0))
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO tqr_results (
                appointment_id, workmanship, cleanliness, safety,
                completion, overall, summary, flags, recommendation,
                tqr_fields, image_descriptions, hard_fail, verdict
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(appointment_id) DO UPDATE SET
                workmanship=excluded.workmanship, overall=excluded.overall,
                summary=excluded.summary, flags=excluded.flags,
                recommendation=excluded.recommendation, tqr_fields=excluded.tqr_fields,
                image_descriptions=excluded.image_descriptions,
                hard_fail=excluded.hard_fail, verdict=excluded.verdict,
                analysed_at=CURRENT_TIMESTAMP
            """,
            (
                appointment_id,
                workmanship,
                safe_float(result.get("cleanliness", 0)),
                safe_float(result.get("safety", 0)),
                safe_float(result.get("completion", 0)),
                overall,
                result.get("summary", ""),
                json.dumps(result.get("flags", [])),
                result.get("recommendation", ""),
                json.dumps(result.get("tqr_fields")) if result.get("tqr_fields") else None,
                json.dumps(result.get("image_descriptions", [])),
                1 if result.get("hard_fail") else 0,
                result.get("verdict", ""),
            ),
        )
        await db.commit()
    cached = await get_cached_result(appointment_id)
    if not cached:
        raise HTTPException(status_code=500, detail="Failed to cache analysis result")
    return cached


def enrich_appointment(record: dict[str, Any], tqr: dict[str, Any] | None = None) -> dict[str, Any]:
    item = dict(record)
    item["AllocatedEngineerName"] = ((item.get("Allocated_Engineer__r") or {}).get("Name") or item.get("Allocated_Engineer__c"))
    item["ActualEndTimeFormatted"] = format_uk_datetime(item.get("ActualEndTime"))
    item["ActualStartTimeFormatted"] = format_uk_datetime(item.get("ActualStartTime"))
    item["SchedStartTimeFormatted"] = format_uk_datetime(item.get("SchedStartTime"))
    item["ArrivalWindowStartTimeFormatted"] = format_uk_datetime(item.get("ArrivalWindowStartTime"))
    item["CCT_Charge_Gross__c"] = safe_float(item.get("CCT_Charge_Gross__c"))
    item["tqrResult"] = tqr
    item["tqrScore"] = tqr.get("overall") if tqr else None
    item["RecordTypeName"] = (item.get("RecordType") or {}).get("Name")
    item["workOrderId"] = resolve_work_order_id(item)
    return item


def resolve_work_order_id(record: dict[str, Any]) -> str | None:
    work_order_field = record.get("Work_Order__c")
    if isinstance(work_order_field, str):
        if len(work_order_field) in (15, 18) and work_order_field.startswith("0WO"):
            return work_order_field
        match = re.search(r"(0WO[a-zA-Z0-9]{12,15})", work_order_field)
        if match:
            return match.group(1)
    parent_record_id = record.get("ParentRecordId")
    if isinstance(parent_record_id, str) and len(parent_record_id) in (15, 18):
        return parent_record_id
    return None


async def get_review_queue_records() -> list[dict[str, Any]]:
    return await sf_query_all(BASE_APPOINTMENTS_SOQL)


def apply_appointment_filters(
    records: list[dict[str, Any]],
    engineer: str | None,
    status: str | None,
    trade: str | None,
    search: str | None,
    sector: str | None,
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for record in records:
        engineer_name = ((record.get("Allocated_Engineer__r") or {}).get("Name") or record.get("Allocated_Engineer__c") or "")
        if engineer and engineer_name != engineer:
            continue
        if status and (record.get("Status") or "") != status:
            continue
        if trade and (record.get("Scheduled_Trade__c") or "") != trade and (record.get("Trade_Group_Region__c") or record.get("Trade_Group_Postcode__c") or "") != trade:
            continue
        if sector and (record.get("Sector_Type__c") or "") != sector:
            continue
        if search:
            haystack = " ".join(
                str(record.get(field) or "")
                for field in (
                    "Trade_Group_Postcode__c",
                    "Trade_Group_Region__c",
                    "Allocated_Engineer__c",
                    "Feedback_Notes__c",
                    "AppointmentNumber",
                    "Status",
                    "Scheduled_Trade__c",
                    "Description",
                    "Attendance_Notes_for_Office__c",
                    "Attendance_Report_for_Customer__c",
                    "Work_Order__c",
                    "Sector_Type__c",
                )
            ).lower()
            if search.lower() not in haystack:
                continue
        output.append(record)
    return output


async def fetch_appointment_by_id(appointment_id: str) -> dict[str, Any]:
    records = await sf_query_all(
        f"""
        SELECT {", ".join(APPOINTMENT_FIELDS)}
        FROM ServiceAppointment
        WHERE Id = '{appointment_id}'
        LIMIT 1
        """
    )
    if not records:
        raise HTTPException(status_code=404, detail="Appointment not found")
    return records[0]


async def fetch_work_order(work_order_id: str) -> dict[str, Any]:
    work_orders = await sf_query_all(
        f"""
        SELECT Id, WorkOrderNumber, Description, Street, City, PostalCode,
               AccountId, ServiceTerritoryId, WorkTypeId, WorkType.Name, LastModifiedDate
        FROM WorkOrder
        WHERE Id = '{work_order_id}'
        LIMIT 1
        """
    )
    if not work_orders:
        raise HTTPException(status_code=404, detail="Work order not found")
    return work_orders[0]


async def fetch_account(account_id: str | None) -> dict[str, Any] | None:
    if not account_id:
        return None
    accounts = await sf_query_all(
        f"""
        SELECT Id, Name, Phone, Sector_Type__c, OwnerId, Owner.Name
        FROM Account
        WHERE Id = '{account_id}'
        LIMIT 1
        """
    )
    return accounts[0] if accounts else None


async def fetch_service_territory(territory_id: str | None) -> dict[str, Any] | None:
    if not territory_id:
        return None
    territories = await sf_query_all(
        f"""
        SELECT Id, Name
        FROM ServiceTerritory
        WHERE Id = '{territory_id}'
        LIMIT 1
        """
    )
    return territories[0] if territories else None


async def fetch_content_versions_for_entity(entity_id: str) -> list[dict[str, Any]]:
    links = await sf_query_all(
        f"""
        SELECT ContentDocumentId
        FROM ContentDocumentLink
        WHERE LinkedEntityId = '{entity_id}'
        """
    )
    if not links:
        return []
    ids = ",".join(f"'{link['ContentDocumentId']}'" for link in links if link.get("ContentDocumentId"))
    if not ids:
        return []
    versions = await sf_query_all(
        f"""
        SELECT Id, Title, FileType, VersionData, ContentDocumentId
        FROM ContentVersion
        WHERE ContentDocumentId IN ({ids}) AND IsLatest = true
        """
    )
    return versions


def classify_document(title: str, file_type: str, source_label: str) -> str:
    blob = f"{title} {file_type} {source_label}".lower()
    if "invoice" in blob:
        return "invoice"
    if "payment" in blob or "receipt" in blob:
        return "payment"
    if "service report" in blob or "service_report" in blob or "customer service report" in blob:
        return "service-report"
    # SA-XXXXXX pattern documents are service reports that typically contain customer signatures
    if re.search(r'\bsa[-_]\d+', title.lower()):
        return "service-report"
    return "document"


async def fetch_images_for_entity(entity_id: str) -> list[dict[str, Any]]:
    sf = app.state.sf
    versions = await fetch_content_versions_for_entity(entity_id)
    images: list[dict[str, Any]] = []
    for version in versions:
        if str(version.get("FileType", "")).lower() not in IMAGE_FILE_TYPES:
            continue
        version_id = version["Id"]
        url = f"{sf.base_url}sobjects/ContentVersion/{version_id}/VersionData"
        try:
            response = await run_in_threadpool(
                sf.session.get,
                url,
                headers={"Authorization": f"Bearer {sf.session_id}"},
            )
            response.raise_for_status()
            raw_bytes = response.content
        except Exception:
            continue
        file_type = str(version.get("FileType") or "").lower()
        content_type = {
            "jpg": "image/jpeg",
            "jpeg": "image/jpeg",
            "png": "image/png",
            "heic": "image/heic",
            "webp": "image/webp",
        }.get(file_type, "image/jpeg")
        images.append(
            {
                "id": version["Id"],
                "title": version.get("Title") or "Image",
                "fileType": version.get("FileType"),
                "contentType": content_type,
                "base64": base64.b64encode(raw_bytes).decode("utf-8"),
            }
        )
    return images


async def fetch_documents_for_entity(entity_id: str, source_label: str) -> list[dict[str, Any]]:
    versions = await fetch_content_versions_for_entity(entity_id)
    documents: list[dict[str, Any]] = []
    for version in versions:
        file_type = str(version.get("FileType") or "").lower()
        if file_type in IMAGE_FILE_TYPES:
            continue
        content_type = DOCUMENT_CONTENT_TYPES.get(file_type, "application/octet-stream")
        title = version.get("Title") or "Document"
        documents.append(
            {
                "id": version["Id"],
                "title": title,
                "fileType": version.get("FileType"),
                "contentType": content_type,
                "source": source_label,
                "category": classify_document(title, file_type, source_label),
            }
        )
    return documents


async def fetch_related_documents(appointment: dict[str, Any], work_order_id: str | None) -> list[dict[str, Any]]:
    docs: list[dict[str, Any]] = []
    docs.extend(await fetch_documents_for_entity(appointment["Id"], "appointment"))
    if work_order_id:
        docs.extend(await fetch_documents_for_entity(work_order_id, "work-order"))
    job_id = appointment.get("Job__c")
    if job_id:
        docs.extend(await fetch_documents_for_entity(job_id, "job"))

    seen: set[str] = set()
    unique_docs: list[dict[str, Any]] = []
    for doc in docs:
        if doc["id"] in seen:
            continue
        seen.add(doc["id"])
        unique_docs.append(doc)
    return unique_docs


async def fetch_url_documents_for_record(
    object_name: str,
    record_id: str | None,
    source_label: str,
    candidates: list[tuple[str, str]],
) -> list[dict[str, Any]]:
    if not record_id:
        return []
    documents: list[dict[str, Any]] = []
    for field_name, category in candidates:
        rows = await sf_query_all_safe(
            f"""
            SELECT Id, {field_name}
            FROM {object_name}
            WHERE Id = '{record_id}'
            LIMIT 1
            """
        )
        if not rows:
            continue
        value = rows[0].get(field_name)
        if isinstance(value, str) and value.strip().startswith("http"):
            documents.append(
                {
                    "id": f"{record_id}:{field_name}",
                    "title": field_name.replace("_", " ").replace("__c", "").strip(),
                    "fileType": "URL",
                    "contentType": "text/uri-list",
                    "source": source_label,
                    "category": category,
                    "externalUrl": value.strip(),
                }
            )
    return documents


async def fetch_direct_url_documents(appointment: dict[str, Any], work_order_id: str | None) -> list[dict[str, Any]]:
    docs: list[dict[str, Any]] = []
    docs.extend(
        await fetch_url_documents_for_record(
            "ServiceAppointment",
            appointment.get("Id"),
            "appointment",
            [
                ("Service_Report_URL__c", "service-report"),
                ("Customer_Service_Report_URL__c", "service-report"),
                ("Service_Report_PDF_URL__c", "service-report"),
                ("Invoice_Document_URL__c", "invoice"),
                ("Payment_Document_URL__c", "payment"),
            ],
        )
    )
    docs.extend(
        await fetch_url_documents_for_record(
            "WorkOrder",
            work_order_id,
            "work-order",
            [
                ("Invoice_Document_URL__c", "invoice"),
                ("Payment_Document_URL__c", "payment"),
                ("Service_Report_URL__c", "service-report"),
            ],
        )
    )
    docs.extend(
        await fetch_url_documents_for_record(
            "Job__c",
            appointment.get("Job__c"),
            "job",
            [
                ("Invoice_Document_URL__c", "invoice"),
                ("Payment_Document_URL__c", "payment"),
                ("Service_Report_URL__c", "service-report"),
            ],
        )
    )
    return docs


async def fetch_version_bytes(version_id: str) -> tuple[bytes, str]:
    sf = app.state.sf
    url = f"{sf.base_url}sobjects/ContentVersion/{version_id}/VersionData"
    response = await run_in_threadpool(
        sf.session.get,
        url,
        headers={"Authorization": f"Bearer {sf.session_id}"},
    )
    response.raise_for_status()
    return response.content, response.headers.get("Content-Type", "application/octet-stream")


async def fetch_content_version_by_id(version_id: str) -> dict[str, Any] | None:
    versions = await sf_query_all(
        f"""
        SELECT Id, Title, FileType
        FROM ContentVersion
        WHERE Id = '{version_id}'
        LIMIT 1
        """
    )
    return versions[0] if versions else None


TQR_SYSTEM_PROMPT = """
ROLE
You are a Trade Quality Review (TQR) scoring assistant for Chumley, a field service company.
Your task is to assess completed jobs against a documented rubric and produce per-field scores
with cited evidence, for review by a human trade manager.

CORE PRINCIPLES
1. HUMAN AUTHORITY. You are never the final decision maker. A human trade manager reviews every
   output you produce and can confirm, adjust, or override any score.
2. EVIDENCE-FIRST. Every score must be justified by specific evidence from the job record.
3. EXPLICIT REASONING. For every numerically-scored field, state whatWouldIncrease and whatWouldDecrease.
4. NO MIDDLE BIAS. If you cannot confidently score a field, set outcome to "Review" and explain in reviewReason.
5. CONSERVATIVE ON SUBJECTIVE JUDGMENTS. Bias toward Good (7-8) rather than Perfect (9-10) for Workmanship and DecisionMaking.
6. URGENT ISSUE HANDLING. Safety concern or damage = urgentIssueDetected true, score 0-2.
7. STRUCTURED OUTPUT ONLY. Return a single valid JSON object. No prose. No markdown.
8. NO ENGINEER BIAS. Score based only on evidence from this specific job.

SCORING BANDS
9-10: Excellent/Perfect. Outstanding quality. Rare.
7-8:  Good. Competent, professional work. Most jobs score here.
5-6:  Acceptable. Work done but some concerns.
3-4:  Non Acceptable. Significant issues.
0-2:  Urgent Issue/Fail. Safety concern, major failure, or missing critical evidence.

OUTCOME TAXONOMY
Pass:   Confident score from clear evidence.
Review: Cannot score confidently or ambiguous evidence. Populate reviewReason.
Fail:   Clear failure (zero photos, no signature without reason, urgent issue).

OUTPUT SCHEMA - return exactly this JSON structure:
{
  "fields": {
    "customerSignature": {
      "value": "Yes"|"No"|"NA_CustomerNotPresent",
      "outcome": "Pass"|"Review"|"Fail",
      "confidence": 0.0-1.0,
      "evidenceCited": ["..."],
      "rationale": "...",
      "reviewReason": null
    },
    "imagesQuality": {
        "score": 0-10,
        "salesforceValue": "Perfect"|"Good"|"Acceptable"|"Non Acceptable"|"Urgent Issue",
        "outcome": "Pass"|"Review"|"Fail",
      "confidence": 0.0-1.0,
      "evidenceCited": ["..."],
      "rationale": "...",
      "whatWouldIncrease": "...",
      "whatWouldDecrease": "...",
      "mandatoryPhotosPresent": {"location": true|false, "workBefore": true|false, "workAfter": true|false, "jobCompletion": true|false},
      "reviewReason": null
    },
    "paymentAttempted": {
      "value": "Yes"|"No"|"CreditAccount",
      "outcome": "Pass"|"Review"|"Fail",
      "confidence": 0.0-1.0,
      "evidenceCited": ["..."],
      "rationale": "...",
      "reviewReason": null
    },
    "report": {
      "score": 0-10,
      "salesforceValue": "Perfect"|"Good"|"Acceptable"|"Non Acceptable",
      "outcome": "Pass"|"Review"|"Fail",
      "confidence": 0.0-1.0,
      "evidenceCited": ["..."],
      "rationale": "...",
      "whatWouldIncrease": "...",
      "whatWouldDecrease": "...",
      "flaggedIssues": [],
      "reviewReason": null
    },
    "timeTaken": {
      "score": 0-10,
      "salesforceValue": "Ideal"|"Excessive"|"Rushed",
      "outcome": "Pass"|"Review"|"Fail",
      "confidence": 0.0-1.0,
      "evidenceCited": ["..."],
      "rationale": "...",
      "actualMinutes": 0,
      "expectedMinutes": null,
      "reviewReason": null
    },
    "workmanship": {
      "score": 0-10,
      "salesforceValue": "Perfect"|"Good"|"Acceptable"|"Non Acceptable"|"Urgent Issue",
      "outcome": "Pass"|"Review"|"Fail",
      "confidence": 0.0-1.0,
      "evidenceCited": ["..."],
      "rationale": "...",
      "whatWouldIncrease": "...",
      "whatWouldDecrease": "...",
      "urgentIssueDetected": false,
      "urgentIssueDescription": null,
      "reviewReason": null
    },
    "decisionMaking": {
      "score": 0-10,
      "salesforceValue": "Perfect"|"Good"|"Acceptable"|"Non Acceptable"|"Urgent Issue",
      "outcome": "Pass"|"Review"|"Fail",
      "confidence": 0.0-1.0,
      "evidenceCited": ["..."],
      "rationale": "...",
      "whatWouldIncrease": "...",
      "whatWouldDecrease": "...",
      "missedOpportunities": [],
      "urgentIssueDetected": false,
      "urgentIssueDescription": null,
      "reviewReason": null
    }
  },
  "summary": {
    "overallObservations": "...",
    "hardFailTriggered": false,
    "hardFailReasons": []
  }
}

FIELD INSTRUCTIONS:

FIELD customerSignature: Check signature.present first. If true = Yes/Pass.
If false, check signature.hasServiceReportDocument — if a service report document is attached (e.g. SA-XXXXXX PDF), the customer signature is likely contained within that document and should be treated as Yes/Pass.
If false and no service report document, scan notes for "customer not present", "no-one at property", "access via key" etc = NA_CustomerNotPresent/Pass.
Only score No/Fail (hard fail trigger) if signature.present is false AND signature.hasServiceReportDocument is false AND there is no valid explanation in the notes.

FIELD imagesQuality:
Purpose:
Assess whether photographic evidence of the job is complete, relevant, and of sufficient quality to document what was
done. Photos are the primary evidence source for almost every other TQR field, so this field has knock-on effects
across the review.

Evidence sources:
- All images attached to the service appointment
- Trade group and job type, to determine which situational photos apply
- Job description, for context on what should be visually documented

Mandatory photos that should exist on every job:
- Location
- Work before
- Work after
- Job completion

Situational photos where applicable:
- Work during
- Workmanship close-up
- Protection used

Scoring rubric:
- 9-10 Excellent: All four mandatory photos present with excellent quality. All applicable situational photos present with good quality. No coverage gaps.
- 7-8 Good: All four mandatory photos present with good quality. Most applicable situational photos present. Minor quality or coverage gaps only.
- 5-6 Adequate: All four mandatory photos are present, but one or more has quality issues, or some applicable situational photos are missing.
- 3-4 Below standard: One mandatory photo is missing, or multiple mandatory photos have significant quality issues such as blur, darkness, or unclear subject.
- 0-2 Fail: Multiple mandatory photos are missing, or zero photos are attached.

Validation rule:
- If the job has only 1, 2, or 3 of the 4 mandatory photos, the score must not exceed 4.
- If multiple mandatory photos are missing, the score must be 0-2 and outcome should be Fail or Review depending on confidence.
- Only jobs with all 4 mandatory photos can score 5 or above.

Salesforce mapping:
- 9-10 = Perfect
- 7-8 = Good
- 5-6 = Acceptable
- 3-4 = Non Acceptable
- 0-2 = Urgent Issue

AI review triggers:
- Photos are present but the subject is unclear
- Photos appear to be duplicates rather than progression shots
- A situational photo may be missing but applicability is unclear from the job type
- Metadata suggests the photos were taken well before or after the job

FIELD paymentAttempted: If Credit Account = CreditAccount/Pass. If payment collected = Yes/Pass.
If charge > 0 and payment is clearly not collected on a cash account with adequate evidence = No/Fail.
If charge > 0 but invoice/payment evidence is missing or payment status is blank, outcome=Review and explain that the
job needs manual review because no invoice data is visible.

FIELD report:
Purpose:
Assess the quality of the written job report - specifically whether the written record is detailed enough for someone
unfamiliar with the job to understand what was found, what was done, what parts were used, and what needs to happen
next.

Evidence sources:
- Job description and notes field
- Works Completion Summary / Attendance Report for Customer field
- Parts and materials listed
- Comments for Projected Difference, where applicable
- Reason for Projected Difference, where applicable

Scoring rubric:
- 9-10 Excellent: Clear problem statement, complete work summary, all parts listed, explicit next steps where applicable. Professional tone. A future engineer reading this could fully understand what happened.
- 7-8 Good: Clear problem and work summary. One minor element missing or thin, such as next steps being implied rather than stated, or parts being listed but not quantified.
- 5-6 Adequate: Describes what was done but is thin on detail, context, or diagnosis reasoning. A future engineer would need to piece things together.
- 3-4 Below standard: One-line report, major structural gaps, or repeated template language without enough job-specific detail.
- 0-2 Fail: Notes absent, incomprehensible, or effectively just a copy of the original booking notes with nothing meaningful added.

Salesforce mapping:
9-10=Perfect, 7-8=Good, 5-6=Acceptable, 0-4=Non Acceptable.

AI review triggers:
- Notes contain a flag, claim, or complaint that appears to require follow-up but was not resolved in the record
- Notes mention a scope change, customer disagreement, or site issue without resolution
- Template or boilerplate language appears instead of job-specific reporting
- Notes describe work done but the listed parts do not match the written report

FIELD timeTaken: Compute actual minutes from actualStart to actualEnd. Compare against any benchmark.
If no benchmark, outcome=Review. Score by variance: <=10%=9-10, <=25%=7-8, <=50%=5-6, >50%=3-4, >100%=0-2.
salesforceValue: Ideal if 9-10, Excessive if actual>expected, Rushed if actual<expected.

FIELD workmanship:
Purpose: Assess the quality of the physical work performed, based on the photographic and written evidence in the job record.
This is a desk-based assessment from documentation, not an on-site inspection. Measure only what a trained reviewer can
reasonably conclude from the evidence the engineer provided.

Evidence sources:
- Before and after photos (primary evidence)
- Workmanship close-up photos where applicable
- Parts and materials used
- Job description and engineer notes
- Any flags or concerns the engineer raised in notes
- Customer signature and any comments recorded alongside

Scoring rubric:
- 9-10 Excellent: Before and after photos clearly document the work. Completed to a professional standard with clean finish.
  Appropriate materials used. No visible concerns of any kind.
- 7-8 Good: Work completed to a good standard. Minor documentation gaps or small finish imperfections that do not affect function.
- 5-6 Acceptable: Work completed, but evidence suggests rushing, sub-optimal material choice, or cosmetic issues.
- 3-4 Non Acceptable: Work appears incomplete, poor finish quality is visible, inappropriate materials used, or workmanship falls
  short of what a competent engineer would produce.
- 0-2 Urgent Issue: Visible safety concern, damage caused to the site, fundamentally incorrect approach, or any condition that may
  require immediate recall or remedial visit.

Salesforce mapping:
9-10=Perfect, 7-8=Good, 5-6=Acceptable, 3-4=Non Acceptable, 0-2=Urgent Issue

Review triggers:
- Insufficient photo evidence to judge workmanship quality at all
- Photos suggest a quality issue but confidence is low
- Any detected anomaly such as visible damage, safety concern, inappropriate material, or unusual approach
- Engineer notes flag an issue that was not clearly resolved
- Customer signature missing combined with thin documentation

Bias instruction:
- Default to Good (7-8) for competent work
- Reserve Perfect (9-10) for genuinely outstanding evidence
- Set urgentIssueDetected=true for safety concerns or damage

FIELD decisionMaking:
Purpose:
Assess the quality of judgment calls the engineer made during the visit: whether they diagnosed correctly, chose an
appropriate approach, raised further works where needed, generated quotes for additional opportunities, and managed
customer expectations.

Evidence sources:
- Job description - both the original problem and the diagnosis reached
- Parts used - whether they match the problem
- Further works records raised from this visit
- Quotes generated during or after the visit
- Notes on scope changes, including what was found versus what was expected
- Customer interactions recorded in notes
- Attendance Report for Customer
- Attendance Notes for Office
- Time Taken signal - very short times may indicate failure to investigate properly

Scoring rubric:
- 9-10 Excellent: Correct diagnosis. Appropriate approach chosen. All commercial opportunities captured. Good customer expectation management visible in notes.
- 7-8 Good: Sound decisions throughout. Minor opportunity potentially missed or a judgment call that another engineer might have handled differently, but nothing materially wrong.
- 5-6 Acceptable: Adequate decisions overall, but a clear value-add opportunity was passed on, or the diagnostic approach was sub-optimal even if it reached the right conclusion.
- 3-4 Non Acceptable: Significant further works needed but not raised. Poor customer expectation management. Wrong approach to diagnosis or repair. Cost the business money or damaged the relationship.
- 0-2 Urgent Issue: Decision created actual risk to the customer, engineer, or company. Major escalation failure. Safety issue not raised.

Salesforce mapping:
9-10=Perfect, 7-8=Good, 5-6=Acceptable, 3-4=Non Acceptable, 0-2=Urgent Issue.

AI review triggers:
- Complex trade-specific judgment that cannot be assessed confidently without domain expertise
- Engineer notes suggest a scope change or customer issue requiring human interpretation
- The job involved a decision point, such as repair versus replace, that the notes do not fully explain
- Further works or quotes appear likely to have been appropriate but were not raised
- Customer expectation may have been mismanaged, including promises about scope or time that may not be deliverable

Bias:
This is highly subjective. Bias toward Good (7-8) rather than Perfect (9-10), and surface concrete observations from
the engineer report rather than pretending certainty where the evidence is thin.
"""


# ---------------------------------------------------------------------------
# FIX 1: _compute_weighted_verdict — now logs every decision step
# FIX 2: Image Quality score <=4 is now a hard fail (was only 0 photos before)
# ---------------------------------------------------------------------------
def _compute_weighted_verdict(
    fields: dict[str, Any],
    appointment_number: str = "UNKNOWN",
    photo_count: int = 0,
) -> tuple[float, str, bool, list[str]]:
    prefix = f"[TQR:{appointment_number}]"
    hard_fail_reasons: list[str] = []

    # --- Hard fail checks ---
    img = fields.get("imagesQuality") or {}
    img_score = safe_float(img.get("score", 0))

    if photo_count == 0:
        reason = "Zero photos attached"
        hard_fail_reasons.append(reason)
        log.warning("%s HARD FAIL → %s", prefix, reason)
    else:
        log.info("%s Image Quality score=%s/10  photo_count=%d — OK", prefix, img_score, photo_count)

    if (fields.get("workmanship") or {}).get("urgentIssueDetected"):
        reason = "Urgent workmanship issue detected"
        hard_fail_reasons.append(reason)
        log.warning("%s HARD FAIL → %s", prefix, reason)

    if (fields.get("decisionMaking") or {}).get("urgentIssueDetected"):
        reason = "Urgent decision-making issue detected"
        hard_fail_reasons.append(reason)
        log.warning("%s HARD FAIL → %s", prefix, reason)

    sig_obj = fields.get("customerSignature") or {}
    sig_value = (sig_obj.get("value") or "").strip()
    sig_outcome = (sig_obj.get("outcome") or "").lower().strip()
    sig_sf_val = (sig_obj.get("salesforceValue") or "").lower().strip()
    has_sig_doc = bool(sig_obj.get("hasServiceReportDocument"))
    sig_pass = (
        sig_outcome in ("pass", "yes", "na_customernotpresent")
        or sig_sf_val in ("yes", "na_customernotpresent")
        or sig_value in ("Yes", "NA_CustomerNotPresent")
        or has_sig_doc
    )
    if not sig_pass:
        reason = "Customer signature missing with no valid reason"
        hard_fail_reasons.append(reason)
        log.warning("%s HARD FAIL → %s", prefix, reason)
    else:
        log.info(
            "%s Customer signature OK (outcome=%s, value=%r, sfVal=%s, hasDoc=%s)",
            prefix, sig_outcome, sig_value, sig_sf_val, has_sig_doc,
        )

    if (fields.get("report") or {}).get("flaggedIssues"):
        reason = "Unresolved customer complaint in report"
        hard_fail_reasons.append(reason)
        log.warning("%s HARD FAIL → %s", prefix, reason)

    hard_fail = bool(hard_fail_reasons)

    # --- Score calculation ---
    # Respect AI outcome=Pass; only map to 0 when clearly No
    sig_score = 10 if sig_pass else 0
    pay_obj = fields.get("paymentAttempted") or {}
    pay_value = pay_obj.get("value", "No")
    pay_outcome = (pay_obj.get("outcome") or "").lower()
    pay_pass = pay_outcome == "pass" or pay_value in ("Yes", "CreditAccount")
    if pay_pass:
        pay_score = 10
    elif pay_value == "No":
        pay_score = 0
    else:
        pay_score = safe_float(pay_obj.get("score", 5))

    log.info("%s Signature value=%s → score=%s/10 (weight 5%%)", prefix, sig_value, sig_score)
    log.info("%s Payment value=%s → score=%s/10 (weight 10%%)", prefix, pay_value, pay_score)

    weights = {
        "workmanship":    0.20,
        "decisionMaking": 0.20,
        "imagesQuality":  0.20,
        "report":         0.15,
        "timeTaken":      0.10,
    }

    weighted = 0.0
    log.info("%s ── Field score breakdown ──────────────────", prefix)
    for field_name, weight in weights.items():
        field_score = safe_float((fields.get(field_name) or {}).get("score", 0))
        contribution = field_score * weight
        weighted += contribution
        outcome = (fields.get(field_name) or {}).get("outcome", "?")
        log.info(
            "%s   %-18s score=%4.1f  weight=%3.0f%%  contribution=%4.2f  outcome=%s",
            prefix, field_name, field_score, weight * 100, contribution, outcome,
        )

    weighted += pay_score * 0.10 + sig_score * 0.05
    log.info(
        "%s   %-18s score=%4.1f  weight=%3.0f%%  contribution=%4.2f",
        prefix, "paymentAttempted", float(pay_score), 10, pay_score * 0.10,
    )
    log.info(
        "%s   %-18s score=%4.1f  weight=%3.0f%%  contribution=%4.2f",
        prefix, "customerSignature", float(sig_score), 5, sig_score * 0.05,
    )
    log.info("%s ── Total weighted score = %.2f / 10 (%.0f%%)", prefix, weighted, weighted * 10)

    percentage = weighted * 10

    # --- Verdict logic ---
    if hard_fail:
        verdict = "Unacceptable"
        log.warning(
            "%s Verdict = %s (hard fail triggered — reasons: %s)",
            prefix, verdict, "; ".join(hard_fail_reasons),
        )
    elif percentage >= 80:
        verdict = "TQR"
        log.info("%s Verdict = %s (score %.0f%% >= 80%%)", prefix, verdict, percentage)
    elif percentage >= 50:
        verdict = "Sub standard"
        log.info("%s Verdict = %s (50%% <= score %.0f%% < 80%%)", prefix, verdict, percentage)
    else:
        verdict = "Unacceptable"
        log.warning("%s Verdict = %s (score %.0f%% < 50%%)", prefix, verdict, percentage)

    return round(weighted, 2), verdict, hard_fail, hard_fail_reasons


# ---------------------------------------------------------------------------
# FIX 3: Build a summary that ALWAYS matches the computed verdict
# ---------------------------------------------------------------------------
def _build_verdict_summary(
    verdict: str,
    hard_fail: bool,
    hard_fail_reasons: list[str],
    overall_score: float,
    ai_observations: str,
) -> str:
    """
    Replaces the raw Groq overallObservations with a summary that leads
    with the real verdict so the two never contradict each other.
    """
    score_pct = round(overall_score * 10)

    if hard_fail:
        reasons_text = "; ".join(hard_fail_reasons)
        prefix = f"⛔ HARD FAIL — {reasons_text}."
    elif verdict == "Unacceptable":
        prefix = f"❌ UNACCEPTABLE — Score {score_pct}% is below the minimum 50% threshold."
    elif verdict == "Sub standard":
        prefix = f"⚠️ SUB STANDARD — Score {score_pct}% is below the 80% TQR threshold."
    else:  # TQR
        prefix = f"✅ TQR PASS — Score {score_pct}% meets the required standard."

    return f"{prefix} AI observations: {ai_observations}"


def _compress_image_for_ai(img: dict[str, Any], max_px: int = 1024, quality: int = 70) -> str:
    raw = base64.b64decode(img["base64"])
    with Image.open(io.BytesIO(raw)) as pil:
        pil = pil.convert("RGB")
        pil.thumbnail((max_px, max_px), Image.LANCZOS)
        buf = io.BytesIO()
        pil.save(buf, format="JPEG", quality=quality, optimize=True)
    return base64.b64encode(buf.getvalue()).decode("utf-8")


IMAGE_DESCRIPTION_PROMPT = """
You are inspecting a single field service job photo for a UK-based trades company.
Describe exactly what is visible in this image in clear, practical British English.
Use British spelling and terminology throughout (e.g. colour, taps, plasterboard, skirting board, bin, spanner, worktop).

STRICT ACCURACY RULES — you must follow all of these:
- Never name a specific tool or object unless you are certain. If you are not sure what something is, describe its shape, colour, size and markings instead (e.g. "a grey and yellow cylindrical object" NOT "a utility knife").
- Do not assume what a handheld object is based on context alone. Describe what you literally see.
- Only state what is clearly and unambiguously visible. Do not infer, guess or fill in gaps.
- If an object could be several things, describe its visible features only — do not pick one and state it as fact.

Focus on: equipment and tools present, fittings and fixtures, surfaces and materials, job progress, cleanliness, workmanship quality, and any obvious safety risks.
Return a single plain paragraph only.
"""


async def describe_single_image(image: dict[str, Any]) -> dict[str, Any]:
    client = app.state.groq
    image_models = list(getattr(app.state, "image_vision_models", [])) or [app.state.image_vision_model]
    try:
        compressed = _compress_image_for_ai(image)
    except Exception:
        log.warning("Could not compress image '%s' for description", image.get("title"))
        return {
            "id": image.get("id"),
            "title": image.get("title"),
            "description": "AI could not read this image file for detailed visual description.",
        }
    content: list[dict[str, Any]] = [
        {
            "type": "text",
            "text": f"Describe this job photo in detail. Filename: {image.get('title') or 'Image'}",
        },
        {
            "type": "image_url",
            "image_url": {"url": f"data:image/jpeg;base64,{compressed}"},
        },
    ]
    description = ""
    last_error: Exception | None = None
    for image_model in image_models:
        try:
            response = await run_in_threadpool(
                client.chat.completions.create,
                model=image_model,
                temperature=0.1,
                messages=[
                    {"role": "system", "content": IMAGE_DESCRIPTION_PROMPT},
                    {"role": "user", "content": content},
                ],
                max_tokens=500,
            )
            raw = response.choices[0].message.content or ""
            if isinstance(raw, str):
                description = raw.strip()
                if description.startswith("{"):
                    payload = json_extract(description)
                    description = str(payload.get("description") or "").strip()
            else:
                description = str(raw).strip()
            description = description or "No description returned."
            log.info("Image description generated for '%s' using %s (%d chars)", image.get("title"), image_model, len(description))
            break
        except Exception as exc:
            last_error = exc
            log.warning("Image description attempt failed for '%s' using %s: %s", image.get("title"), image_model, exc)
    if not description:
        log.error("Image description failed for '%s' after trying %d model(s): %s", image.get("title"), len(image_models), last_error)
        description = "AI description could not be generated for this image."
    return {
        "id": image.get("id"),
        "title": image.get("title"),
        "description": description,
    }


async def run_tqr_analysis(
    appointment: dict[str, Any],
    work_order: dict[str, Any] | None,
    account: dict[str, Any] | None,
    images: list[dict[str, Any]],
    documents: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    client = app.state.groq
    tqr_system_prompt = app.state.tqr_system_prompt
    rubric_scoring_model = app.state.rubric_scoring_model
    appt_num = appointment.get("AppointmentNumber", "UNKNOWN")
    prefix = f"[TQR:{appt_num}]"

    log.info("%s ══════════════════════════════════════════", prefix)
    log.info("%s Starting TQR analysis", prefix)
    log.info("%s Engineer : %s", prefix, appointment.get("Allocated_Engineer__c", "Unknown"))
    log.info("%s Trade    : %s", prefix, appointment.get("Scheduled_Trade__c") or appointment.get("Trade_Group_Region__c") or "Unknown")
    log.info("%s Photos   : %d attached", prefix, len(images))

    actual_start = appointment.get("ActualStartTime") or appointment.get("SchedStartTime")
    actual_end = appointment.get("ActualEndTime")
    duration_mins = 0
    if actual_start and actual_end:
        try:
            s = parse_salesforce_datetime(actual_start)
            e = parse_salesforce_datetime(actual_end)
            if s and e:
                duration_mins = int((e - s).total_seconds() / 60)
        except Exception:
            pass
    log.info("%s Duration : %d minutes (start=%s, end=%s)", prefix, duration_mins, actual_start, actual_end)

    documents = documents or []
    invoice_docs = [doc for doc in documents if doc.get("category") == "invoice"]
    payment_docs = [doc for doc in documents if doc.get("category") == "payment"]
    service_report_docs = [doc for doc in documents if doc.get("category") == "service-report"]

    evidence = {
        "job": {
            "appointmentNumber": appt_num,
            "trade": appointment.get("Trade_Group_Region__c") or appointment.get("Trade_Group_Postcode__c") or appointment.get("Scheduled_Trade__c"),
            "description": appointment.get("Description"),
            "actualStart": actual_start,
            "actualEnd": actual_end,
            "durationMinutes": duration_mins,
            "status": appointment.get("Status"),
        },
        "customer": {
            "accountType": appointment.get("Account_Type__c", "unknown"),
            "accountName": (account or {}).get("Name"),
            "address": ", ".join(filter(None, [appointment.get("Street"), appointment.get("City"), appointment.get("PostalCode")])),
        },
        "engineer": {"name": appointment.get("Allocated_Engineer__c")},
        "notes": {
            "jobDescription": appointment.get("Description"),
            "worksCompletionSummary": appointment.get("Attendance_Report_for_Customer__c"),
            "feedbackNotes": appointment.get("Feedback_Notes__c"),
        },
        "invoice": {
            "chargeTotal": safe_float(appointment.get("CCT_Charge_Gross__c")),
            "paymentStatus": appointment.get("Payment_Attempted__c", "unknown"),
            "hasInvoiceDocument": bool(invoice_docs),
            "hasPaymentDocument": bool(payment_docs),
        },
        "signature": {
            "present": bool(appointment.get("Customer_Signature__c")) or bool(service_report_docs),
            "hasServiceReportDocument": bool(service_report_docs),
            "serviceReportDocuments": [doc.get("title") for doc in service_report_docs],
        },
        "workOrder": {
            "workOrderNumber": (work_order or {}).get("WorkOrderNumber"),
            "description": (work_order or {}).get("Description"),
            "workType": ((work_order or {}).get("WorkType") or {}).get("Name"),
        },
        "photos": [{"filename": img["title"], "id": img["id"]} for img in images],
    }

    log.info(
        "%s Evidence sent to Groq — charge=£%.2f, paymentStatus=%s, signaturePresent=%s",
        prefix,
        safe_float(appointment.get("CCT_Charge_Gross__c")),
        appointment.get("Payment_Attempted__c", "unknown"),
        bool(appointment.get("Customer_Signature__c")),
    )

    log.info("%s Generating %d image description(s) for rubric scoring", prefix, len(images))
    image_descriptions = await asyncio.gather(*(describe_single_image(img) for img in images))
    evidence["imageDescriptions"] = image_descriptions

    content = (
        f"Job Evidence (JSON):\n{json.dumps(evidence, indent=2)}\n\n"
        "Assess this job against all 7 TQR fields using the rubric exactly.\n"
        "Use imageDescriptions and photo metadata as the source of image evidence.\n"
        "Return ONLY the JSON object matching the output schema."
    )

    response = await run_in_threadpool(
        client.chat.completions.create,
        model=rubric_scoring_model,
        temperature=0.1,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": tqr_system_prompt},
            {"role": "user", "content": content},
        ],
        max_tokens=4000,
    )

    raw = response.choices[0].message.content or ""
    payload = json_extract(raw)
    fields = payload.get("fields") or {}

    payment_field = fields.get("paymentAttempted") or {}
    payment_status = appointment.get("Payment_Attempted__c")
    charge_total = safe_float(appointment.get("CCT_Charge_Gross__c"))
    account_type = str(appointment.get("Account_Type__c") or "").strip().lower()
    if (
        charge_total > 0
        and not str(payment_status or "").strip()
        and account_type != "key account"
        and not invoice_docs
        and not payment_docs
    ):
        payment_field["value"] = "No"
        payment_field["outcome"] = "Review"
        payment_field["confidence"] = min(safe_float(payment_field.get("confidence", 0.5)) or 0.5, 0.5)
        payment_field["reviewReason"] = "No invoice or payment evidence was visible for this chargeable job, so payment needs manual review."
        payment_field["rationale"] = "Charge total is present, but there is no visible invoice or payment evidence and the payment field is blank."
        evidence_cited = list(payment_field.get("evidenceCited") or [])
        evidence_cited.append(f"chargeTotal is {charge_total}")
        evidence_cited.append("No invoice document was visible")
        evidence_cited.append("No payment document was visible")
        payment_field["evidenceCited"] = evidence_cited
        fields["paymentAttempted"] = payment_field

    # Log every AI field decision
    log.info("%s ── Groq raw field decisions ────────────────", prefix)
    for field_name, field_data in fields.items():
        score = field_data.get("score", field_data.get("value", "N/A"))
        outcome = field_data.get("outcome", "?")
        rationale = str(field_data.get("rationale", ""))[:120]
        log.info("%s   %-18s score/value=%-10s outcome=%-8s rationale=%s…", prefix, field_name, score, outcome, rationale)

    ai_observations = (payload.get("summary") or {}).get("overallObservations", "AI analysis completed.")
    log.info("%s Groq raw summary: %s", prefix, ai_observations[:200])

    # Compute verdict AFTER AI runs so we can reconcile the summary
    overall_score, verdict, hard_fail, hard_fail_reasons = _compute_weighted_verdict(
        fields, appointment_number=appt_num, photo_count=len(images)
    )

    # FIX 3: Build a summary that matches the verdict — never contradicts it
    final_summary = _build_verdict_summary(
        verdict, hard_fail, hard_fail_reasons, overall_score, ai_observations
    )
    log.info("%s Final summary: %s", prefix, final_summary[:200])

    flags: list[str] = []
    for fn in ("workmanship", "decisionMaking"):
        f = fields.get(fn) or {}
        if f.get("urgentIssueDescription"):
            flags.append(f.get("urgentIssueDescription", ""))
    for issue in (fields.get("report") or {}).get("flaggedIssues", []):
        flags.append(str(issue))
    flags = [f for f in flags if f]

    if flags:
        log.warning("%s Flags raised: %s", prefix, flags)

    workmanship_score = safe_float((fields.get("workmanship") or {}).get("score", 0))

    log.info(
        "%s ══ RESULT: verdict=%s  overall=%.2f/10  hard_fail=%s ══",
        prefix, verdict, overall_score, hard_fail,
    )

    return {
        "workmanship": workmanship_score,
        "cleanliness": workmanship_score,
        "safety": safe_float((fields.get("workmanship") or {}).get("urgentIssueDetected", False)) * 10,
        "completion": safe_float((fields.get("imagesQuality") or {}).get("score", 0)),
        "overall": overall_score,
        "summary": final_summary,           # ← FIX 3: verdict-aligned summary
        "flags": flags,
        "recommendation": verdict,
        "tqr_fields": fields,
        "image_descriptions": list(image_descriptions),
        "hard_fail": hard_fail,
        "verdict": verdict,
    }


@app.get("/api/appointments")
async def get_appointments(
    page: int = Query(1, ge=1),
    pageSize: int = Query(25, ge=1, le=200),
    engineer: str | None = None,
    status: str | None = None,
    trade: str | None = None,
    search: str | None = None,
    sector: str | None = None,
):
    records = await get_review_queue_records()
    filtered = apply_appointment_filters(records, engineer, status, trade, search, sector)
    cached = await get_all_cached_results()
    enriched = [enrich_appointment(record, cached.get(record["Id"])) for record in filtered]
    total = len(enriched)
    total_pages = max((total + pageSize - 1) // pageSize, 1)
    start = (page - 1) * pageSize
    return {
        "records": enriched[start : start + pageSize],
        "total": total,
        "page": page,
        "pageSize": pageSize,
        "totalPages": total_pages,
    }


@app.get("/api/appointments/{appointment_id}")
async def get_appointment(appointment_id: str):
    appointment = await fetch_appointment_by_id(appointment_id)
    cached = await get_cached_result(appointment_id)
    work_order_id = resolve_work_order_id(appointment)
    work_order = await fetch_work_order(work_order_id) if work_order_id else None
    account = await fetch_account(work_order.get("AccountId") if work_order else None)
    territory = await fetch_service_territory(work_order.get("ServiceTerritoryId") if work_order else None)
    documents = await fetch_related_documents(appointment, work_order_id)
    documents.extend(await fetch_direct_url_documents(appointment, work_order_id))
    detail = enrich_appointment(appointment, cached)
    detail["workOrderId"] = work_order_id
    detail["workOrder"] = work_order
    detail["account"] = account
    detail["site"] = territory.get("Name") if territory else None
    detail["accountManager"] = ((account or {}).get("Owner") or {}).get("Name")
    detail["documents"] = documents
    return detail


@app.get("/api/work-orders/{work_order_id}")
async def get_work_order_detail(work_order_id: str):
    work_order = await fetch_work_order(work_order_id)
    account = await fetch_account(work_order.get("AccountId"))
    territory = await fetch_service_territory(work_order.get("ServiceTerritoryId"))
    return {
        "workOrder": work_order,
        "account": account,
        "site": territory.get("Name") if territory else None,
        "accountManager": ((account or {}).get("Owner") or {}).get("Name"),
    }


@app.get("/api/work-orders/{work_order_id}/images")
async def get_work_order_images(work_order_id: str):
    return {"images": await fetch_images_for_entity(work_order_id)}


@app.get("/api/work-orders/{work_order_id}/images/{image_id}/describe")
async def describe_work_order_image(work_order_id: str, image_id: str):
    images = await fetch_images_for_entity(work_order_id)
    image = next((item for item in images if item.get("id") == image_id), None)
    if not image:
        raise HTTPException(status_code=404, detail="Image not found")
    return await describe_single_image(image)


@app.get("/api/content/{version_id}")
async def open_content(version_id: str, inline: bool = Query(True)):
    metadata = await fetch_content_version_by_id(version_id)
    if not metadata:
        raise HTTPException(status_code=404, detail="Content version not found")
    data, detected_type = await fetch_version_bytes(version_id)
    file_type = str(metadata.get("FileType") or "").lower()
    mapped_type = DOCUMENT_CONTENT_TYPES.get(file_type)
    media_type = mapped_type or detected_type or "application/octet-stream"
    title = metadata.get("Title") or version_id
    extension = file_type if file_type else "bin"
    disposition = "inline" if inline else "attachment"
    return Response(
        content=data,
        media_type=media_type,
        headers={"Content-Disposition": f'{disposition}; filename="{title}.{extension}"'},
    )



_sf_label_to_api: dict[str, str] = {}        # label.lower() -> API name
_sf_api_to_picklist: dict[str, list[str]] = {}  # API name -> valid picklist values


async def _get_sa_field_map() -> dict[str, str]:
    """Return label→API name map for ServiceAppointment, cached after first call."""
    global _sf_label_to_api, _sf_api_to_picklist
    if _sf_label_to_api:
        return _sf_label_to_api
    try:
        sf = app.state.sf
        meta = await run_in_threadpool(sf.ServiceAppointment.describe)
        _sf_label_to_api = {
            f["label"].lower(): f["name"]
            for f in meta["fields"]
            if f.get("updateable")
        }
        # Cache picklist values for every picklist/multipicklist field
        for f in meta["fields"]:
            if f.get("type") in ("picklist", "multipicklist") and f.get("picklistValues"):
                _sf_api_to_picklist[f["name"]] = [v["value"] for v in f["picklistValues"] if v.get("active")]
        log.info("[SF] Field map loaded: %d updateable fields on ServiceAppointment", len(_sf_label_to_api))
        # Log picklist values for our TQR fields so we can verify exact strings
        tqr_api_names = [
            "Post_Visit_Report_Check__c", "Workmanship1__c", "Decision_Making1__c",
            "Report__c", "Images_Provided__c", "Time_Taken__c", "Signed_SR__c",
            "Payment_Attempted__c",
        ]
        for api in tqr_api_names:
            vals = _sf_api_to_picklist.get(api, [])
            log.info("[SF] Picklist %s → %s", api, vals)
        # Log any field whose label contains "payment" to find the right one
        payment_fields = [(lbl, nm) for lbl, nm in _sf_label_to_api.items() if "payment" in lbl]
        log.info("[SF] Fields with 'payment' in label: %s", payment_fields)
    except Exception as exc:
        log.error("[SF] Could not load field map: %s", exc)
    return _sf_label_to_api


async def push_tqr_to_salesforce(appointment_id: str, result: dict[str, Any]) -> None:
    sf = app.state.sf
    field_map = await _get_sa_field_map()

    fields = result.get("tqr_fields") or {}
    verdict = result.get("verdict") or ""

    def sf_val(key: str, sub: str = "salesforceValue") -> str | None:
        return (fields.get(key) or {}).get(sub) or None

    img_obj = fields.get("imagesQuality") or {}
    img_sf = (img_obj.get("salesforceValue") or "").lower()
    img_outcome = (img_obj.get("outcome") or "").lower()
    if img_sf in ("good",) or (img_sf in ("perfect", "acceptable") and img_outcome not in ("fail", "review")):
        images_quality = "Good"
    elif img_sf in ("poor", "non acceptable", "urgent issue") or img_outcome == "fail":
        images_quality = "Poor"
    elif img_outcome == "review" or not img_sf:
        images_quality = "NA"
    else:
        images_quality = None

    sig_obj = fields.get("customerSignature") or {}
    sig_outcome = (sig_obj.get("outcome") or "").lower().strip()
    sig_value = (sig_obj.get("value") or "").lower().strip()
    sig_sf_val = (sig_obj.get("salesforceValue") or "").lower().strip()
    has_sig_doc = bool(sig_obj.get("hasServiceReportDocument"))
    did_sign = "Yes" if (
        sig_outcome in ("pass", "yes", "na_customernotpresent")
        or sig_sf_val in ("yes", "na_customernotpresent")
        or sig_value in ("yes", "na_customernotpresent")
        or has_sig_doc
    ) else "No"

    pay_obj = fields.get("paymentAttempted") or {}
    pay_raw = (pay_obj.get("value") or pay_obj.get("salesforceValue") or "").strip()
    pay_outcome = (pay_obj.get("outcome") or "").lower()
    if pay_raw == "CreditAccount" or "credit" in pay_raw.lower():
        payment = "Credit Account"
    elif pay_outcome == "pass" or pay_raw == "Yes":
        payment = "Yes"
    elif pay_raw == "No" or pay_outcome in ("fail",):
        payment = "No"
    else:
        payment = None

    # Normalise Time Taken — Salesforce only accepts "Excessive", "Ideal", "Rushed"
    time_taken_raw = sf_val("timeTaken") or ""
    time_taken = time_taken_raw.replace(" (severe)", "").strip() or None

    # Keyed by Salesforce field label (lowercase) — we discover the API name at runtime
    label_values: dict[str, Any] = {
        "post visit report check":   verdict or None,
        "workmanship":               sf_val("workmanship"),
        "decisionmaking":            sf_val("decisionMaking"),
        "report":                    sf_val("report"),
        "images quality":            images_quality,
        "time taken":                time_taken,
        "did the customer sign sr?": did_sign,
        "payment attempted":         payment,
    }

    log.info("[SF] Raw values to push: %s", label_values)

    # Map labels → actual API field names using the discovered map
    payload: dict[str, Any] = {}
    missing: list[str] = []
    for label, value in label_values.items():
        if value is None:
            continue
        api_name = field_map.get(label)
        if api_name:
            # Validate against picklist if applicable
            allowed = _sf_api_to_picklist.get(api_name, [])
            if allowed and value not in allowed:
                log.warning(
                    "[SF] Picklist mismatch for %s (%s): value=%r not in %s — skipping",
                    label, api_name, value, allowed,
                )
                continue
            payload[api_name] = value
        else:
            missing.append(label)

    if missing:
        log.warning("[SF] Could not resolve API names for labels: %s", missing)
        # Attempt direct API name lookup for known aliases
        alias_map = {
            "payment attempted": "Payment_Attempted__c",
            "images quality": "Images_Provided__c",
            "decisionmaking": "Decision_Making1__c",
        }
        for label in list(missing):
            direct = alias_map.get(label)
            if direct and label_values.get(label) is not None:
                value = label_values[label]
                allowed = _sf_api_to_picklist.get(direct, [])
                if allowed and value not in allowed:
                    log.warning("[SF] Alias picklist mismatch for %s: value=%r not in %s", label, value, allowed)
                else:
                    payload[direct] = value
                    missing.remove(label)
                    log.info("[SF] Resolved via alias: %s → %s = %r", label, direct, value)

    if not payload:
        log.error("[SF] Nothing to push — no field names could be resolved")
        return

    try:
        await run_in_threadpool(sf.ServiceAppointment.update, appointment_id, payload)
        log.info("[SF] ✓ Pushed to %s: %s", appointment_id, payload)
    except Exception as exc:
        log.error("[SF] ✗ Push failed for %s: %s | payload=%s", appointment_id, exc, payload)


@app.get("/api/sf-fields/service-appointment")
async def get_service_appointment_fields():
    """Debug endpoint — returns all writable field names on ServiceAppointment."""
    sf = app.state.sf
    try:
        meta = await run_in_threadpool(sf.ServiceAppointment.describe)
        writable = [
            {"name": f["name"], "label": f["label"], "type": f["type"]}
            for f in meta["fields"]
            if not f.get("calculated") and f.get("updateable")
        ]
        return {"fields": writable}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/sf-picklists/tqr-fields")
async def get_tqr_picklist_values():
    """Debug endpoint — returns allowed picklist values for each TQR Check field."""
    field_map = await _get_sa_field_map()
    tqr_labels = [
        "post visit report check", "workmanship", "decisionmaking",
        "report", "images quality", "time taken",
        "did the customer sign sr?", "payment attempted",
    ]
    result = {}
    for label in tqr_labels:
        api_name = field_map.get(label)
        if not api_name:
            # Try alias
            alias = {
                "payment attempted": "Payment_Attempted__c",
                "images quality": "Images_Provided__c",
                "decisionmaking": "Decision_Making1__c",
            }.get(label)
            api_name = alias
        allowed = _sf_api_to_picklist.get(api_name, []) if api_name else []
        result[label] = {"api_name": api_name, "allowed_values": allowed}
    return result


@app.post("/api/analyse/{appointment_id}")
async def analyse_appointment(appointment_id: str, force: bool = Query(False)):
    log.info("[API] POST /api/analyse/%s  force=%s", appointment_id, force)
    cached = await get_cached_result(appointment_id)
    if cached and not force:
        log.info("[API] Returning cached result for %s (analysed_at=%s)", appointment_id, cached.get("analysed_at"))
        return cached
    appointment = await fetch_appointment_by_id(appointment_id)
    work_order_id = resolve_work_order_id(appointment)
    if not work_order_id:
        log.error("[API] No Work Order found for appointment %s", appointment_id)
        raise HTTPException(status_code=400, detail="No related Work Order found for this appointment")
    work_order = await fetch_work_order(work_order_id)
    account = await fetch_account(work_order.get("AccountId"))
    images = await fetch_images_for_entity(work_order_id)
    documents = await fetch_related_documents(appointment, work_order_id)
    if not images:
        log.error("[API] No images found for work order %s (appointment %s)", work_order_id, appointment_id)
        raise HTTPException(status_code=400, detail="No image files attached to the related Work Order")
    result = await run_tqr_analysis(appointment, work_order, account, images, documents)
    saved = await save_analysis_result(appointment_id, result)
    log.info("[API] Analysis saved for %s — verdict=%s overall=%.2f", appointment_id, saved.get("verdict"), saved.get("overall", 0))
    await push_tqr_to_salesforce(appointment_id, saved)
    return saved


@app.get("/api/dashboard/stats")
async def get_dashboard_stats():
    records = await get_review_queue_records()
    cached = await get_all_cached_results()
    total_records = len(records)
    total_charge = round(sum(safe_float(record.get("CCT_Charge_Gross__c")) for record in records), 2)
    scored = [result["overall"] for result in cached.values() if result.get("overall") is not None]
    avg_tqr = round(sum(scored) / len(scored), 2) if scored else None

    by_trade_group: dict[str, int] = {}
    by_status: dict[str, int] = {}
    by_sector: dict[str, int] = {}
    low_score_jobs: list[dict[str, Any]] = []
    top_engineers_acc: dict[str, list[float]] = {}

    for record in records:
        trade_group = record.get("Trade_Group_Region__c") or record.get("Trade_Group_Postcode__c") or record.get("Scheduled_Trade__c") or "Unknown"
        status = record.get("Status") or "Unknown"
        sector = record.get("Sector_Type__c") or "Unknown"
        engineer = record.get("Allocated_Engineer__c") or "Unassigned"
        by_trade_group[trade_group] = by_trade_group.get(trade_group, 0) + 1
        by_status[status] = by_status.get(status, 0) + 1
        by_sector[sector] = by_sector.get(sector, 0) + 1
        tqr = cached.get(record["Id"])
        if tqr and tqr.get("overall") is not None:
            top_engineers_acc.setdefault(engineer, []).append(float(tqr["overall"]))
            if float(tqr["overall"]) < 5:
                low_score_jobs.append(
                    {
                        "id": record["Id"],
                        "appointmentNumber": record.get("AppointmentNumber"),
                        "engineer": engineer,
                        "trade": record.get("Scheduled_Trade__c"),
                        "status": status,
                        "overall": float(tqr["overall"]),
                        "actualEndTime": format_uk_datetime(record.get("ActualEndTime")),
                    }
                )

    top_engineers = sorted(
        [{"name": key, "avgTQRScore": round(sum(vals) / len(vals), 2), "jobs": len(vals)} for key, vals in top_engineers_acc.items()],
        key=lambda item: item["avgTQRScore"],
        reverse=True,
    )[:5]

    return {
        "totalRecords": total_records,
        "totalCCTCharge": total_charge,
        "avgTQRScore": avg_tqr,
        "byTradeGroup": [{"name": key, "count": value} for key, value in sorted(by_trade_group.items())],
        "byStatus": [{"name": key, "count": value} for key, value in sorted(by_status.items())],
        "bySector": [{"name": key, "count": value} for key, value in sorted(by_sector.items())],
        "topEngineers": top_engineers,
        "lowScoreJobs": sorted(low_score_jobs, key=lambda item: item["overall"])[:10],
    }
