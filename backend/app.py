import asyncio
import base64
import io
import json
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
from groq import Groq
from simple_salesforce import Salesforce


BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")
DB_PATH = BASE_DIR / "tqr_results.db"
IMAGE_FILE_TYPES = {"jpg", "jpeg", "png", "heic", "webp"}

APPOINTMENT_FIELDS = [
    "Id",
    "Trade_Group_Postcode__c",
    "Allocated_Engineer__c",
    "Allocated_Engineer__r.Name",
    "Feedback_Notes__c",
    "AppointmentNumber",
    "Status",
    "Scheduled_Trade__c",
    "Description",
    "ActualEndTime",
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
        for col, col_type in [("tqr_fields", "TEXT"), ("image_descriptions", "TEXT"), ("hard_fail", "INTEGER"), ("verdict", "TEXT")]:
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
                   overall, summary, flags, recommendation, tqr_fields, image_descriptions, hard_fail, verdict, analysed_at
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
                   overall, summary, flags, recommendation, tqr_fields, image_descriptions, hard_fail, verdict, analysed_at
            FROM tqr_results
            """
        )
        rows = await cursor.fetchall()
    output: dict[str, dict[str, Any]] = {}
    for row in rows:
        item = _deserialise_cached_row(dict(row))
        output[item["appointment_id"]] = item
    return output


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
    item["ActualEndTimeFormatted"] = format_uk_datetime(item.get("ActualEndTime"))
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
        if engineer and (record.get("Allocated_Engineer__c") or "") != engineer:
            continue
        if status and (record.get("Status") or "") != status:
            continue
        if trade and (record.get("Scheduled_Trade__c") or "") != trade and (record.get("Trade_Group_Postcode__c") or "") != trade:
            continue
        if sector and (record.get("Sector_Type__c") or "") != sector:
            continue
        if search:
            haystack = " ".join(
                str(record.get(field) or "")
                for field in (
                    "Trade_Group_Postcode__c",
                    "Allocated_Engineer__c",
                    "Feedback_Notes__c",
                    "AppointmentNumber",
                    "Status",
                    "Scheduled_Trade__c",
                    "Description",
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
    return [
        version
        for version in versions
        if str(version.get("FileType", "")).lower() in IMAGE_FILE_TYPES
    ]


async def fetch_images_for_entity(entity_id: str) -> list[dict[str, Any]]:
    sf = app.state.sf
    versions = await fetch_content_versions_for_entity(entity_id)
    images: list[dict[str, Any]] = []
    for version in versions:
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
      "salesforceValue": "Good"|"Poor",
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

FIELD customerSignature: Check signature.present. If true and clear = Yes/Pass. If false, scan notes for
"customer not present", "no-one at property", "access via key" etc = NA_CustomerNotPresent/Pass.
If false and no explanation = No/Fail (hard fail trigger).

FIELD imagesQuality: Assess all photos. Mandatory: location shot, work before, work after, job completion.
Score 9-10 all mandatory+situational present with quality. 7-8 all mandatory present good quality.
5-6 all mandatory present but quality issues. 3-4 one mandatory missing. 0-2 multiple mandatory missing or zero photos.
salesforceValue = "Good" if score 7-10, "Poor" if score 0-6.

FIELD paymentAttempted: If Credit Account = CreditAccount/Pass. If payment collected = Yes/Pass.
If charge > 0 and not collected and no notes explanation = No/Fail.

FIELD report: Score how well notes answer: (1) what was the problem, (2) what did engineer find,
(3) what did they do, (4) what happens next. 9-10 all four clear. 7-8 all four present minor gaps.
5-6 three answered. 3-4 two or fewer. 0-2 absent or incomprehensible.
salesforceValue: 9-10=Perfect, 7-8=Good, 5-6=Acceptable, 0-4=Non Acceptable.

FIELD timeTaken: Compute actual minutes from actualStart to actualEnd. Compare against any benchmark.
If no benchmark, outcome=Review. Score by variance: <=10%=9-10, <=25%=7-8, <=50%=5-6, >50%=3-4, >100%=0-2.
salesforceValue: Ideal if 9-10, Excessive if actual>expected, Rushed if actual<expected.

FIELD workmanship: Assess physical quality from photos. Default Good (7-8) for competent work.
Reserve Perfect (9-10) for genuinely outstanding evidence. Set urgentIssueDetected=true for safety concerns.
salesforceValue: 9-10=Perfect, 7-8=Good, 5-6=Acceptable, 3-4=Non Acceptable, 0-2=Urgent Issue.

FIELD decisionMaking: Assess diagnosis, approach, escalation, further works raised, commercial opportunities.
Check for missed opportunities and overcommitments to customer. Set urgentIssueDetected=true if safety not escalated.
salesforceValue: 9-10=Perfect, 7-8=Good, 5-6=Acceptable, 3-4=Non Acceptable, 0-2=Urgent Issue.
"""


def _compute_weighted_verdict(fields: dict[str, Any]) -> tuple[float, str, bool, list[str]]:
    hard_fail_reasons: list[str] = []
    img = fields.get("imagesQuality") or {}
    mandatory = img.get("mandatoryPhotosPresent") or {}
    if not any(mandatory.values()):
        hard_fail_reasons.append("Zero photos attached")
    if (fields.get("workmanship") or {}).get("urgentIssueDetected"):
        hard_fail_reasons.append("Urgent workmanship issue detected")
    if (fields.get("decisionMaking") or {}).get("urgentIssueDetected"):
        hard_fail_reasons.append("Urgent decision-making issue detected")
    if (fields.get("customerSignature") or {}).get("value") == "No":
        hard_fail_reasons.append("Customer signature missing with no valid reason")
    if (fields.get("report") or {}).get("flaggedIssues"):
        hard_fail_reasons.append("Unresolved customer complaint in report")

    hard_fail = bool(hard_fail_reasons)

    sig_score = {"Yes": 10, "NA_CustomerNotPresent": 10, "No": 0}.get(
        (fields.get("customerSignature") or {}).get("value", "No"), 5
    )
    pay_score = {"Yes": 10, "CreditAccount": 10, "No": 0}.get(
        (fields.get("paymentAttempted") or {}).get("value", "No"), 5
    )
    weights = {"workmanship": 0.20, "decisionMaking": 0.20, "imagesQuality": 0.20,
               "report": 0.15, "timeTaken": 0.10}
    weighted = sum(
        safe_float((fields.get(f) or {}).get("score", 0)) * w for f, w in weights.items()
    ) + pay_score * 0.10 + sig_score * 0.05
    percentage = weighted * 10

    if hard_fail:
        verdict = "Unacceptable"
    elif percentage >= 80:
        verdict = "TQR"
    elif percentage >= 50:
        verdict = "Sub standard"
    else:
        verdict = "Unacceptable"

    return round(weighted, 2), verdict, hard_fail, hard_fail_reasons


def _compress_image_for_ai(img: dict[str, Any], max_px: int = 512, quality: int = 40) -> str:
    """Resize and re-compress an image to reduce base64 payload size for Groq."""
    raw = base64.b64decode(img["base64"])
    with Image.open(io.BytesIO(raw)) as pil:
        pil = pil.convert("RGB")
        pil.thumbnail((max_px, max_px), Image.LANCZOS)
        buf = io.BytesIO()
        pil.save(buf, format="JPEG", quality=quality, optimize=True)
    return base64.b64encode(buf.getvalue()).decode("utf-8")


IMAGE_DESCRIPTION_PROMPT = """
You are inspecting a single field service job photo.
Describe exactly what is visible in this image in clear practical detail.
Focus on visible equipment, fittings, surfaces, tools, measurements, job progress, cleanliness, workmanship, and any obvious risks.
Do not guess hidden context or invent anything that is not visible.
Return ONLY valid JSON:
{
  "description": "Detailed visual description"
}
"""


async def describe_single_image(image: dict[str, Any]) -> dict[str, Any]:
    client = app.state.groq
    try:
        compressed = _compress_image_for_ai(image)
    except Exception:
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
    try:
        response = await run_in_threadpool(
            client.chat.completions.create,
            model="meta-llama/llama-4-scout-17b-16e-instruct",
            temperature=0.1,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": IMAGE_DESCRIPTION_PROMPT},
                {"role": "user", "content": content},
            ],
            max_tokens=500,
        )
        raw = response.choices[0].message.content or ""
        payload = json_extract(raw)
        description = str(payload.get("description") or "").strip() or "No description returned."
    except Exception:
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
) -> dict[str, Any]:
    client = app.state.groq

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

    evidence = {
        "job": {
            "appointmentNumber": appointment.get("AppointmentNumber"),
            "trade": appointment.get("Scheduled_Trade__c") or appointment.get("Trade_Group_Postcode__c"),
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
        },
        "signature": {
            "present": bool(appointment.get("Customer_Signature__c")),
        },
        "workOrder": {
            "workOrderNumber": (work_order or {}).get("WorkOrderNumber"),
            "description": (work_order or {}).get("Description"),
            "workType": ((work_order or {}).get("WorkType") or {}).get("Name"),
        },
        "photos": [{"filename": img["title"], "id": img["id"]} for img in images],
    }

    sample_images = images[:5]
    compressed: list[tuple[dict[str, Any], str]] = []
    for img in sample_images:
        try:
            b64 = _compress_image_for_ai(img)
            compressed.append((img, b64))
        except Exception:
            pass

    content: list[dict[str, Any]] = [
        {
            "type": "text",
            "text": (
                f"Job Evidence (JSON):\n{json.dumps(evidence, indent=2)}\n\n"
                f"Total photos on job: {len(images)}. Showing {len(compressed)} sample photo(s) below.\n\n"
                "Assess this job against all 7 TQR fields. Return ONLY the JSON object matching the output schema."
            ),
        }
    ]
    for img, b64 in compressed:
        content.append({
            "type": "image_url",
            "image_url": {"url": f"data:image/jpeg;base64,{b64}"},
        })

    response = await run_in_threadpool(
        client.chat.completions.create,
        model="meta-llama/llama-4-scout-17b-16e-instruct",
        temperature=0.1,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": TQR_SYSTEM_PROMPT},
            {"role": "user", "content": content},
        ],
        max_tokens=4000,
    )

    raw = response.choices[0].message.content or ""
    payload = json_extract(raw)
    fields = payload.get("fields") or {}
    ai_summary = (payload.get("summary") or {}).get("overallObservations", "AI analysis completed.")
    image_descriptions = await asyncio.gather(*(describe_single_image(img) for img in images))

    overall_score, verdict, hard_fail, hard_fail_reasons = _compute_weighted_verdict(fields)

    flags: list[str] = []
    for fn in ("workmanship", "decisionMaking"):
        f = fields.get(fn) or {}
        if f.get("urgentIssueDescription"):
            flags.append(f.get("urgentIssueDescription", ""))
    for issue in (fields.get("report") or {}).get("flaggedIssues", []):
        flags.append(str(issue))
    flags = [f for f in flags if f]

    workmanship_score = safe_float((fields.get("workmanship") or {}).get("score", 0))

    return {
        "workmanship": workmanship_score,
        "cleanliness": workmanship_score,
        "safety": safe_float((fields.get("workmanship") or {}).get("urgentIssueDetected", False)) * 10,
        "completion": safe_float((fields.get("imagesQuality") or {}).get("score", 0)),
        "overall": overall_score,
        "summary": ai_summary,
        "flags": flags,
        "recommendation": verdict,
        "tqr_fields": fields,
        "image_descriptions": image_descriptions,
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
    detail = enrich_appointment(appointment, cached)
    detail["workOrderId"] = work_order_id
    detail["workOrder"] = work_order
    detail["account"] = account
    detail["site"] = territory.get("Name") if territory else None
    detail["accountManager"] = ((account or {}).get("Owner") or {}).get("Name")
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


@app.post("/api/analyse/{appointment_id}")
async def analyse_appointment(appointment_id: str, force: bool = Query(False)):
    cached = await get_cached_result(appointment_id)
    if cached and not force:
        return cached
    appointment = await fetch_appointment_by_id(appointment_id)
    work_order_id = resolve_work_order_id(appointment)
    if not work_order_id:
        raise HTTPException(status_code=400, detail="No related Work Order found for this appointment")
    work_order = await fetch_work_order(work_order_id)
    account = await fetch_account(work_order.get("AccountId"))
    images = await fetch_images_for_entity(work_order_id)
    if not images:
        raise HTTPException(status_code=400, detail="No image files attached to the related Work Order")
    result = await run_tqr_analysis(appointment, work_order, account, images)
    return await save_analysis_result(appointment_id, result)


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
        # ✅ FIXED: prioritise Scheduled_Trade__c so all trade types appear on the chart
        trade_group = record.get("Scheduled_Trade__c") or record.get("Trade_Group_Postcode__c") or "Unknown"
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
