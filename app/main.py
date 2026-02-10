import os
import json
import time
from uuid import uuid4
from datetime import datetime
from pathlib import Path
import httpx
import logging
from typing import Optional, Literal, Any, List, Dict

from b4u_utils import api_url, export_record_with_labels, connect_to_project

from utils import *
from utils import _date_only_date, _parse_iso_datetime, _serialize_response_doc

from fastapi import FastAPI, Query, Body
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import secrets

from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator
from pymongo import MongoClient
from pymongo.errors import PyMongoError


# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("redcap-utils")

# --- Security setup ---
security = HTTPBasic()

# Credentials from environment variables
BASIC_AUTH_USER = os.getenv("API_USER", "admin")
BASIC_AUTH_PASS = os.getenv("API_PASS", "changeme")

# --- CONFIGURATION ---
REDCAP_API_URL = os.getenv("BASE_URL")
REDCAP_API_TOKEN = os.getenv("API_TOKEN")

# ---- Config for forwarding (set in env when deployed) ----
FORWARD_URL = os.getenv("FORWARD_URL")  # e.g. "https://other-service/api/ingest"
# FORWARD_TIMEOUT_S = float(os.getenv("FORWARD_TIMEOUT_S", "10"))
FORWARD_ENABLED = os.getenv("FORWARD_ENABLED", "0") == "1"


def get_current_username(credentials: HTTPBasicCredentials = Depends(security)):
    correct_username = secrets.compare_digest(credentials.username, BASIC_AUTH_USER)
    correct_password = secrets.compare_digest(credentials.password, BASIC_AUTH_PASS)
    if not (correct_username and correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username


# Initialize FastAPI app
app = FastAPI(
    title="B4U REDCap Utilities API",
    description="""
    API for communication between REDCap and the MELIORA servers.
    It provides endpoints for:
    - retrieving responses from REDCap API
    - storing REDCap responses to DB --> /store-redcap-responses TBD
    - retrieving REDCap responses from DB --> /get-redcap-responses TBD
    
    You can explore the available endpoints below.
    """,
    version="1.0.0",
    contact={
        "name": "Aristotelis Ballas (HUA)",
        # "url": "http://example.com/contact/",
        "email": "aballas@hua.gr",
    },
    servers=[
        {
            "url": "http://195.251.31.231:9994/",
            "description": "Actual VM port"
        },
        {
            "url": "http://localhost:8001/",
            "description": "Localhost testing"
        },
    ],
    dependencies=[Depends(get_current_username)]
)

origins = [
    "http://localhost:8001",
    "http://195.251.31.231:9994/",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# MongoDB setup
# client = MongoClient(os.getenv("MONGODB_URI", "mongodb://localhost:27017"))
# client = MongoClient(os.getenv("MONGODB_URI"))
# db = client["meliora_dev_rct"]
# collection = db["UserProfile"]
# responses_col = db["RedcapResponses"]


# ==== Models ====
# class UpdateUserProfile(BaseModel):
#     userId: str
#     isControl: bool
#
#
# class CreateRecordRequest(BaseModel):
#     userId: str
#     health_status: str
#     country_code: str
#
#
# class CreateRecordResponse(BaseModel):
#     userId: str
#     status: Literal["success", "error"]
#     data: Optional[dict]
#
#
# class RandomizationPayload(BaseModel):
#     project_id: int
#     project_title: str
#     record_id: str
#     event_id: int
#     event_name: str
#     instrument: str
#     dag: Optional[str] = None
#     allocation_field: str
#     allocation: str
#     timestamp: str
#
# class ForwardUserResponse(BaseModel):
#     status: Literal["received", "forwarded", "error"]
#     userId: str
#     category: Optional[str] = None
#     upstream_status_code: Optional[int] = None
#     upstream_body: Optional[Any] = None
#
# class AnswerItem(BaseModel):
#     field_name: str
#     field_label: Optional[str] = None
#     value_raw: Optional[str] = None
#     value_label: Optional[str] = None
#     field_type: Optional[str] = None
#
#     @field_validator("value_raw", "value_label", mode="before")
#     @classmethod
#     def _coerce_to_str_or_none(cls, v: Any) -> Optional[str]:
#         # Treat empty-like values as None
#         if v in (None, "", [], {}):
#             return None
#         # If it's a list (e.g., checkboxes), join as comma-separated
#         if isinstance(v, list):
#             return ",".join(str(x) for x in v) if v else None
#         # If it's a dict, store JSON
#         if isinstance(v, dict):
#             return json.dumps(v, ensure_ascii=False)
#         # Everything else → string
#         return str(v)
#
#
# class Snapshot(BaseModel):
#     instrument: str
#     answers: List[AnswerItem] = Field(default_factory=list)
#
#
# class RedcapResponsePayload(BaseModel):
#     project_id: int
#     project_title: str
#     record_id: str
#     event_id: int
#     event_unique: str
#     event_label: Optional[str] = None
#     dag: Optional[str] = None
#     instrument_language: str
#     timestamp: str
#     snapshot: Snapshot

# ==== Endpoints ====
# @app.get("/get-user-action-plans", summary="List all action plans for a user")
# async def get_user_action_plans(userId: str):
#     try:
#         plans = collection.find({"userId": userId}).sort("createdAt", -1)
#         result = []
#         for plan in plans:
#             plan["_id"] = str(plan["_id"])
#             result.append(plan)
#
#         if not result:
#             return JSONResponse(status_code=404, content={"message": "No action plans found"})
#         return result
#     except Exception as e:
#         return JSONResponse(status_code=500, content={"message": str(e)})


# @app.post(
#     "/create-record",
#     response_model=CreateRecordResponse,
#     summary="Create a single REDCap record for a user"
# )
# async def create_single_record(payload: CreateRecordRequest, username: str = Depends(get_current_username)):
#     try:
#         result = create_record(payload.userId, payload.health_status, payload.country_code)
#         print(result)
#         return CreateRecordResponse(
#             userId=payload.userId,
#             status="success",
#             data={"imported_ids": result}
#         )
#     except Exception as e:
#         # Keep message concise but useful
#         return JSONResponse(
#             status_code=400,
#             content={
#                 "userId": payload.userId,
#                 "status": "error",
#                 "message": str(e),
#             },
#         )


@app.get("/hello", summary="Hello World")
async def hello_world():
    return {"message": "Hello, world!"}





# @app.post("/store-redcap-responses", summary="Upsert a REDCap instrument response snapshot")
# async def upsert_redcap_response(
#     payload: RedcapResponsePayload,
#     # username: str = Depends(get_current_username)  # uncomment if you secured routes
# ):
#     try:
#         logger.info(f"Received /redcap-responses payload for record_id={payload.record_id}, "
#                     f"event={payload.event_unique}, instrument={payload.snapshot.instrument}")
#
#         ts_dt = _parse_iso_datetime(payload.timestamp)  # BSON Date
#         now_dt = datetime.now(timezone.utc)
#
#         # Composite uniqueness key (prevents duplicates for the same user/event/instrument)
#         filter_q = {
#             "record_id": payload.record_id,
#             "event_unique": payload.event_unique,
#             "snapshot.instrument": payload.snapshot.instrument,
#         }
#
#         # Document fields to store/update
#         set_doc = {
#             "project_id": payload.project_id,
#             "project_title": payload.project_title,
#             "record_id": payload.record_id,
#             "userId": payload.record_id,  # convenience alias (optional)
#             "event_id": payload.event_id,
#             "event_unique": payload.event_unique,
#             "event_label": payload.event_label,
#             "dag": payload.dag,
#             "timestamp": ts_dt,                  # BSON Date
#             "snapshot": payload.snapshot.dict(), # instrument + answers[]
#             "updatedAt": now_dt,
#         }
#
#         # Only on first insert
#         set_on_insert = {
#             "createdAt": now_dt,
#         }
#
#         res = responses_col.update_one(
#             filter_q,
#             {"$set": set_doc, "$setOnInsert": set_on_insert},
#             upsert=True
#         )
#
#         status_str = "inserted" if res.upserted_id is not None else ("updated" if res.modified_count else "no_change")
#
#         return {
#             "status": status_str,
#             "key": {
#                 "record_id": payload.record_id,
#                 "event_unique": payload.event_unique,
#                 "instrument": payload.snapshot.instrument
#             }
#         }
#
#     except ValueError as ve:
#         logger.error(f"/redcap-responses validation error: {ve}")
#         return HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve))
#     except Exception as e:
#         logger.exception("/redcap-responses unexpected error")
#         return HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@app.get("/get-redcap-responses", summary="List RedcapResponses for a record_id")
async def list_redcap_responses(
    record_id: str = Query(..., description="REDCap record_id (e.g., 304 for example results)"),
):
    try:
        project = connect_to_project(api_url(REDCAP_API_URL), REDCAP_API_TOKEN)

        record_data = export_record_with_labels(project, record_id)

        return record_data

    except Exception as e:
        return HTTPException(status_code=500, detail=str(e))


# @app.get(
#     "/list-user-ids",
#     summary="Return all userIds in the UserProfile collection"
# )
# async def list_user_ids():
#     try:
#         cursor = collection.find({}, {"_id": 0, "userId": 1})
#         user_ids = [doc["userId"] for doc in cursor if doc.get("userId")]
#         return user_ids
#
#     except Exception as e:
#         logger.exception("Error in /list-user-ids")
#         return HTTPException(status_code=500, detail=str(e))

#
# @app.get("/mongo-health", summary="Check MongoDB connectivity (ping + optional deep check)")
# async def mongo_health(deep: bool = Query(True, description="Include DB/collections info")):
#     t0 = time.monotonic()
#     try:
#         # Quick connectivity check (works with auth)
#         client.admin.command("ping")
#         latency_ms = int((time.monotonic() - t0) * 1000)
#
#         payload = {
#             "status": "ok",
#             "latency_ms": latency_ms,
#             "host": str(client.HOST) if hasattr(client, "HOST") else None,
#             "port": int(client.PORT) if hasattr(client, "PORT") else None,
#             "db": db.name,
#         }
#
#         if deep:
#             # “Deep” but still light: list collections in the target DB
#             payload["collections"] = sorted(db.list_collection_names())
#             # Optionally add quick counts (fast, approximate):
#             # try:
#             #     payload["estimated_counts"] = {
#             #         "UserProfile": db["UserProfile"].estimated_document_count(),
#             #         "RedcapResponses": db["RedcapResponses"].estimated_document_count(),
#             #     }
#             # except Exception:
#             #     # ignore if permissions don’t allow counts
#             #     pass
#
#         return JSONResponse(status_code=200, content=payload)
#
#     except PyMongoError as e:
#         raise HTTPException(
#             status_code=500,
#             detail={
#                 "status": "error",
#                 "message": "MongoDB ping failed",
#                 "error": str(e),
#             },
#         )
#     except Exception as e:
#         raise HTTPException(
#             status_code=500,
#             detail={
#                 "status": "error",
#                 "message": "Unexpected error during MongoDB health check",
#                 "error": str(e),
#             },
#         )
#
#
# @app.post(
#     "/redcap-completed-user",
#     response_model=ForwardUserResponse,
#     summary="Receive JSON, extract userId, forward only userId to external service"
# )
# async def forward_user_id(
#     payload: Dict[str, Any] = Body(...),
#     # payload: RedcapResponsePayload,
#     username: str = Depends(get_current_username)
# ):
#     try:
#         # ----------------------------
#         # 1. Extract userId
#         # ----------------------------
#         user_id = payload.get("record_id")
#         instrument_name = payload.get("instrument")
#
#         if not user_id:
#             logger.error("Missing userId in incoming JSON")
#             raise HTTPException(status_code=400, detail="Missing required field: userId")
#
#         logger.info(f"/redcap-complete-user received payload for userId={user_id}")
#         logger.debug(f"Full payload: {json.dumps(payload, ensure_ascii=False)}")
#
#         SEND_REQUEST = False
#
#         doc = collection.find_one(
#             {"userId": user_id},
#             {"_id": 0, "category": 1}
#         )
#
#         category = doc["category"]
#
#         logger.info(f"The group of user {user_id} is {category}.")
#
#         ###
#         if instrument_name == "functionality_appreciation_scale_fas" and category in ["healthy", "HEALTHY"]:
#             SEND_REQUEST = True
#             logger.info("Will send upstream request.")
#
#         if (instrument_name == "edmonton_symptom_assessment_system_revised_esasr" and
#                 category not in ["healthy", "HEALTHY"]):
#             SEND_REQUEST = True
#             logger.info("Will send upstream request.")
#
#         # ----------------------------
#         # 2. Build outbound payload
#         # ----------------------------
#         outbound = {"userId": user_id}
#
#         # ----------------------------
#         # 3. Forward if enabled
#         # ----------------------------
#
#         target_url = f"{FORWARD_URL}/users/{user_id}/status/today"
#
#         will_forward = SEND_REQUEST and FORWARD_ENABLED
#
#         logger.info(f"PLC/Stratification will be initiated: {will_forward}")
#
#         if will_forward:
#             if not FORWARD_URL:
#                 return HTTPException(status_code=500, detail="FORWARD_ENABLED=1 but FORWARD_URL is not set")
#
#             async with httpx.AsyncClient(timeout=15) as client:
#                 resp = await client.get(target_url)
#
#             try:
#                 upstream_body = resp.json()
#             except Exception:
#                 upstream_body = resp.text
#
#             logger.info(f"/redcap-complete-user forwarded userId={user_id} → {FORWARD_URL} ({resp.status_code})")
#
#             return ForwardUserResponse(
#                 status="forwarded",
#                 userId=user_id,
#                 category=category,
#                 upstream_status_code=resp.status_code,
#                 upstream_body=upstream_body,
#             )
#
#         # ----------------------------
#         # 4. Local testing mode
#         # ----------------------------
#         logger.info(f"/redcap-complete-user TEST MODE — retrieved for: {outbound}")
#
#         return ForwardUserResponse(
#             status="received",
#             userId=user_id,
#         )
#
#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.exception("Unexpected error in /redcap-completed-user")
#         raise HTTPException(status_code=500, detail=str(e))


# Run locally
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
