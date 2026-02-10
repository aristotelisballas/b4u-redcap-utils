import os
import re
from redcap import Project

from datetime import datetime, timezone
from typing import Optional, Tuple


BASE_URL = os.getenv("BASE_URL")
API_TOKEN = os.getenv("API_TOKEN")
HEALTH_FIELD = os.getenv("HEALTH_FIELD")
ALLOC_FIELD = os.getenv("ALLOC_FIELD")


def api_url(base):
    base = base.rstrip("/") + "/"
    return base if base.endswith("api/") else base + "api/"


def _resolve_dag_unique(proj, country_code: str) -> str:
    # import re
    # m = re.match(r"^[A-Za-z]+", user_id)
    # if not m:
        # raise ValueError("user_id must start with a country prefix like EL, LT, ES, SE")
    # prefix = m.group(0).upper()

    # prefix_to_site = {"EL": "greece", "LT": "lithuania", "ES": "spain", "SE": "sweden"}
    prefix_to_site = {"EL": "greece", "LT": "lithuania", "ES": "spain", "SE": "sweden", "TEST": "greece"}
    # ES, EL, LT, SW
    site = prefix_to_site.get(country_code)

    if not site:
        raise ValueError(f"Unknown prefix '{country_code}'")

    dags = proj.export_dags()  # [{'data_access_group_name': 'Greece', 'unique_group_name': 'greece'}, ...]

    for dag in dags:
        if dag.get("unique_group_name", "").lower() == site:
            return dag["unique_group_name"]

    for dag in dags:
        if dag.get("data_access_group_name", "").lower() == site:
            return dag["unique_group_name"]

    norm = lambda s: s.lower().replace(" ", "").replace("_", "").replace("-", "")
    for dag in dags:
        if norm(dag.get("data_access_group_name", "")) == norm(site):
            return dag["unique_group_name"]

    raise RuntimeError(f"No matching DAG for site '{site}'. Exported DAGs: {dags}")


def _health_code_from_metadata(proj: Project, value: str) -> str:
    meta = [m for m in proj.export_metadata(fields=[HEALTH_FIELD]) if m.get("field_name") == HEALTH_FIELD]
    if not meta:
        raise RuntimeError(
            f"Field '{HEALTH_FIELD}' not found. Make sure the Variable Name is exactly '{HEALTH_FIELD}'.")
    choices = meta[0].get("select_choices_or_calculations", "")
    pairs = [p.strip() for p in choices.split("|") if p.strip()]
    code_by_label = {}
    code_by_code = {}
    for p in pairs:
        if "," not in p:
            continue
        code, label = [x.strip() for x in p.split(",", 1)]
        code_by_code[code.lower()] = code
        code_by_label[label.lower()] = code

    v = value.strip().lower()
    if v in code_by_code:
        return code_by_code[v]
    if v in code_by_label:
        return code_by_label[v]
    synonyms = {"healthy": "healthy", "patient": "patient", "survivor": "survivor"}
    if v in synonyms and synonyms[v] in code_by_label:
        return code_by_label[synonyms[v]]
    raise RuntimeError(
        f"Value '{value}' does not match any choice for '{HEALTH_FIELD}'. "
        f"Choices are: {choices}"
    )


def _date_only_date(ts: str) -> datetime:
    """
    Parse ISO8601 timestamp into a datetime.date, then return
    a datetime object at midnight UTC (stored as BSON Date in Mongo).
    """
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        # fallback: handle naive string
        dt = datetime.fromisoformat(ts.split("+")[0])
    # keep only date part, normalize to midnight UTC
    return datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc)


def create_record(user_id: str, health_value: str, country_code: str):
    proj = Project(api_url(BASE_URL), API_TOKEN)
    record_id_field = proj.def_field
    if country_code == "TEST":
        _country_code = "EL"
    else:
        _country_code = country_code

    dag_unique = _resolve_dag_unique(proj, _country_code)
    health_code = _health_code_from_metadata(proj, health_value)

    rec = {
        record_id_field: user_id,
        "redcap_data_access_group": dag_unique,
        HEALTH_FIELD: health_code,
        "registration_complete": 2
    }
    if proj.is_longitudinal:
        rec["redcap_event_name"] = proj.export_events()[0]["unique_event_name"]

    return proj.import_records(
        [rec],
        overwrite="overwrite",
        return_content="ids",
        date_format="YMD",
    )


def choice_map(proj: Project, field: str) -> dict:
    md = [m for m in proj.export_metadata(fields=[field]) if m.get("field_name") == field]
    if not md:
        return {}
    choices = md[0].get("select_choices_or_calculations", "") or ""
    out = {}
    for part in [p.strip() for p in choices.split("|") if p.strip()]:
        if "," in part:
            code, label = [x.strip() for x in part.split(",", 1)]
            out[code] = label
    return out


def get_randomization_group(record_id: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Returns (raw_value, label, event_name) for ALLOC_FIELD.
    If not set yet, returns (None, None, None).
    """
    proj = Project(api_url(BASE_URL), API_TOKEN)
    record_id_field = proj.def_field

    rows = proj.export_records(
        records=[record_id],
        fields=[record_id_field, ALLOC_FIELD],
        raw_or_label="raw",
    )
    if not rows:
        return (None, None, None)

    found_raw: Optional[str] = None
    found_event: Optional[str] = None

    for row in rows:
        v = row.get(ALLOC_FIELD)
        if v not in (None, "", [], {}):
            found_raw = str(v)
            found_event = row.get("redcap_event_name")
            break

    if not found_raw:
        return (None, None, None)

    cmap = choice_map(proj, ALLOC_FIELD)
    label = cmap.get(found_raw)
    return (found_raw, label, found_event)


def _parse_iso_datetime(ts: str) -> datetime:
    """
    Parse ISO 8601 timestamps like '2025-10-13T09:58:04+00:00' or ending with 'Z'
    into timezone-aware datetimes.
    """
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception as e:
        raise ValueError(f"Invalid ISO8601 timestamp: {ts}") from e


def _serialize_response_doc(doc: dict) -> dict:
    # Convert Mongo types to JSON-friendly values
    d = dict(doc)
    if "_id" in d:
        d["_id"] = str(d["_id"])
    for k in ("timestamp", "createdAt", "updatedAt"):
        if k in d and d[k] is not None:
            try:
                d[k] = d[k].isoformat()
            except Exception:
                pass
    return d
