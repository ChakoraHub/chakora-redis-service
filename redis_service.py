"""
redis_service.py  ─  Shared Redis Cache Microservice
Port : 6380
Run  : uvicorn redis_service:app --host 0.0.0.0 --port 6380

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Redis DB Allocation  (shared across ALL microservices)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  DB 0  →  Session Cache          (student, employee, admin …)
  DB 1  →  User Profile Cache     (all services)
  DB 2  →  Auth / Roles Cache     (all services)
  DB 3  →  Frequently Used Data   (course videos, PPTs, resources, offers, festivals …)
  DB 4  →  Rate Limiting          (login, feedback, enquiry …)
  DB 5  →  API Response Cache     (dashboard, home-page sections, blogger …)
  DB 6  →  Chatbot Cache          (response cache · embedding cache · conversation memory)
  DB 7  →  Chatbot Knowledge Base (RAG vector chunks · KB index · KB metadata)
  DB 8  →  Meeting Service        (slot availability · slot locks · pending holds · user booking cache)
  DB 9  →  RAG Vector Store       (rag_service: dense embeddings · chunk metadata · doc index · etags)
  DB 10 →  RAG BM25 Index         (rag_service: inverted index · term scores · doc lengths)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Canonical key schema (student_service):
  session:{user_id}               DB 0   7 days
  user:{user_id}                  DB 1   30 min
  roles:{user_id}                 DB 2   1 hr
  student:dashboard:{user_id}     DB 5   5 min
  resources:{subject}:{lang}      DB 3   2 hr
  resources:{subject}:files       DB 3   2 hr
  offers:{date}                   DB 3   1 hr
  festival:{date}                 DB 3   24 hr
  feedbacks:latest                DB 3   10 min
  registration:{user_id}          DB 3   5 min

Home-page keys (home_service):
  home:batches   DB 5   10 min
  home:feedback  DB 5    5 min
  home:offers    DB 5   15 min
  home:about     DB 5   15 min

Blogger keys (blogger_service):
  blog:list                DB 5   20 min   post list base (likes merged at read-time)
  blog:{id}                DB 5   10 min   single post full response
  blog:stats               DB 5   30 min   aggregate stats
  blog:subscriber_count    DB 5   30 min   subscriber count
  blog:dashboard           DB 5   10 min   combined dashboard payload
  blog:likes:{id}          DB 5   no TTL   persistent like counter (INCR / SETNX)

Chatbot keys (chatbot_service):
  chat:response:{hash}          DB 6   30 min   cached LLM response (deduplicate identical queries)
  chat:history:{conv_id}        DB 6   24 hr    full message history JSON array
  chat:embeddings:{conv_id}     DB 6   24 hr    per-message embedding vectors
  chat:meta:{conv_id}           DB 6   24 hr    conversation metadata (user_id, timestamps, count)
  embedding:{doc_id}            DB 6   7 days   embedding vector for a KB chunk / RAG document
  chatbot:kb:index              DB 7   no TTL   SADD set of all chunk IDs
  chatbot:kb:metadata           DB 7   no TTL   JSON sync metadata (manifest hash, file count …)
  chatbot:kb:chunk:{id}         DB 7   no TTL   individual RAG chunk JSON (text + vector)
  chatbot:kb:source:{id}        DB 7   no TTL   SADD set of chunk IDs per source document
  chatbot:kb:source-meta:{id}   DB 7   no TTL   JSON source document metadata

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Canonical key schema (employee_service):
  session:{employee_id}                   DB 0   7 days   (via /session/set)
  user:{employee_id}                      DB 1   30 min   (via /profile/set)
  roles:{employee_id}                     DB 2   1 hr     (via /auth/set)
  employee:{employee_id}                  DB 5   15 min   (partial API cache — personal details)
  salary:{employee_id}                    DB 5   2 min    (short TTL — sensitive, changes often)
  leave:{employee_id}                     DB 5   5 min    (leave tracker cache)
  emp_service:appraisal_*:{id}            DB 5   5 min
  emp_service:hierarchy_*:{id}            DB 5   5 min
  emp_service:employee_id_card:{id}       DB 5   5 min
  emp_service:employee_queries:{id}       DB 5   1 min
  emp_service:festivals:{year}:{month}    DB 5   5 min
  emp_service:manager_pending_leaves:{id} DB 5   1 min
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Meeting keys (meeting_service):
  meeting:slots:{date}              DB 8   5 min    slot availability payload (JSON)
  meeting:lock:slot:{slot_key}      DB 8   20 sec   short-TTL SETNX lock  (prevents double-booking)
    meeting:pending:{auth_identifier} DB 8   90 sec   temporary hold while Razorpay checkout is open
    meeting:user:{auth_identifier}    DB 8   2 min    per-user booking list  (read acceleration)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Any, Dict, List, Optional
import os, json, redis, threading
from kafka import KafkaConsumer

# ================= SERVICE URLS =================

HOME_SERVICE_URL = os.getenv("HOME_SERVICE_URL","http://localhost:5001")
MEETING_SERVICE_URL = os.getenv("MEETING_SERVICE_URL","http://localhost:9000")
CHATBOT_SERVICE_URL = os.getenv("CHATBOT_SERVICE_URL","http://localhost:7600")
ASSET_SERVICE_URL = os.getenv("ASSET_SERVICE_URL","http://localhost:8090")
INTERNSHIP_SERVICE_URL = os.getenv("INTERNSHIP_SERVICE_URL","http://localhost:5050")
MS365_SERVICE_URL = os.getenv("MS365_SERVICE_URL","http://localhost:7700")
EMPLOYEE_SERVICE_URL = os.getenv("EMPLOYEE_SERVICE_URL","http://localhost:8002")
BLOGGER_SERVICE_URL = os.getenv("BLOGGER_SERVICE_URL","http://localhost:7500")
#REDIS_SERVICE_URL = os.getenv("REDIS_SERVICE_URL","http://localhost:6380")
BRS_SERVICE_URL = os.getenv("BRS_SERVICE_URL","http://localhost:8020")
LAMBDA_URL = 'https://lwug4xhfz27whiuu3acjfwsgtm0ttwja.lambda-url.eu-north-1.on.aws/'
STATIC_CDN = "https://d1pjjckqswt5z7.cloudfront.net"

CANONICAL_HOST = os.getenv("CANONICAL_HOST","www.chakorahub.com").strip().lower()
INTERNSHIP_PUBLIC_HOST = os.getenv("INTERNSHIP_PUBLIC_HOST","api.chakorahub.com").strip().lower()
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


app = FastAPI(title="redis_service", version="3.0")

# ─────────────────────────────────────────────
# DB INDEX CONSTANTS
# ─────────────────────────────────────────────
DB_SESSIONS   = 0
DB_PROFILES   = 1
DB_AUTH       = 2
DB_FREQ_DATA  = 3
DB_RATE_LIMIT = 4
DB_API_CACHE  = 5
DB_CHATBOT    = 6   # Response cache + embedding cache + conversation memory
DB_MEETING_INTELLIGENCE_KB = 7   # Meeting Intelligence / RAG index store
DB_CHATBOT_KB = DB_MEETING_INTELLIGENCE_KB   # legacy alias for existing chatbot endpoints
DB_MEETING    = 8   # Meeting service  (availability · locks · pending holds · user cache)
DB_RAG_VEC    = 9   # RAG dense vector store  (rag_service: rag:chunk:* · rag:doc:* · rag:doc:etags)
DB_RAG_BM25   = 10  # RAG BM25 inverted index (rag_service: rag:bm25:term:* · rag:bm25:meta)

# ─────────────────────────────────────────────
# CANONICAL TTLs  (seconds)
# ─────────────────────────────────────────────
TTL_SESSION      = 604_800   # 7 days
TTL_PROFILE      = 1_800     # 30 min
TTL_AUTH         = 3_600     # 1 hr
TTL_DASHBOARD    = 300       # 5 min
TTL_RESOURCES    = 7_200     # 2 hr
TTL_OFFERS       = 3_600     # 1 hr
TTL_FESTIVALS    = 86_400    # 24 hr
TTL_FEEDBACKS    = 600       # 10 min
TTL_REGISTRATION = 300       # 5 min
TTL_RATE_WINDOW  = 60        # 1-min sliding window
TTL_API_RESPONSE = 300       # 5 min

# Home-page TTLs
TTL_HOME_BATCHES  = 600
TTL_HOME_FEEDBACK = 300
TTL_HOME_OFFERS   = 900
TTL_HOME_ABOUT    = 900

_HOME_SECTIONS: Dict[str, int] = {
    "batches":  TTL_HOME_BATCHES,
    "feedback": TTL_HOME_FEEDBACK,
    "offers":   TTL_HOME_OFFERS,
    "about":    TTL_HOME_ABOUT,
}

# Blogger TTLs  (DB 5 — blog: namespace)
TTL_BLOG_LIST             = 1_200   # 20 min  — post list (base, no likes merged)
TTL_BLOG_POST             = 600     # 10 min  — single post full response
TTL_BLOG_STATS            = 1_800   # 30 min  — aggregate stats
TTL_BLOG_SUBSCRIBER_COUNT = 1_800   # 30 min  — subscriber count
TTL_BLOG_DASHBOARD        = 600     # 10 min  — combined dashboard payload
# blog:likes:{id} → persistent INCR counter, no TTL

# Billing TTLs  (DB 5 — billing namespace)
TTL_BILLING_INVOICE        = 300    # 5 min   — invoice:{transaction_id}
TTL_BILLING_PAYMENT_STATUS = 180    # 3 min   — payment_status:{transaction_id}
TTL_BILLING_HISTORY        = 300    # 5 min   — billing:history:{phone}
TTL_BILLING_USER_PHONE     = 1_800  # 30 min  — user:phone:{phone}
TTL_BILLING_COURSES        = 900    # 15 min  — courses:all

# Chatbot TTLs  (DB 6 — chat: / embedding: namespace)
TTL_CHAT_RESPONSE  = 1_800    # 30 min  — chat:response:{hash}  deduplicated LLM responses
TTL_CHAT_HISTORY   = 86_400   # 24 hr   — chat:history:{conv_id} message history
TTL_CHAT_EMBEDDING = 86_400   # 24 hr   — chat:embeddings:{conv_id} per-message vectors
TTL_CHAT_META      = 86_400   # 24 hr   — chat:meta:{conv_id} conversation metadata
TTL_EMBED_DOC      = 604_800  # 7 days  — embedding:{doc_id} pre-computed KB embeddings
# KB keys in DB 7 have no TTL — they are permanent until the next sync clears them

# Meeting TTLs  (DB 8 — meeting: namespace)
TTL_MEETING_SLOT_LOCK    = 20     # seconds — SETNX-style lock per slot (short; expires if checkout abandoned)
TTL_MEETING_PENDING_HOLD = 90     # seconds — temporary hold while Razorpay checkout is open
TTL_MEETING_SLOT_AVAIL   = 300    # seconds — slot availability cache (5 min)
TTL_MEETING_USER_CACHE   = 120    # seconds — per-user booking list (2 min read acceleration)

MEETING_SLOTS_KEY_PREFIX = "meeting:slots:"
MEETING_SLOT_LOCK_KEY_PREFIX = "meeting:lock:slot:"
MEETING_PENDING_HOLD_KEY_PREFIX = "meeting:pending:"
MEETING_USER_CACHE_KEY_PREFIX = "meeting:user:"

TTL_RESOURCES_SESSION     = 1_800   # 30 min  — user profile for /resources
TTL_RESOURCES_SHARED      = 300     # 5 min   — shared page content
TTL_RESOURCES_COURSES_GRID = 300    # 5 min   — course cards grid

# ─────────────────────────────────────────────
# CONNECTION HELPERS
# ─────────────────────────────────────────────

def _clean(raw: Optional[str]) -> str:
    if raw is None:
        return ""
    v = str(raw).strip()
    if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
        v = v[1:-1].strip()
    return v

def _env(name: str, default: str = "") -> str:
    return _clean(os.getenv(name)) or default

def _norm_pw(raw: str) -> Optional[str]:
    v = _clean(raw)
    return None if (not v or v.lower() in {"none", "null", "nil", "undefined"}) else v

def _is_no_auth_err(exc: Exception) -> bool:
    msg = str(exc).lower()
    return "called without any password" in msg or "no password is set" in msg

_clients: Dict[int, redis.Redis] = {}


def _meeting_slots_key(date: str) -> str:
    return f"{MEETING_SLOTS_KEY_PREFIX}{date}"


def _meeting_slot_lock_key(slot_key: str) -> str:
    return f"{MEETING_SLOT_LOCK_KEY_PREFIX}{slot_key}"


def _meeting_pending_hold_key(auth_identifier: str) -> str:
    return f"{MEETING_PENDING_HOLD_KEY_PREFIX}{auth_identifier}"


def _meeting_user_cache_key(auth_identifier: str) -> str:
    return f"{MEETING_USER_CACHE_KEY_PREFIX}{auth_identifier}"

def _build_client(
    db: int,
    password_override: Optional[str] = None,
    force_password_override: bool = False,
) -> redis.Redis:

    # 👉 Decide which Redis to use
    if db in (DB_MEETING_INTELLIGENCE_KB, DB_RAG_VEC, DB_RAG_BM25):   # DB 7, 9, 10 → RAG
        port = _REDIS_RAG_PORT
        pw   = _REDIS_RAG_PW
    else:                     # all others → cache
        port = _REDIS_CACHE_PORT
        pw   = _REDIS_CACHE_PW

    if force_password_override:
        pw = password_override

    return redis.Redis(
        host=_REDIS_HOST,
        port=port,
        db=db,
        password=pw,
        decode_responses=True,
        socket_timeout=2,
        socket_connect_timeout=2,
        retry_on_timeout=True,
        health_check_interval=30,
        socket_keepalive=True,
    )

def _get_client(db: int) -> redis.Redis:
    if db in _clients:
        return _clients[db]

    uses_rag = db in (DB_MEETING_INTELLIGENCE_KB, DB_RAG_VEC, DB_RAG_BM25)
    target_port = _REDIS_RAG_PORT if uses_rag else _REDIS_CACHE_PORT
    target_pw = _REDIS_RAG_PW if uses_rag else _REDIS_CACHE_PW

    try:
        c = _build_client(db)
        c.ping()
        _clients[db] = c
        print(f"[Redis] DB={db} connected on {_REDIS_HOST}:{target_port}")
        return c
    except Exception as exc:
        if target_pw and _is_no_auth_err(exc):
            c = _build_client(db, password_override=None, force_password_override=True)
            c.ping()
            _clients[db] = c
            print(f"[Redis] DB={db} connected on {_REDIS_HOST}:{target_port} (no auth fallback)")
            return c
        raise

_REDIS_HOST = _env("REDIS_HOST") or _env("REDIS_PRIVATE_HOST") or "localhost"
#_REDIS_PORT = int(_env("REDIS_CACHE_PORT", _env("REDIS_PORT", "6379")))
#_REDIS_PW   = _norm_pw(_env("REDIS_CACHE_PASSWORD", _env("REDIS_PASSWORD")))

# Cache Redis (6379)
_REDIS_CACHE_PORT = int(_env("REDIS_CACHE_PORT", "6379"))
_REDIS_CACHE_PW   = _norm_pw(_env("REDIS_CACHE_PASSWORD"))

# RAG Redis (DB 7). Default to cache Redis port unless explicitly overridden.
_REDIS_RAG_PORT = int(_env("REDIS_RAG_PORT", _env("REDIS_CACHE_PORT", "6379")))
_REDIS_RAG_PW   = _norm_pw(_env("REDIS_RAG_PASSWORD"))

# ── Pydantic models to add in the models section ────────────────────────────

class ResourcesSessionSetRequest(BaseModel):
    """
    Store the user-profile slice needed for /resources rendering.
    Written at login time (alongside the main session) so /resources
    never needs a Snowflake round-trip just to get username + profile_pic.

    key  : resources:session:{user_id}
    DB   : DB_API_CACHE (5)
    TTL  : 30 min (matches profile cache)
    """
    user_id:  str               # string — matches session.get("user_id")
    username: str
    usertype: str               # "student" | "admin" | "administrator"
    profile_pic: str = ""

class ResourcesSharedSetRequest(BaseModel):
    """
    Cache the shared /resources page content:
      offers        → dict  (course discounts etc.)
      festival_today → str | None  (e.g. "Diwali")
      greeting      → str | None  (dynamic greeting text)

    key : resources:shared
    DB  : DB_API_CACHE (5)
    TTL : 5 min
    Shared across ALL users — one key, no user_id.
    Written by student_service or app.py after fetching from Snowflake.
    """
    offers:         Any = None
    festival_today: Optional[str] = None
    greeting:       Optional[str] = None
    ttl:            int = TTL_RESOURCES_SHARED

class ResourcesCoursesGridSetRequest(BaseModel):
    """
    Cache the NRM_COURSES LEFT JOIN COURSE_RESOURCES grid
    that drives the course cards in resources.html.

    key : resources:courses_grid
    DB  : DB_API_CACHE (5)
    TTL : 5 min
    Shared across ALL users — one key.
    Invalidated by /admin/add-course and /admin/batch-schedule writes.
    """
    courses: Any      # list[dict] from Snowflake DictCursor
    ttl:     int = TTL_RESOURCES_COURSES_GRID


# ── Endpoint implementations (add after the existing /home/cache/* block) ───

# NOTE: In the actual redis_service.py these imports and the `app` object
# already exist.  Only the endpoint functions below need to be added.

# ─────────────────────────────────────────────────────────────────────────────
# Sub-system: Resources Session Profile
# key: resources:session:{user_id}   DB 5   TTL 30 min
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/resources/session/set")
def resources_session_set(payload: ResourcesSessionSetRequest):
    """
    Cache-Aside WRITE — user profile for the /resources page.

    Called at login time so /resources reads from here instead of
    hitting Snowflake on every page load.

    Network path:
      Windows EC2 (app.py) ──POST──► Linux EC2 (redis_service:6380)
                                       └─► Redis DB 5  resources:session:{user_id}
    """
    key = f"resources:session:{payload.user_id}"
    print(
        "📦 [resources/session/set] request | "
        f"user_id={payload.user_id} username={payload.username} usertype={payload.usertype} "
        f"profile_pic_present={bool(payload.profile_pic)} ttl={TTL_RESOURCES_SESSION}"
    )
    data = {
        "username":   payload.username,
        "usertype":   payload.usertype,
        "profile_pic": payload.profile_pic,
    }
    try:
        _get_client(DB_API_CACHE).setex(key, TTL_RESOURCES_SESSION, json.dumps(data))
        print(f"✅ [resources/session/set] stored key={key}")
        return {"success": True, "key": key, "ttl": TTL_RESOURCES_SESSION}
    except Exception as exc:
        print(f"❌ [resources/session/set] failed key={key}: {exc}")
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


@app.get("/resources/session/get")
def resources_session_get(user_id: str = Query(..., min_length=1)):
    """
    Cache-Aside READ — return cached user profile for /resources.

    Returns found=False on cache MISS; app.py then falls back to the
    Flask session dict and renders the page with whatever is there.

    Response shape:
      { success, found, data: { username, usertype, profile_pic } | null }
    """
    key = f"resources:session:{user_id}"
    try:
        print(f"🔎 [resources/session/get] lookup key={key}")
        raw = _get_client(DB_API_CACHE).get(key)
        if raw is None:
            print(f"⚠️ [resources/session/get] miss key={key}")
            return {"success": True, "found": False, "data": None}
        print(f"✅ [resources/session/get] hit key={key}")
        return {"success": True, "found": True, "key": key, "data": json.loads(raw)}
    except Exception as exc:
        print(f"❌ [resources/session/get] failed key={key}: {exc}")
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


@app.delete("/resources/session/delete")
def resources_session_delete(user_id: str = Query(..., min_length=1)):
    """
    Cache-Aside INVALIDATE — evict user profile cache on logout or profile update.

    Call this from:
      • /logout route in app.py
      • Any route that updates username / profile_pic / usertype
    """
    key = f"resources:session:{user_id}"
    try:
        deleted = _get_client(DB_API_CACHE).delete(key)
        return {"success": True, "key": key, "deleted": int(deleted) > 0}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


# ─────────────────────────────────────────────────────────────────────────────
# Sub-system: Resources Shared Content
# key: resources:shared   DB 5   TTL 5 min
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/resources/shared/set")
def resources_shared_set(payload: ResourcesSharedSetRequest):
    """
    Cache-Aside WRITE — shared /resources page content (offers, festival, greeting).

    One global key for all users.  Written by app.py after a successful
    fetch from student_service on a cache MISS.

    Network path on MISS:
      app.py ──GET──► student_service ──► Snowflake
      app.py ──POST──► redis_service /resources/shared/set  (write-back)
    """
    key = "resources:shared"
    data = {
        "offers":         payload.offers,
        "festival_today": payload.festival_today,
        "greeting":       payload.greeting,
    }
    try:
        _get_client(DB_API_CACHE).setex(key, payload.ttl, json.dumps(data, default=str))
        return {"success": True, "key": key, "ttl": payload.ttl}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


@app.get("/resources/shared/get")
def resources_shared_get():
    """
    Cache-Aside READ — shared /resources page content.

    Response shape:
      { success, found, data: { offers, festival_today, greeting } | null }

    On found=False app.py fetches from student_service and calls /set.
    """
    key = "resources:shared"
    try:
        raw = _get_client(DB_API_CACHE).get(key)
        if raw is None:
            return {"success": True, "found": False, "data": None}
        return {"success": True, "found": True, "key": key, "data": json.loads(raw)}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


@app.delete("/resources/shared/delete")
def resources_shared_delete():
    """
    Cache-Aside INVALIDATE — force-refresh shared content.

    Call from admin routes that change offers, festival data, or the greeting.
    """
    try:
        deleted = _get_client(DB_API_CACHE).delete("resources:shared")
        return {"success": True, "key": "resources:shared", "deleted": int(deleted) > 0}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


# ─────────────────────────────────────────────────────────────────────────────
# Sub-system: Resources Courses Grid
# key: resources:courses_grid   DB 5   TTL 5 min
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/resources/courses-grid/set")
def resources_courses_grid_set(payload: ResourcesCoursesGridSetRequest):
    """
    Cache-Aside WRITE — NRM_COURSES LEFT JOIN COURSE_RESOURCES result.

    Written by app.py after a Snowflake query on cache MISS.
    One global key — same course list for all users.

    Network path on MISS:
      app.py ──SQL──► Snowflake  (NRM_COURSES LEFT JOIN COURSE_RESOURCES)
      app.py ──POST──► redis_service /resources/courses-grid/set  (write-back)
    """
    key = "resources:courses_grid"
    try:
        _get_client(DB_API_CACHE).setex(key, payload.ttl, json.dumps(payload.courses, default=str))
        return {"success": True, "key": key, "ttl": payload.ttl, "count": len(payload.courses) if isinstance(payload.courses, list) else "?"}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


@app.get("/resources/courses-grid/get")
def resources_courses_grid_get():
    """
    Cache-Aside READ — course cards grid for resources.html.

    Response shape:
      { success, found, count, data: list[dict] | null }

    On found=False app.py hits Snowflake and calls /set (write-back).
    """
    key = "resources:courses_grid"
    try:
        print(f"🔎 [resources/courses-grid/get] lookup key={key}")
        raw = _get_client(DB_API_CACHE).get(key)
        if raw is None:
            print(f"⚠️ [resources/courses-grid/get] miss key={key}")
            return {"success": True, "found": False, "count": 0, "data": None}
        data = json.loads(raw)
        print(
            "✅ [resources/courses-grid/get] hit | "
            f"key={key} count={len(data) if isinstance(data, list) else 0}"
        )
        return {
            "success": True, "found": True,
            "key": key, "count": len(data) if isinstance(data, list) else 0,
            "data": data,
        }
    except Exception as exc:
        print(f"❌ [resources/courses-grid/get] failed key={key}: {exc}")
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


@app.delete("/resources/courses-grid/delete")
def resources_courses_grid_delete():
    """
    Cache-Aside INVALIDATE — evict the course grid cache.

    Call from:
      • /admin/add-course  (new course added)
      • /admin/batch-schedule  (batch status changes may affect course display)
      • Any route that edits COURSE_RESOURCES (PPT, code, IQ links)
    """
    try:
        deleted = _get_client(DB_API_CACHE).delete("resources:courses_grid")
        return {"success": True, "key": "resources:courses_grid", "deleted": int(deleted) > 0}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


@app.get("/resources/courses-grid/ttl")
def resources_courses_grid_ttl():
    """Return remaining TTL on the courses grid cache (useful for debug)."""
    try:
        ttl = _get_client(DB_API_CACHE).ttl("resources:courses_grid")
        return {"success": True, "key": "resources:courses_grid", "ttl_seconds": ttl}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


# ─────────────────────────────────────────────────────────────────────────────
# Sub-system: Resources bulk invalidation (admin convenience)
# ─────────────────────────────────────────────────────────────────────────────

@app.delete("/resources/invalidate-all")
def resources_invalidate_all(user_id: Optional[str] = Query(None)):
    """
    Flush all /resources cache keys in one call.

    Pass ?user_id=<id> to also evict the per-user session profile.
    Useful after a bulk admin operation (e.g. re-importing all courses).

    Deletes:
      resources:shared           (DB 5)
      resources:courses_grid     (DB 5)
      resources:session:{user_id} (DB 5, only if user_id provided)
    """
    client = _get_client(DB_API_CACHE)
    keys_to_delete = ["resources:shared", "resources:courses_grid"]
    if user_id:
        keys_to_delete.append(f"resources:session:{user_id}")
    try:
        deleted = client.delete(*keys_to_delete)
        return {
            "success": True,
            "keys_attempted": keys_to_delete,
            "deleted_count": int(deleted),
        }
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


# ─────────────────────────────────────────────
# PYDANTIC MODELS
# ─────────────────────────────────────────────
class SetRequest(BaseModel):
    key: str; value: Any; db: int = 0; ttl: Optional[int] = None

class DeleteRequest(BaseModel):
    keys: List[str]; db: int = 0

class SAddRequest(BaseModel):
    key: str; members: List[str]; db: int = 0

class SessionSetRequest(BaseModel):
    user_id: int                        # key → session:{user_id}
    data: Dict[str, Any]
    ttl: int = TTL_SESSION

class ProfileSetRequest(BaseModel):
    user_id: int                        # key → user:{user_id}
    data: Dict[str, Any]
    ttl: int = TTL_PROFILE

class AuthSetRequest(BaseModel):
    user_id: int                        # key → roles:{user_id}
    roles: List[str]
    usertype: str
    ttl: int = TTL_AUTH

class FreqDataSetRequest(BaseModel):
    key: str; data: Any; ttl: int = TTL_RESOURCES

class ApiCacheSetRequest(BaseModel):
    cache_key: str; response: Any; ttl: int = TTL_API_RESPONSE

class HomeCacheSetRequest(BaseModel):
    section: str; data: Any; ttl: Optional[int] = None

# ── Chatbot models ────────────────────────────────────────────
class ChatResponseCacheSet(BaseModel):
    """
    Cache a deduplicated LLM response in DB 6.
    response_hash : SHA-256 of (user_message + context_fingerprint) - caller computes
    response_text : the raw LLM response string
    ttl           : defaults to 30 min (TTL_CHAT_RESPONSE)
    """
    response_hash: str
    response_text: str
    ttl: int = TTL_CHAT_RESPONSE

class ChatHistoryAppend(BaseModel):
    """
    Append one turn to a conversation history list (DB 6).
    key: chat:history:{conversation_id}  TTL reset to 24 hr on every write.
    """
    conversation_id: str
    role:    str            # "user" | "assistant"
    content: str
    user_id: str = "anonymous"

class ChatEmbeddingAppend(BaseModel):
    """
    Append a per-message embedding to the conversation embedding array (DB 6).
    key: chat:embeddings:{conversation_id}  TTL 24 hr
    """
    conversation_id: str
    text:   str
    vector: List[float]

class DocEmbeddingSet(BaseModel):
    """
    Cache a pre-computed embedding vector for a KB document chunk (DB 6).
    key: embedding:{doc_id}  TTL 7 days
    doc_id : any stable identifier (chunk hash, S3 ETag hash, etc.)
    vector : list[float] from sentence-transformer
    """
    doc_id: str
    vector: List[float]
    ttl: int = TTL_EMBED_DOC

# ── Blogger models ────────────────────────────────────────────────────────────
class BlogCacheSetRequest(BaseModel):
    """
    Write a blog API-response blob to DB 5.
    key  : one of  blog:list | blog:{id} | blog:stats |
                   blog:subscriber_count | blog:dashboard
    data : any JSON-serialisable object
    ttl  : optional override; omit to use the canonical TTL for that key
    """
    key:  str
    data: Any
    ttl:  Optional[int] = None

class BlogLikeSeedItem(BaseModel):
    post_id:    int
    like_count: int

class BlogLikeSeedRequest(BaseModel):
    """
    Bulk-seed like counters from Snowflake at startup.
    Uses SETNX so live counters are never overwritten on restart.
    """
    items: List[BlogLikeSeedItem]

# ── Billing models ────────────────────────────────────────────────────────────
class BillingCacheSetRequest(BaseModel):
    """
    Generic billing cache write.
    key  : must start with one of the approved billing prefixes
           (invoice: | payment_status: | billing:history: | user:phone: | courses:)
    value: any JSON-serialisable object
    ttl  : caller may override; if omitted the canonical TTL for the prefix is used
    """
    key:   str
    value: Any
    ttl:   Optional[int] = None


# ═════════════════════════════════════════════════════
# HEALTH
# ═════════════════════════════════════════════════════
@app.get("/health")
def health(db: int = Query(0, ge=0)):
    try:
        _get_client(db).ping()
        return {"success": True, "service": "redis_service", "version": "3.0", "db": db}
    except Exception as exc:
        return JSONResponse(status_code=503, content={"success": False, "message": str(exc), "db": db})


# ═════════════════════════════════════════════════════
# 1. SESSION CACHE  DB 0   key: session:{user_id}  TTL 7 days
# ═════════════════════════════════════════════════════
@app.post("/session/set")
def session_set(payload: SessionSetRequest):
    """Create or refresh a user session."""
    try:
        key = f"session:{payload.user_id}"
        _get_client(DB_SESSIONS).setex(key, payload.ttl, json.dumps(payload.data))
        return {"success": True, "key": key, "ttl": payload.ttl}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})

@app.get("/session/get")
def session_get(user_id: int = Query(..., ge=1)):
    """Retrieve session data for a user."""
    try:
        raw = _get_client(DB_SESSIONS).get(f"session:{user_id}")
        if raw is None:
            return {"success": True, "found": False, "data": None}
        return {"success": True, "found": True, "data": json.loads(raw)}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})

@app.delete("/session/delete")
def session_delete(user_id: int = Query(..., ge=1)):
    """Invalidate session on logout."""
    try:
        deleted = _get_client(DB_SESSIONS).delete(f"session:{user_id}")
        return {"success": True, "deleted": int(deleted)}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})

@app.post("/session/refresh")
def session_refresh(user_id: int = Query(..., ge=1), ttl: int = Query(TTL_SESSION)):
    """Slide the TTL of an existing session (keep-alive)."""
    try:
        result = _get_client(DB_SESSIONS).expire(f"session:{user_id}", ttl)
        return {"success": True, "refreshed": bool(result)}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})

@app.get("/session/ttl")
def session_ttl(user_id: int = Query(..., ge=1)):
    try:
        ttl = _get_client(DB_SESSIONS).ttl(f"session:{user_id}")
        return {"success": True, "key": f"session:{user_id}", "ttl_seconds": ttl}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


# ═════════════════════════════════════════════════════
# 2. USER PROFILE CACHE  DB 1   key: user:{user_id}  TTL 30 min
# ═════════════════════════════════════════════════════
@app.post("/profile/set")
def profile_set(payload: ProfileSetRequest):
    try:
        key = f"user:{payload.user_id}"
        _get_client(DB_PROFILES).setex(key, payload.ttl, json.dumps(payload.data))
        return {"success": True, "key": key, "ttl": payload.ttl}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})

@app.get("/profile/get")
def profile_get(user_id: int = Query(..., ge=1)):
    try:
        raw = _get_client(DB_PROFILES).get(f"user:{user_id}")
        if raw is None:
            return {"success": True, "found": False, "data": None}
        return {"success": True, "found": True, "data": json.loads(raw)}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})

@app.delete("/profile/delete")
def profile_delete(user_id: int = Query(..., ge=1)):
    try:
        deleted = _get_client(DB_PROFILES).delete(f"user:{user_id}")
        return {"success": True, "deleted": int(deleted)}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


# ═════════════════════════════════════════════════════
# 3. AUTH / ROLES CACHE  DB 2   key: roles:{user_id}  TTL 1 hr
# ═════════════════════════════════════════════════════
@app.post("/auth/set")
def auth_set(payload: AuthSetRequest):
    try:
        key = f"roles:{payload.user_id}"
        data = {"roles": payload.roles, "usertype": payload.usertype}
        _get_client(DB_AUTH).setex(key, payload.ttl, json.dumps(data))
        return {"success": True, "key": key, "ttl": payload.ttl}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})

@app.get("/auth/get")
def auth_get(user_id: int = Query(..., ge=1)):
    try:
        raw = _get_client(DB_AUTH).get(f"roles:{user_id}")
        if raw is None:
            return {"success": True, "found": False, "data": None}
        return {"success": True, "found": True, "data": json.loads(raw)}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})

@app.delete("/auth/delete")
def auth_delete(user_id: int = Query(..., ge=1)):
    try:
        deleted = _get_client(DB_AUTH).delete(f"roles:{user_id}")
        return {"success": True, "deleted": int(deleted)}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


# ═════════════════════════════════════════════════════
# 4. FREQUENTLY USED DATA  DB 3
#    Generic key/value; caller controls the key name.
#    student_service uses:
#      resources:{subject}:{lang}  → videos      2 hr
#      resources:{subject}:files   → PPT/code/IQ 2 hr
#      offers:{date}               →             1 hr
#      festival:{date}             →            24 hr
#      feedbacks:latest            →            10 min
#      registration:{user_id}      →             5 min
# ═════════════════════════════════════════════════════
@app.post("/freq/set")
def freq_set(payload: FreqDataSetRequest):
    try:
        value = json.dumps(payload.data) if not isinstance(payload.data, str) else payload.data
        _get_client(DB_FREQ_DATA).setex(payload.key, payload.ttl, value)
        return {"success": True, "key": payload.key, "ttl": payload.ttl}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})

@app.get("/freq/get")
def freq_get(key: str = Query(..., min_length=1)):
    try:
        raw = _get_client(DB_FREQ_DATA).get(key)
        if raw is None:
            return {"success": True, "found": False, "data": None}
        try:
            data = json.loads(raw)
        except Exception:
            data = raw
        return {"success": True, "found": True, "data": data}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})

@app.delete("/freq/delete")
def freq_delete(key: str = Query(..., min_length=1)):
    try:
        deleted = _get_client(DB_FREQ_DATA).delete(key)
        return {"success": True, "deleted": int(deleted)}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})

@app.get("/freq/ttl")
def freq_ttl(key: str = Query(..., min_length=1)):
    try:
        ttl = _get_client(DB_FREQ_DATA).ttl(key)
        return {"success": True, "key": key, "ttl_seconds": ttl}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


# ═════════════════════════════════════════════════════
# 5. RATE LIMITING  DB 4   key: rl:{endpoint}:{identifier}
#    Atomic INCR + EXPIRE pipeline — no race conditions.
#    Fails open if Redis is unavailable.
# ═════════════════════════════════════════════════════
@app.post("/ratelimit/check")
def ratelimit_check(
    identifier: str = Query(..., description="IP address or user_id"),
    endpoint:   str = Query(..., description="Route path, e.g. /api/student/login"),
    limit:      int = Query(10,              description="Max requests per window"),
    window:     int = Query(TTL_RATE_WINDOW, description="Window size in seconds"),
):
    try:
        key = f"rl:{endpoint}:{identifier}"
        client = _get_client(DB_RATE_LIMIT)
        pipe = client.pipeline()
        pipe.incr(key)
        pipe.expire(key, window)
        results = pipe.execute()
        count = int(results[0])
        return {
            "success": True, "allowed": count <= limit,
            "count": count, "limit": limit,
            "window": window, "ttl_remaining": client.ttl(key),
        }
    except Exception as exc:
        return {"success": False, "allowed": True, "message": str(exc)}   # fail open

@app.delete("/ratelimit/reset")
def ratelimit_reset(identifier: str = Query(...), endpoint: str = Query(...)):
    try:
        deleted = _get_client(DB_RATE_LIMIT).delete(f"rl:{endpoint}:{identifier}")
        return {"success": True, "deleted": int(deleted)}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


# ═════════════════════════════════════════════════════
# 6. API RESPONSE CACHE  DB 5
#    Generic full-response cache.  Caller controls key.
#    student_service uses: student:dashboard:{user_id}  5 min
#    employee_service uses:
#      employee:{id}               15 min   personal details
#      salary:{id}                  2 min   short TTL — sensitive
#      leave:{id}                   5 min   leave tracker
#      emp_service:*                varies  see docstring above
# ═════════════════════════════════════════════════════
@app.post("/apicache/set")
def apicache_set(payload: ApiCacheSetRequest):
    try:
        value = json.dumps(payload.response) if not isinstance(payload.response, str) else payload.response
        _get_client(DB_API_CACHE).setex(payload.cache_key, payload.ttl, value)
        return {"success": True, "cache_key": payload.cache_key, "ttl": payload.ttl}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})

@app.get("/apicache/get")
def apicache_get(cache_key: str = Query(..., min_length=1)):
    try:
        raw = _get_client(DB_API_CACHE).get(cache_key)
        if raw is None:
            return {"success": True, "found": False, "data": None}
        try:
            data = json.loads(raw)
        except Exception:
            data = raw
        return {"success": True, "found": True, "data": data}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})

@app.delete("/apicache/delete")
def apicache_delete(cache_key: str = Query(..., min_length=1)):
    try:
        deleted = _get_client(DB_API_CACHE).delete(cache_key)
        return {"success": True, "deleted": int(deleted)}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})

@app.get("/apicache/ttl")
def apicache_ttl(cache_key: str = Query(..., min_length=1)):
    try:
        ttl = _get_client(DB_API_CACHE).ttl(cache_key)
        return {"success": True, "cache_key": cache_key, "ttl_seconds": ttl}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


# ═════════════════════════════════════════════════════
# 7. HOME PAGE CACHE  (DB 5, home: namespace)
#    Caller: home_service.py
#    Keys: home:batches | home:feedback | home:offers | home:about
# ═════════════════════════════════════════════════════
@app.post("/home/cache/set")
def home_cache_set(payload: HomeCacheSetRequest):
    section = (payload.section or "").strip().lower()
    if section not in _HOME_SECTIONS:
        return JSONResponse(status_code=400, content={
            "success": False,
            "message": f"Unknown section '{section}'. Valid: {list(_HOME_SECTIONS)}",
        })
    ttl = payload.ttl if payload.ttl and payload.ttl > 0 else _HOME_SECTIONS[section]
    key = f"home:{section}"
    try:
        value = json.dumps(payload.data) if not isinstance(payload.data, str) else payload.data
        _get_client(DB_API_CACHE).setex(key, ttl, value)
        return {"success": True, "cache_key": key, "ttl": ttl}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})

@app.get("/home/cache/get")
def home_cache_get(section: str = Query(..., min_length=1)):
    section = section.strip().lower()
    if section not in _HOME_SECTIONS:
        return JSONResponse(status_code=400, content={
            "success": False,
            "message": f"Unknown section '{section}'. Valid: {list(_HOME_SECTIONS)}",
        })
    try:
        raw = _get_client(DB_API_CACHE).get(f"home:{section}")
        if raw is None:
            return {"success": True, "found": False, "data": None}
        try:
            data = json.loads(raw)
        except Exception:
            data = raw
        return {"success": True, "found": True, "cache_key": f"home:{section}", "data": data}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})

@app.delete("/home/cache/delete")
def home_cache_delete(section: str = Query(..., min_length=1)):
    """Pass section='*' to flush all home cache entries at once."""
    section = section.strip().lower()
    try:
        if section == "*":
            client = _get_client(DB_API_CACHE)
            deleted_keys = [f"home:{s}" for s in _HOME_SECTIONS if client.delete(f"home:{s}")]
            return {"success": True, "deleted_keys": deleted_keys}
        if section not in _HOME_SECTIONS:
            return JSONResponse(status_code=400, content={
                "success": False,
                "message": f"Unknown section '{section}'. Valid: {list(_HOME_SECTIONS)} or '*'",
            })
        deleted = _get_client(DB_API_CACHE).delete(f"home:{section}")
        return {"success": True, "cache_key": f"home:{section}", "deleted": int(deleted)}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})

@app.get("/home/cache/ttl")
def home_cache_ttl(section: str = Query(..., min_length=1)):
    section = section.strip().lower()
    if section not in _HOME_SECTIONS:
        return JSONResponse(status_code=400, content={"success": False, "message": f"Unknown section '{section}'."})
    try:
        ttl = _get_client(DB_API_CACHE).ttl(f"home:{section}")
        return {"success": True, "cache_key": f"home:{section}", "ttl_seconds": ttl}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


# ═════════════════════════════════════════════════════
# GENERIC LOW-LEVEL ENDPOINTS  (backward compatibility)
# ═════════════════════════════════════════════════════
@app.get("/redis/get")
def redis_get(key: str = Query(..., min_length=1), db: int = Query(0, ge=0)):
    try:
        value = _get_client(db).get(key)
        return {"success": True, "found": value is not None, "value": value}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})

@app.post("/redis/set")
def redis_set(payload: SetRequest):
    try:
        client = _get_client(payload.db)
        value = payload.value if isinstance(payload.value, str) else str(payload.value)
        if payload.ttl is not None:
            client.setex(payload.key, int(payload.ttl), value)
        else:
            client.set(payload.key, value)
        return {"success": True, "key": payload.key, "db": payload.db}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})

@app.post("/redis/delete")
def redis_delete(payload: DeleteRequest):
    try:
        if not payload.keys:
            return {"success": True, "deleted": 0, "db": payload.db}
        deleted = _get_client(payload.db).delete(*payload.keys)
        return {"success": True, "deleted": int(deleted), "db": payload.db}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})

@app.get("/redis/exists")
def redis_exists(key: str = Query(..., min_length=1), db: int = Query(0, ge=0)):
    try:
        return {"success": True, "exists": _get_client(db).exists(key) == 1, "db": db}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})

@app.get("/redis/scard")
def redis_scard(key: str = Query(..., min_length=1), db: int = Query(0, ge=0)):
    try:
        return {"success": True, "count": int(_get_client(db).scard(key)), "db": db}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})

@app.post("/redis/sadd")
def redis_sadd(payload: SAddRequest):
    try:
        if not payload.members:
            return {"success": True, "added": 0, "db": payload.db}
        added = _get_client(payload.db).sadd(payload.key, *payload.members)
        return {"success": True, "added": int(added), "db": payload.db}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})

@app.get("/redis/scan")
def redis_scan(pattern: str = Query("*"), db: int = Query(0, ge=0)):
    try:
        keys = list(_get_client(db).scan_iter(pattern))
        return {"success": True, "keys": keys, "count": len(keys), "db": db}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})

@app.get("/redis/ttl")
def redis_ttl(key: str = Query(..., min_length=1), db: int = Query(0, ge=0)):
    try:
        return {"success": True, "key": key, "ttl_seconds": _get_client(db).ttl(key), "db": db}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


def _consume_rag_index_updated():
    try:
        consumer = KafkaConsumer(
            "rag.index.updated",
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id="redis-rag-index-updated-group",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
        print("✅ Kafka consumer listening on: rag.index.updated")
    except Exception as exc:
        print(f"⚠️ Kafka consumer failed to start (rag.index.updated): {exc}")
        return

    for message in consumer:
        print(f"📥 Kafka consume ← {message.topic} | partition={message.partition} offset={message.offset}")
        event = message.value or {}
        booking_id = event.get("booking_id")
        if not booking_id:
            print(f"⚠️ Invalid rag.index.updated payload: {event}")
            continue
        try:
            client = _get_client(DB_MEETING_INTELLIGENCE_KB)
            status_key = f"meeting:intelligence:index-status:{booking_id}"
            client.set(status_key, json.dumps(event))
            client.sadd("meeting:intelligence:index-updated", booking_id)
            print(f"✅ meeting intelligence index status stored | booking_id={booking_id}")
        except Exception as exc:
            print(f"❌ Failed to store meeting intelligence index status | booking_id={booking_id} | {exc}")

# ═════════════════════════════════════════════════════════════════════════════
# 8. BILLING CACHE  (DB 5 — billing namespace)
#
# Strategy summary
# ─────────────────────────────────────────────────────────────────────────────
#  CACHED (short TTL):
#    invoice:{transaction_id}         5 min  — lightweight read helper
#    payment_status:{transaction_id}  3 min  — short; DB is the authoritative source
#    billing:history:{phone}          5 min  — evicted on every write mutation
#    user:phone:{phone}              30 min  — stable lookup, saves Snowflake round-trips
#    courses:all                     15 min  — rarely changes
#
#  NOT CACHED (by design):
#    x  presigned S3 URLs            — contain short-lived credentials; always generate fresh
#    x  auth password hashes         — security risk; always verify against DB
#    x  payment state blindly        — DB is always authoritative; max 3-min TTL
#    x  billing:history long-TTL     — stale after every create / approve
# ═════════════════════════════════════════════════════════════════════════════

# Approved key prefixes and their canonical TTLs.
_BILLING_PREFIX_TTL: Dict[str, int] = {
    "invoice:":          TTL_BILLING_INVOICE,
    "payment_status:":   TTL_BILLING_PAYMENT_STATUS,
    "billing:history:":  TTL_BILLING_HISTORY,
    "user:phone:":       TTL_BILLING_USER_PHONE,
    "courses:":          TTL_BILLING_COURSES,
}


def _billing_canonical_ttl(key: str) -> Optional[int]:
    """Return canonical TTL for a billing key, or None if prefix unknown."""
    for prefix, ttl in _BILLING_PREFIX_TTL.items():
        if key.startswith(prefix):
            return ttl
    return None


@app.post("/billing/cache/set")
def billing_cache_set(payload: BillingCacheSetRequest):
    """
    Store a billing cache entry in DB 5.
    TTL resolves as: explicit payload.ttl -> canonical prefix TTL.
    Rejects keys that don't match any approved billing prefix.
    """
    key = (payload.key or "").strip()
    if not key:
        return JSONResponse(status_code=400, content={"success": False, "message": "key is required"})

    canonical_ttl = _billing_canonical_ttl(key)
    if canonical_ttl is None:
        return JSONResponse(
            status_code=400,
            content={
                "success": False,
                "message": (
                    f"Key '{key}' does not match any approved billing prefix. "
                    f"Allowed prefixes: {list(_BILLING_PREFIX_TTL)}"
                ),
            },
        )
    ttl = payload.ttl if (payload.ttl and payload.ttl > 0) else canonical_ttl
    try:
        value = json.dumps(payload.value) if not isinstance(payload.value, str) else payload.value
        _get_client(DB_API_CACHE).setex(key, ttl, value)
        return {"success": True, "key": key, "ttl": ttl}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


@app.get("/billing/cache/get")
def billing_cache_get(key: str = Query(..., min_length=1)):
    """Retrieve a billing cache entry from DB 5."""
    key = key.strip()
    if _billing_canonical_ttl(key) is None:
        return JSONResponse(
            status_code=400,
            content={"success": False, "message": f"Key '{key}' is not in the billing namespace."},
        )
    try:
        raw = _get_client(DB_API_CACHE).get(key)
        if raw is None:
            return {"success": True, "found": False, "data": None}
        try:
            data = json.loads(raw)
        except Exception:
            data = raw
        return {"success": True, "found": True, "key": key, "data": data}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


@app.delete("/billing/cache/delete")
def billing_cache_delete(key: str = Query(..., min_length=1)):
    """
    Evict a specific billing cache key.
    Call this after any payment mutation (create, approve) to prevent
    serving stale billing:history or payment_status data.
    """
    key = key.strip()
    if _billing_canonical_ttl(key) is None:
        return JSONResponse(
            status_code=400,
            content={"success": False, "message": f"Key '{key}' is not in the billing namespace."},
        )
    try:
        deleted = _get_client(DB_API_CACHE).delete(key)
        return {"success": True, "key": key, "deleted": int(deleted)}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


@app.delete("/billing/cache/delete-pattern")
def billing_cache_delete_pattern(pattern: str = Query(..., min_length=1)):
    """
    Evict all billing cache keys matching a glob pattern (SCAN-based, non-blocking).
    Pattern must start with a known billing prefix.
    Example: DELETE /billing/cache/delete-pattern?pattern=billing:history:*
    """
    pattern = pattern.strip()
    prefix_matched = any(
        pattern.startswith(p) or p.startswith(pattern.split(":")[0] + ":")
        for p in _BILLING_PREFIX_TTL
    )
    if not prefix_matched:
        return JSONResponse(
            status_code=400,
            content={
                "success": False,
                "message": (
                    f"Pattern '{pattern}' does not start with a billing prefix. "
                    f"Allowed prefixes: {list(_BILLING_PREFIX_TTL)}"
                ),
            },
        )
    try:
        client  = _get_client(DB_API_CACHE)
        pipe    = client.pipeline()
        evicted = []
        for matched_key in client.scan_iter(pattern, count=100):
            pipe.delete(matched_key)
            evicted.append(matched_key)
        pipe.execute()
        return {"success": True, "pattern": pattern, "evicted": evicted, "count": len(evicted)}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


@app.get("/billing/cache/ttl")
def billing_cache_ttl(key: str = Query(..., min_length=1)):
    """Return the remaining TTL for a billing cache key."""
    key = key.strip()
    if _billing_canonical_ttl(key) is None:
        return JSONResponse(
            status_code=400,
            content={"success": False, "message": f"Key '{key}' is not in the billing namespace."},
        )
    try:
        ttl = _get_client(DB_API_CACHE).ttl(key)
        return {"success": True, "key": key, "ttl_seconds": ttl}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


# ═════════════════════════════════════════════════════════════════════════════
# 9. BLOGGER CACHE  (DB 5 — blog: namespace)
#
# Sub-system A — API Response Cache  (TTL-based JSON blobs)
# ─────────────────────────────────────────────────────────
#   blog:list              20 min   post list (base; likes merged at read-time)
#   blog:{id}              10 min   single post full response
#   blog:stats             30 min   aggregate stats
#   blog:subscriber_count  30 min   subscriber count
#   blog:dashboard         10 min   combined dashboard payload
#
# Sub-system B — Like Counters  (persistent INCR counters, no TTL)
# ─────────────────────────────────────────────────────────────────
#   blog:likes:{id}        —        atomic INCR / MGET bulk / SETNX seed
#
# Caller: blogger_service.py  (zero redis imports there; all cache I/O via HTTP)
# ═════════════════════════════════════════════════════════════════════════════

# Approved blog response-cache keys → canonical TTL
_BLOG_EXACT_TTL: Dict[str, int] = {
    "blog:list":             TTL_BLOG_LIST,
    "blog:stats":            TTL_BLOG_STATS,
    "blog:subscriber_count": TTL_BLOG_SUBSCRIBER_COUNT,
    "blog:dashboard":        TTL_BLOG_DASHBOARD,
}


def _blog_response_ttl(key: str) -> Optional[int]:
    """
    Return canonical TTL for a blog API-response cache key.
    Handles exact keys (blog:list, blog:stats …) and parameterised
    blog:{integer} keys (single-post cache).
    Returns None for unrecognised keys.
    """
    if key in _BLOG_EXACT_TTL:
        return _BLOG_EXACT_TTL[key]
    # blog:{integer}  e.g.  blog:42
    parts = key.split(":", 1)
    if len(parts) == 2 and parts[0] == "blog" and parts[1].lstrip("-").isdigit():
        return TTL_BLOG_POST
    return None


# ── Sub-system A: API Response Cache ─────────────────────────────────────────

@app.post("/blogger/cache/set")
def blogger_cache_set(payload: BlogCacheSetRequest):
    """
    Store a blog API-response blob in DB 5.

    Approved keys
    ─────────────
      blog:list | blog:{id} | blog:stats | blog:subscriber_count | blog:dashboard

    TTL resolution:  explicit payload.ttl  →  canonical key TTL (see constants above)
    """
    key = (payload.key or "").strip()
    if not key:
        return JSONResponse(
            status_code=400,
            content={"success": False, "message": "key is required"},
        )
    canonical = _blog_response_ttl(key)
    if canonical is None:
        return JSONResponse(
            status_code=400,
            content={
                "success": False,
                "message": (
                    f"'{key}' is not an approved blog cache key. "
                    "Approved: blog:list | blog:{{id}} | blog:stats | "
                    "blog:subscriber_count | blog:dashboard"
                ),
            },
        )
    ttl = payload.ttl if (payload.ttl and payload.ttl > 0) else canonical
    try:
        value = json.dumps(payload.data) if not isinstance(payload.data, str) else payload.data
        _get_client(DB_API_CACHE).setex(key, ttl, value)
        return {"success": True, "key": key, "ttl": ttl}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


@app.get("/blogger/cache/get")
def blogger_cache_get(key: str = Query(..., min_length=1)):
    """
    Retrieve a blog API-response blob from DB 5.

    key: blog:list | blog:{id} | blog:stats | blog:subscriber_count | blog:dashboard
    Returns found=False (not 404) on cache MISS so callers can fall through to Snowflake.
    """
    key = key.strip()
    if _blog_response_ttl(key) is None:
        return JSONResponse(
            status_code=400,
            content={"success": False, "message": f"'{key}' is not a recognised blog cache key."},
        )
    try:
        raw = _get_client(DB_API_CACHE).get(key)
        if raw is None:
            return {"success": True, "found": False, "data": None}
        try:
            data = json.loads(raw)
        except Exception:
            data = raw
        return {"success": True, "found": True, "key": key, "data": data}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


@app.delete("/blogger/cache/delete")
def blogger_cache_delete(key: str = Query(..., min_length=1)):
    """
    Evict a blog cache entry or a glob pattern.

    Single key  :  key=blog:42
    Pattern     :  key=blog:*   (flushes all response blobs; like counters are protected)

    Like-counter keys (blog:likes:*) are intentionally excluded from wildcard flushes —
    those counters are permanent and must be deleted explicitly if ever needed.
    """
    key = key.strip()
    try:
        if "*" in key:
            client  = _get_client(DB_API_CACHE)
            pipe    = client.pipeline()
            evicted = []
            for matched in client.scan_iter(key, count=100):
                if "likes" in matched:      # never nuke like counters via wildcard
                    continue
                pipe.delete(matched)
                evicted.append(matched)
            pipe.execute()
            return {"success": True, "pattern": key, "evicted": evicted, "count": len(evicted)}

        if _blog_response_ttl(key) is None:
            return JSONResponse(
                status_code=400,
                content={"success": False, "message": f"'{key}' is not a recognised blog cache key."},
            )
        deleted = _get_client(DB_API_CACHE).delete(key)
        return {"success": True, "key": key, "deleted": int(deleted)}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


@app.get("/blogger/cache/ttl")
def blogger_cache_ttl_endpoint(key: str = Query(..., min_length=1)):
    """Return the remaining TTL (seconds) for a blog response-cache entry."""
    key = key.strip()
    if _blog_response_ttl(key) is None:
        return JSONResponse(
            status_code=400,
            content={"success": False, "message": f"'{key}' is not a recognised blog cache key."},
        )
    try:
        ttl = _get_client(DB_API_CACHE).ttl(key)
        return {"success": True, "key": key, "ttl_seconds": ttl}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


# ── Sub-system B: Like Counters ───────────────────────────────────────────────

@app.get("/blogger/likes/get")
def blogger_likes_get(post_id: int = Query(..., ge=1)):
    """
    Return the like count for a single post.
    Key  : blog:likes:{post_id}  in DB 5 — persistent INCR counter (no TTL).
    Returns like_count=0 (never 404) when the counter does not exist yet.
    """
    try:
        val = _get_client(DB_API_CACHE).get(f"blog:likes:{post_id}")
        return {"success": True, "post_id": post_id, "like_count": int(val) if val else 0}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


@app.post("/blogger/likes/mget")
def blogger_likes_mget(post_ids: List[int]):
    """
    Bulk-fetch like counts for multiple posts via a single Redis MGET.

    Request body : [1, 2, 3, ...]
    Response     : {"success": true, "likes": {"1": 42, "2": 7, "3": 0}}

    Always returns an entry for every requested post_id (0 if counter absent).
    """
    if not post_ids:
        return {"success": True, "likes": {}}
    try:
        keys   = [f"blog:likes:{pid}" for pid in post_ids]
        values = _get_client(DB_API_CACHE).mget(keys)
        likes  = {str(pid): (int(v) if v else 0) for pid, v in zip(post_ids, values)}
        return {"success": True, "likes": likes}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


@app.post("/blogger/likes/incr")
def blogger_likes_incr(post_id: int = Query(..., ge=1)):
    """
    Atomically increment the like counter for a post and return the new total.

    Key   : blog:likes:{post_id}  (created on first INCR if absent)
    No TTL — the counter is permanent; it survives cache flushes.

    Called once per like action in blogger_service, AFTER the Snowflake INSERT succeeds.
    """
    try:
        new_count = _get_client(DB_API_CACHE).incr(f"blog:likes:{post_id}")
        return {"success": True, "post_id": post_id, "like_count": int(new_count)}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


@app.post("/blogger/likes/seed")
def blogger_likes_seed(payload: BlogLikeSeedRequest):
    """
    Startup seed: bulk upsert like counters sourced from Snowflake.

    Uses max(existing_redis_count, snowflake_count) per post so we can
    repair stale zeros while still preserving any higher live Redis value.

    Request body:
      {"items": [{"post_id": 1, "like_count": 42}, {"post_id": 2, "like_count": 7}]}

        Response:
            {"success": true, "total": 2, "seeded": 1, "updated": 1, "skipped": 0}
                seeded  → counter was absent; initialised
                updated → existing counter was lower than Snowflake count
                skipped → existing counter already >= Snowflake count
    """
    if not payload.items:
        return {"success": True, "total": 0, "seeded": 0, "updated": 0, "skipped": 0}
    try:
        client = _get_client(DB_API_CACHE)
        seeded = 0
        updated = 0
        skipped = 0

        for item in payload.items:
            key = f"blog:likes:{item.post_id}"
            incoming = int(item.like_count or 0)
            raw_existing = client.get(key)

            if raw_existing is None:
                client.set(key, incoming)
                seeded += 1
                continue

            try:
                existing = int(raw_existing)
            except Exception:
                existing = 0

            target = max(existing, incoming)
            if target > existing:
                client.set(key, target)
                updated += 1
            else:
                skipped += 1

        return {
            "success": True,
            "total":   len(payload.items),
            "seeded":  seeded,
            "updated": updated,
            "skipped": skipped,
        }
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


# =============================================================================
# 10. CHATBOT CACHE  (DB 6 — chat: / embedding: namespace)
#     Caller: chatbot_service.py  (zero direct redis imports there)
#
# Three sub-systems
# ─────────────────────────────────────────────────────────────────────────────
#  A) Response Cache      chat:response:{sha256_hash}   30 min
#     Identical user queries (same message + same context fingerprint) are
#     served from cache without calling Ollama, cutting P95 latency ~95%.
#
#  B) Embedding Cache     embedding:{doc_id}            7 days
#     Pre-computed sentence-transformer vectors for KB chunks.
#     Avoids re-encoding the same text on every RAG similarity search.
#
#  C) Conversation Memory chat:history:{conv_id}        24 hr
#                         chat:embeddings:{conv_id}     24 hr
#                         chat:meta:{conv_id}           24 hr
#     Full conversation state — message history, per-message embeddings
#     for semantic search, and lightweight metadata — all expire together.
#
# DB split rationale
# ─────────────────────────────────────────────────────────────────────────────
#  DB 6 (DB_CHATBOT)    — all three chatbot sub-systems above
#  DB 7 (DB_MEETING_INTELLIGENCE_KB) — Knowledge-base data (chunks, index, sync metadata)
#                         These keys have no TTL; they are cleared and
#                         re-written on each S3 sync by chatbot_service.
# =============================================================================


# ─── Sub-system A: Response Cache ────────────────────────────────────────────

@app.post("/chatbot/response/set")
def chatbot_response_set(payload: ChatResponseCacheSet):
    """
    Cache a deduplicated LLM response keyed by content hash.

    Key schema : chat:response:{response_hash}   DB 6   30 min
    Call AFTER Ollama responds. On the next identical query, /chatbot/response/get
    returns the cached text and skips Ollama entirely.
    """
    key = f"chat:response:{payload.response_hash}"
    try:
        _get_client(DB_CHATBOT).setex(key, payload.ttl, payload.response_text)
        return {"success": True, "key": key, "ttl": payload.ttl}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


@app.get("/chatbot/response/get")
def chatbot_response_get(response_hash: str = Query(..., min_length=1)):
    """
    Retrieve a cached LLM response by its content hash.

    Returns found=True + response_text on HIT.
    Returns found=False on MISS — caller must then call Ollama and store the result.
    """
    key = f"chat:response:{response_hash}"
    try:
        raw = _get_client(DB_CHATBOT).get(key)
        if raw is None:
            return {"success": True, "found": False, "response_text": None}
        return {"success": True, "found": True, "key": key, "response_text": raw}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


@app.delete("/chatbot/response/delete")
def chatbot_response_delete(response_hash: str = Query(..., min_length=1)):
    """Evict a single cached LLM response."""
    try:
        deleted = _get_client(DB_CHATBOT).delete(f"chat:response:{response_hash}")
        return {"success": True, "deleted": int(deleted)}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


# ─── Sub-system B: Embedding Cache ───────────────────────────────────────────

@app.post("/chatbot/embedding/set")
def chatbot_embedding_set(payload: DocEmbeddingSet):
    """
    Store a pre-computed embedding vector for a KB document chunk.

    Key schema : embedding:{doc_id}   DB 6   7 days
    The vector is serialised as a JSON array of floats.
    SETNX semantics: if the key already exists, the existing vector is kept.
    Use force=true in the query string to overwrite.
    """
    key = f"embedding:{payload.doc_id}"
    try:
        value = json.dumps(payload.vector)
        _get_client(DB_CHATBOT).setex(key, payload.ttl, value)
        return {"success": True, "key": key, "ttl": payload.ttl, "dims": len(payload.vector)}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


@app.get("/chatbot/embedding/get")
def chatbot_embedding_get(doc_id: str = Query(..., min_length=1)):
    """
    Retrieve a cached embedding vector for a KB chunk.

    Returns found=True + vector (list[float]) on HIT.
    Returns found=False on MISS — caller must re-encode and store.
    """
    key = f"embedding:{doc_id}"
    try:
        raw = _get_client(DB_CHATBOT).get(key)
        if raw is None:
            return {"success": True, "found": False, "vector": None}
        vector = json.loads(raw)
        return {"success": True, "found": True, "key": key, "vector": vector}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


@app.post("/chatbot/embedding/mget")
def chatbot_embedding_mget(doc_ids: List[str]):
    """
    Bulk-fetch embedding vectors for multiple doc_ids via a single Redis MGET.

    Request body : ["sha256abc", "sha256def", ...]
    Response     : {
        "hits":   {"sha256abc": [0.1, 0.2, ...]},
        "misses": ["sha256def"]
    }
    The caller should re-encode and store the misses, then proceed.
    """
    if not doc_ids:
        return {"success": True, "hits": {}, "misses": []}
    try:
        keys   = [f"embedding:{did}" for did in doc_ids]
        values = _get_client(DB_CHATBOT).mget(keys)
        hits:   Dict[str, List[float]] = {}
        misses: List[str] = []
        for did, raw in zip(doc_ids, values):
            if raw is None:
                misses.append(did)
            else:
                try:
                    hits[did] = json.loads(raw)
                except Exception:
                    misses.append(did)
        return {"success": True, "hits": hits, "misses": misses}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


@app.delete("/chatbot/embedding/delete")
def chatbot_embedding_delete(doc_id: str = Query(..., min_length=1)):
    """Evict a single cached embedding (e.g. after re-ingesting that chunk)."""
    try:
        deleted = _get_client(DB_CHATBOT).delete(f"embedding:{doc_id}")
        return {"success": True, "deleted": int(deleted)}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


@app.get("/chatbot/embedding/ttl")
def chatbot_embedding_ttl(doc_id: str = Query(..., min_length=1)):
    try:
        ttl = _get_client(DB_CHATBOT).ttl(f"embedding:{doc_id}")
        return {"success": True, "key": f"embedding:{doc_id}", "ttl_seconds": ttl}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


# ─── Sub-system C: Conversation Memory ───────────────────────────────────────

@app.post("/chatbot/history/append")
def chatbot_history_append(payload: ChatHistoryAppend):
    """
    Append one message turn to a conversation's history list (DB 6).

    Key schema : chat:history:{conversation_id}   TTL reset to 24 hr on every write.
    The history is stored as a JSON array; each append deserialises, appends, and
    re-serialises.  TTL is refreshed on every write so active conversations never expire.

    Returns the new message_count.
    """
    key = f"chat:history:{payload.conversation_id}"
    meta_key = f"chat:meta:{payload.conversation_id}"
    try:
        client = _get_client(DB_CHATBOT)
        raw = client.get(key)
        history: List[Dict] = json.loads(raw) if raw else []

        from datetime import datetime as _dt
        turn = {
            "role":      payload.role,
            "content":   payload.content,
            "user_id":   payload.user_id,
            "timestamp": _dt.utcnow().isoformat() + "Z",
        }
        history.append(turn)
        serialised = json.dumps(history)
        client.setex(key, TTL_CHAT_HISTORY, serialised)

        # Update lightweight metadata
        raw_meta = client.get(meta_key)
        meta: Dict = json.loads(raw_meta) if raw_meta else {}
        meta["user_id"]       = payload.user_id
        meta["message_count"] = len(history)
        meta["updated_at"]    = turn["timestamp"]
        if "created_at" not in meta:
            meta["created_at"] = turn["timestamp"]
        client.setex(meta_key, TTL_CHAT_META, json.dumps(meta))

        return {
            "success":       True,
            "conversation_id": payload.conversation_id,
            "message_count": len(history),
        }
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


@app.get("/chatbot/history/get")
def chatbot_history_get(
    conversation_id: str = Query(..., min_length=1),
    limit:           int  = Query(10, ge=1, le=200),
):
    """
    Retrieve the last `limit` messages from a conversation's history.

    Returns the full list when message_count <= limit.
    Returns found=False (not 404) when the conversation does not exist.
    """
    key = f"chat:history:{conversation_id}"
    try:
        raw = _get_client(DB_CHATBOT).get(key)
        if raw is None:
            return {"success": True, "found": False, "messages": [], "total": 0}
        history: List[Dict] = json.loads(raw)
        window = history[-limit:] if len(history) > limit else history
        return {
            "success":         True,
            "found":           True,
            "conversation_id": conversation_id,
            "messages":        window,
            "returned":        len(window),
            "total":           len(history),
        }
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


@app.post("/chatbot/embedding/append")
def chatbot_embedding_append(payload: ChatEmbeddingAppend):
    """
    Append a per-message embedding to a conversation's embedding array (DB 6).

    Key schema : chat:embeddings:{conversation_id}   TTL 24 hr
    Used by chatbot_service for semantic similarity search over conversation history.
    """
    key = f"chat:embeddings:{payload.conversation_id}"
    try:
        client = _get_client(DB_CHATBOT)
        raw = client.get(key)
        embeddings: List[Dict] = json.loads(raw) if raw else []

        from datetime import datetime as _dt
        embeddings.append({
            "text":      payload.text,
            "vector":    payload.vector,
            "timestamp": _dt.utcnow().isoformat() + "Z",
        })
        client.setex(key, TTL_CHAT_EMBEDDING, json.dumps(embeddings))
        return {
            "success":         True,
            "conversation_id": payload.conversation_id,
            "embedding_count": len(embeddings),
        }
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


@app.get("/chatbot/embedding/history")
def chatbot_embedding_history(conversation_id: str = Query(..., min_length=1)):
    """
    Retrieve all stored per-message embedding vectors for a conversation.
    Used by chatbot_service to perform cosine-similarity search across history.
    """
    key = f"chat:embeddings:{conversation_id}"
    try:
        raw = _get_client(DB_CHATBOT).get(key)
        if raw is None:
            return {"success": True, "found": False, "embeddings": []}
        embeddings: List[Dict] = json.loads(raw)
        return {
            "success":         True,
            "found":           True,
            "conversation_id": conversation_id,
            "embeddings":      embeddings,
            "count":           len(embeddings),
        }
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


@app.get("/chatbot/meta/get")
def chatbot_meta_get(conversation_id: str = Query(..., min_length=1)):
    """Return lightweight metadata for a conversation (message count, timestamps)."""
    key = f"chat:meta:{conversation_id}"
    try:
        raw = _get_client(DB_CHATBOT).get(key)
        if raw is None:
            return {"success": True, "found": False, "meta": None}
        return {"success": True, "found": True, "meta": json.loads(raw)}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


@app.delete("/chatbot/conversation/clear")
def chatbot_conversation_clear(conversation_id: str = Query(..., min_length=1)):
    """
    Delete all cached state for a single conversation:
      chat:history:{id}    chat:embeddings:{id}    chat:meta:{id}
    Does NOT touch the response cache (chat:response:*) or embedding cache (embedding:*).
    """
    try:
        client = _get_client(DB_CHATBOT)
        keys = [
            f"chat:history:{conversation_id}",
            f"chat:embeddings:{conversation_id}",
            f"chat:meta:{conversation_id}",
        ]
        deleted = client.delete(*keys)
        return {
            "success":         True,
            "conversation_id": conversation_id,
            "deleted":         int(deleted),
        }
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


@app.get("/chatbot/stats")
def chatbot_stats():
    """
    Return operational statistics for the chatbot cache DB 6.
    Counts active conversations, cached responses, and cached embeddings.
    """
    try:
        client = _get_client(DB_CHATBOT)
        conv_keys     = list(client.scan_iter("chat:history:*",     count=500))
        response_keys = list(client.scan_iter("chat:response:*",    count=500))
        embed_keys    = list(client.scan_iter("embedding:*",        count=500))
        return {
            "success": True,
            "stats": {
                "active_conversations":  len(conv_keys),
                "cached_llm_responses":  len(response_keys),
                "cached_doc_embeddings": len(embed_keys),
                "db": DB_CHATBOT,
            },
        }
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


# =============================================================================
# 11. MEETING SERVICE CACHE  (DB 8 — meeting: namespace)
#
# This DB is exclusively owned by meeting_service.py.
# No other microservice should write to DB 8.
#
# Four sub-systems
# ─────────────────────────────────────────────────────────────────────────────
#  A) Slot Availability Cache   meeting:slots:{date}          5 min
#     Full slot payload (list of {start, available}) for a calendar date.
#     Evicted immediately after any booking or cancellation for that date.
#
#  B) Slot Locking              meeting:lock:slot:{slot_key}  20 sec
#     Short-TTL SETNX-style lock. Acquired before Razorpay checkout opens;
#     released after DynamoDB write succeeds (or expires on abandoned checkout).
#     slot_key format: "{date}:{start_time}:{duration_minutes}"
#       e.g. "2025-08-01:10:30:60"
#
#  C) Pending Hold              meeting:pending:{auth_identifier}  90 sec
#     Temporary reservation while the user is inside Razorpay checkout.
#     Stores hold metadata (date, start_time, duration, complexity, held_at).
#     Cleared by /meeting-book on success or by /meeting-hold-release on abort.
#
#  D) User Booking Cache        meeting:user:{auth_identifier} 2 min
#     Cached booking list for fast repeated reads in /meeting-mybookings.
#     Evicted on every booking, cancellation, and admin approve/reject for that user.
#
# NOT cached by design:
#   x  payment state  — DB is always authoritative
#   x  pricing model  — lives in direct Redis client (DB 0) inside meeting_service
#   x  admin views    — always scanned from DynamoDB (small cardinality, admin only)
# =============================================================================

# Pydantic models for meeting cache endpoints

class MeetingSlotAvailSet(BaseModel):
    """
    Cache the slot availability payload for a calendar date.
    key  : meeting:slots:{date}   DB 8   5 min
    data : full slots response dict  {"date": "...", "slots": [...]}
    ttl  : override (default TTL_MEETING_SLOT_AVAIL)
    """
    date: str
    data: Any
    ttl: int = TTL_MEETING_SLOT_AVAIL


class MeetingSlotLockAcquire(BaseModel):
    """
    Acquire a short-TTL slot lock.
    slot_key : "{date}:{start_time}:{duration_minutes}"
    holder   : username or booking_id of the requester
    ttl      : lock duration in seconds (default 20)
    """
    slot_key: str
    holder: str
    ttl: int = TTL_MEETING_SLOT_LOCK


class MeetingPendingHoldSet(BaseModel):
    """
    Store a pending hold for a user while they complete payment.
    user_id  : authenticated meeting identifier used in the Redis key
    data     : hold metadata dict
    ttl      : hold duration in seconds (default 90)
    """
    user_id: str
    data: Dict[str, Any]
    ttl: int = TTL_MEETING_PENDING_HOLD


class MeetingUserCacheSet(BaseModel):
    """
    Cache a user's booking list for fast repeated reads.
    user_id  : authenticated meeting identifier used in the Redis key
    bookings : list of booking dicts (JSON-serialisable)
    ttl      : cache duration in seconds (default 120)
    """
    user_id: str
    bookings: List[Any]
    ttl: int = TTL_MEETING_USER_CACHE


# ── Sub-system A: Slot Availability Cache ─────────────────────────────────────

@app.post("/meeting/slots/set")
def meeting_slots_set(payload: MeetingSlotAvailSet):
    """
    Cache slot availability for a date in DB 8.

    Key schema : meeting:slots:{date}   TTL 5 min (default)
    Called by meeting_service after computing availability from DynamoDB.
    Evict via /meeting/slots/delete after any booking or cancellation.
    """
    key = _meeting_slots_key(payload.date)
    try:
        value = json.dumps(payload.data) if not isinstance(payload.data, str) else payload.data
        _get_client(DB_MEETING).setex(key, payload.ttl, value)
        return {"success": True, "key": key, "ttl": payload.ttl}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


@app.get("/meeting/slots/get")
def meeting_slots_get(date: str = Query(..., min_length=1)):
    """
    Retrieve cached slot availability for a date from DB 8.

    Returns found=False on cache MISS — caller falls through to DynamoDB scan.
    """
    key = _meeting_slots_key(date)
    try:
        raw = _get_client(DB_MEETING).get(key)
        if raw is None:
            return {"success": True, "found": False, "data": None}
        try:
            data = json.loads(raw)
        except Exception:
            data = raw
        return {"success": True, "found": True, "key": key, "data": data}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


@app.delete("/meeting/slots/delete")
def meeting_slots_delete(date: str = Query(..., min_length=1)):
    """
    Evict the slot availability cache for a date.
    Call after every booking write or cancellation so the next read is fresh.
    """
    key = _meeting_slots_key(date)
    try:
        deleted = _get_client(DB_MEETING).delete(key)
        return {"success": True, "key": key, "deleted": int(deleted)}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


@app.get("/meeting/slots/ttl")
def meeting_slots_ttl(date: str = Query(..., min_length=1)):
    """Return remaining TTL for the slot availability cache of a date."""
    key = _meeting_slots_key(date)
    try:
        ttl = _get_client(DB_MEETING).ttl(key)
        return {"success": True, "key": key, "ttl_seconds": ttl}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


# ── Sub-system B: Slot Locking ─────────────────────────────────────────────────

@app.post("/meeting/lock/acquire")
def meeting_lock_acquire(payload: MeetingSlotLockAcquire):
    """
    Acquire a short-TTL lock for a specific slot (SETNX semantics).

    Key schema : meeting:lock:slot:{slot_key}   DB 8   TTL 20 sec (default)
    slot_key   : "{date}:{start_time}:{duration_minutes}"

    Returns:
      acquired=True  → lock was free and is now held by `holder`
      acquired=False → lock is already held (see `current_holder`)

    The lock expires automatically if checkout is abandoned.
    Call /meeting/lock/release after the DynamoDB write succeeds (or on error).
    """
    lock_key = _meeting_slot_lock_key(payload.slot_key)
    try:
        client = _get_client(DB_MEETING)
        existing = client.get(lock_key)
        if existing is not None:
            return {
                "success": True,
                "acquired": False,
                "current_holder": existing,
                "key": lock_key,
            }
        # Key is free — set it with short TTL
        client.setex(lock_key, payload.ttl, payload.holder)
        return {
            "success": True,
            "acquired": True,
            "holder": payload.holder,
            "key": lock_key,
            "ttl": payload.ttl,
        }
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


@app.get("/meeting/lock/status")
def meeting_lock_status(slot_key: str = Query(..., min_length=1)):
    """
    Check if a slot is currently locked and who holds it.

    slot_key : "{date}:{start_time}:{duration_minutes}"
    Returns locked=False when the key does not exist (or has expired).
    """
    lock_key = _meeting_slot_lock_key(slot_key)
    try:
        client = _get_client(DB_MEETING)
        holder = client.get(lock_key)
        ttl    = client.ttl(lock_key) if holder else -2
        return {
            "success": True,
            "locked": holder is not None,
            "holder": holder,
            "ttl_seconds": ttl,
            "key": lock_key,
        }
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


@app.delete("/meeting/lock/release")
def meeting_lock_release(slot_key: str = Query(..., min_length=1)):
    """
    Explicitly release a slot lock.
    Call after DynamoDB write succeeds or when booking is abandoned.
    """
    lock_key = _meeting_slot_lock_key(slot_key)
    try:
        deleted = _get_client(DB_MEETING).delete(lock_key)
        return {"success": True, "key": lock_key, "released": int(deleted) > 0}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


# ── Sub-system C: Pending Hold ────────────────────────────────────────────────

@app.post("/meeting/hold/set")
def meeting_hold_set(payload: MeetingPendingHoldSet):
    """
    Store a pending hold for a user while they complete Razorpay checkout.

    Key schema : meeting:pending:{auth_identifier}   DB 8   TTL 90 sec (default)
    Stores hold metadata: date, start_time, duration_minutes, slot_key, held_at.
    Cleared by /meeting/hold/delete after booking confirmation or checkout abort.
    """
    key = _meeting_pending_hold_key(payload.user_id)
    try:
        value = json.dumps(payload.data)
        _get_client(DB_MEETING).setex(key, payload.ttl, value)
        return {"success": True, "key": key, "ttl": payload.ttl, "user_id": payload.user_id}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


@app.get("/meeting/hold/get")
def meeting_hold_get(user_id: str = Query(..., min_length=1)):
    """
    Retrieve the current pending hold for a user.

    Returns found=False when the hold has expired or was not set.
    """
    key = _meeting_pending_hold_key(user_id)
    try:
        raw = _get_client(DB_MEETING).get(key)
        if raw is None:
            return {"success": True, "found": False, "data": None}
        try:
            data = json.loads(raw)
        except Exception:
            data = raw
        ttl = _get_client(DB_MEETING).ttl(key)
        return {"success": True, "found": True, "key": key, "data": data, "ttl_remaining": ttl}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


@app.delete("/meeting/hold/delete")
def meeting_hold_delete(user_id: str = Query(..., min_length=1)):
    """
    Clear the pending hold for a user.
    Call after booking is confirmed (success path) or user cancels checkout.
    """
    key = _meeting_pending_hold_key(user_id)
    try:
        deleted = _get_client(DB_MEETING).delete(key)
        return {"success": True, "key": key, "deleted": int(deleted) > 0}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


@app.get("/meeting/hold/ttl")
def meeting_hold_ttl(user_id: str = Query(..., min_length=1)):
    """Return remaining seconds on a user's pending hold."""
    key = _meeting_pending_hold_key(user_id)
    try:
        ttl = _get_client(DB_MEETING).ttl(key)
        return {"success": True, "key": key, "user_id": user_id, "ttl_seconds": ttl}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


# ── Sub-system D: Per-User Booking Cache ──────────────────────────────────────

@app.post("/meeting/user-cache/set")
def meeting_user_cache_set_endpoint(payload: MeetingUserCacheSet):
    """
    Cache a user's booking list for fast repeated reads.

    Key schema : meeting:user:{auth_identifier}   DB 8   TTL 2 min (default)
    Called by meeting_service after fetching from DynamoDB.
    Evict via /meeting/user-cache/delete after any booking mutation.
    """
    key = _meeting_user_cache_key(payload.user_id)
    try:
        value = json.dumps(payload.bookings)
        _get_client(DB_MEETING).setex(key, payload.ttl, value)
        return {"success": True, "key": key, "ttl": payload.ttl}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


@app.get("/meeting/user-cache/get")
def meeting_user_cache_get_endpoint(user_id: str = Query(..., min_length=1)):
    """
    Retrieve cached booking list for a user from DB 8.

    Returns found=False on cache MISS — caller falls through to DynamoDB.
    """
    key = _meeting_user_cache_key(user_id)
    try:
        raw = _get_client(DB_MEETING).get(key)
        if raw is None:
            return {"success": True, "found": False, "bookings": None}
        try:
            bookings = json.loads(raw)
        except Exception:
            bookings = raw
        return {"success": True, "found": True, "key": key, "bookings": bookings}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


@app.delete("/meeting/user-cache/delete")
def meeting_user_cache_delete_endpoint(user_id: str = Query(..., min_length=1)):
    """
    Evict the booking list cache for a user.
    Call after every booking write, cancellation, or admin approve/reject.
    """
    key = _meeting_user_cache_key(user_id)
    try:
        deleted = _get_client(DB_MEETING).delete(key)
        return {"success": True, "key": key, "deleted": int(deleted) > 0}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


# ── Meeting admin / debug ─────────────────────────────────────────────────────

@app.get("/meeting/debug/keys")
def meeting_debug_keys():
    """
    Return all active meeting: keys in DB 8 (grouped by type).
    Useful for monitoring locks, holds, and cache health.
    """
    try:
        client = _get_client(DB_MEETING)
        all_keys = list(client.scan_iter("meeting:*", count=200))
        slot_avail = [k for k in all_keys if k.startswith("meeting:slots:")]
        locks      = [k for k in all_keys if k.startswith("meeting:lock:")]
        holds      = [k for k in all_keys if k.startswith("meeting:pending:")]
        user_cache = [k for k in all_keys if k.startswith("meeting:user:")]
        return {
            "success": True,
            "db": DB_MEETING,
            "total": len(all_keys),
            "slot_availability_cache": slot_avail,
            "active_locks": locks,
            "pending_holds": holds,
            "user_booking_cache": user_cache,
        }
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})


@app.delete("/meeting/debug/flush")
def meeting_debug_flush(confirm: str = Query(..., description="Must be 'yes' to proceed")):
    """
    ⚠️  DANGER: Flush ALL meeting: keys in DB 8.
    Only for development / emergency recovery.
    Pass ?confirm=yes to execute.
    """
    if confirm.lower() != "yes":
        return JSONResponse(
            status_code=400,
            content={"success": False, "message": "Pass ?confirm=yes to flush the meeting DB."},
        )
    try:
        client = _get_client(DB_MEETING)
        keys = list(client.scan_iter("meeting:*", count=200))
        if keys:
            client.delete(*keys)
        return {"success": True, "flushed": len(keys), "keys": keys}
    except Exception as exc:
        return JSONResponse(status_code=500, content={"success": False, "message": str(exc)})

@app.on_event("startup")
def start_rag_index_updated_consumer():
    t = threading.Thread(
        target=_consume_rag_index_updated,
        daemon=True,
        name="kafka-rag-index-updated",
    )
    t.start()
    print("🚀 Kafka consumer thread started: rag.index.updated")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(_env("REDIS_SERVICE_PORT", "6380")))