import os, json, logging, re
import azure.functions as func

from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.ai.documentintelligence.models import AnalyzeDocumentRequest, DocumentField
from azure.core.credentials import AzureKeyCredential
from azure.storage.blob import BlobServiceClient

# --- NEW: SQL via python-tds (pur Python) ---
import datetime as dt
import tds  # pip install python-tds

# --- Function App (Python v2)
app = func.FunctionApp()

# --- Config (depuis App Settings Azure / local.settings.json)
ENDPOINT = (os.getenv("AZURE_DI_ENDPOINT") or "").strip().rstrip("/")
KEY      = (os.getenv("AZURE_DI_KEY") or "").strip()
MODEL_ID = (os.getenv("AZURE_DI_MODEL_ID") or "").strip()

ARCHIVE_CONTAINER = os.getenv("ARCHIVE_CONTAINER", "archive")
STORAGE_CONN_STR  = os.getenv("AzureWebJobsStorage")

# SQL (format simple conseillé: Server=...;Database=...;User Id=...;Password=...;Encrypt=yes)
SQL_CONN_STR      = os.getenv("SQL_CONN_STR", "")
DEFAULT_STATUS_ID = int(os.getenv("DEFAULT_STATUS_ID", "1"))
DEFAULT_SITE_ID   = int(os.getenv("DEFAULT_SITE_ID", "1"))
CREATED_BY        = os.getenv("CREATED_BY", "function-app")

# --- Clients
di_client = DocumentIntelligenceClient(endpoint=ENDPOINT, credential=AzureKeyCredential(KEY))
blob_service = BlobServiceClient.from_connection_string(STORAGE_CONN_STR)
archive_container = blob_service.get_container_client(ARCHIVE_CONTAINER)

def extract_value(field: DocumentField):
    ftype = getattr(field, "type", None) or getattr(field, "value_type", None)
    if ftype in ("string","countryRegion","phoneNumber"): return field.value_string
    if ftype == "date": return str(field.value_date)
    if ftype == "time": return str(field.value_time)
    if ftype == "integer": return field.value_integer
    if ftype in ("number","float"): return field.value_number
    if ftype == "currency":
        cur = field.value_currency
        if cur:
            return {"amount": getattr(cur,"amount",None),
                    "currency": getattr(cur,"currency_code",None) or getattr(cur,"currency_symbol",None)}
        return None
    if ftype == "array": return [extract_value(x) for x in (field.value_array or [])]
    if ftype in ("object","dictionary","map"):
        obj = field.value_object or {}
        return {k: extract_value(v) for k,v in obj.items()}
    return field.content

def save_json_to_archive(source_blob_name: str, data: dict):
    base = os.path.basename(source_blob_name)
    stem = os.path.splitext(base)[0]
    out_name = f"{stem}.json"
    payload = json.dumps(data, ensure_ascii=False, indent=2)
    archive_container.upload_blob(name=out_name, data=payload, overwrite=True)
    logging.info(f"🗄️ JSON archivé dans {ARCHIVE_CONTAINER}/{out_name}")

# ---------- SQL (python-tds) ----------
_CONN_RE = re.compile(
    r"Server=tcp:([^,]+),(\d+);Database=([^;]+);User Id=([^;]+);Password=([^;]+);", re.I
)

def _connect_sql_from_connstr(conn_str: str):
    m = _CONN_RE.search(conn_str or "")
    if not m:
        raise ValueError("SQL_CONN_STR invalide. Attendu: "
                         "Server=tcp:<srv>,1433;Database=<db>;User Id=<user>;Password=<pwd>;Encrypt=yes")
    server, port, database, user, pwd = m.group(1), int(m.group(2)), m.group(3), m.group(4), m.group(5)
    return tds.connect(
        server=server, port=port, user=user, password=pwd,
        database=database, encrypt=True, autocommit=False
    )

def save_to_db(fields: dict, blob_name: str, account_url: str, container: str):
    # Champs issus du modèle
    num   = (fields.get("NumeroFacture") or {}).get("value")
    issue = (fields.get("DateEmission")  or {}).get("value")
    due   = (fields.get("DateEcheance")  or {}).get("value")
    amt   = (fields.get("MontantTotal")  or {}).get("value")

    # Normalisation date / montant
    def to_date(s):
        if not s: return None
        for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%d/%m/%Y"):
            try: return dt.datetime.strptime(str(s), fmt)
            except: pass
        return None

    def to_amount(s):
        if s is None: return 0.0
        return float(str(s).replace(" ", "").replace("\u00a0","").replace(".", "").replace(",", "."))

    date_issue = to_date(issue)
    date_due   = to_date(due)
    amount     = to_amount(amt)
    now        = dt.datetime.utcnow()

    original = os.path.basename(blob_name)
    file_url = f"{account_url}/{container}/{original}"

    with _connect_sql_from_connstr(SQL_CONN_STR) as conn:
        cur = conn.cursor()

        # files.AppFile
        cur.execute("""
            INSERT INTO files.AppFile
            (SourceName, SourceId, ContainerName, OriginalFileName, SystemFileName, DateCreation, FileUrl)
            OUTPUT INSERTED.Id
            VALUES (?,?,?,?,?,?,?)
        """, ("blobTrigger", blob_name, container, original, original, now, file_url))
        row = cur.fetchone()
        file_id = row[0] if row else None

        # invoices.Invoice
        cur.execute("""
            INSERT INTO invoices.Invoice
            (Number, SiteId, RefInvoiceStatusId, IsArchived, DateIssue, DateDue, Amount, FileId, DateCreated, CreatedBy)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (num or "", DEFAULT_SITE_ID, DEFAULT_STATUS_ID, 0, date_issue, date_due, amount, file_id, now, CREATED_BY))

        conn.commit()

    logging.info(f"🗃️ SQL OK — Invoice.Number={num}  Amount={amount}")

# ----- BLOB TRIGGER : écoute 'eem-training/{name}'
@app.function_name(name="ProcessInvoice")
@app.blob_trigger(arg_name="myblob", path="eem-training/{name}", connection="AzureWebJobsStorage")
def ProcessInvoice(myblob: func.InputStream):
    try:
        logging.info(f"🔔 Nouveau blob: {myblob.name} ({myblob.length} bytes)")

        req = AnalyzeDocumentRequest(bytes_source=myblob.read())
        poller = di_client.begin_analyze_document(model_id=MODEL_ID, body=req)
        result = poller.result()

        if not result.documents:
            logging.warning("⚠️ Aucun document détecté")
            return

        doc = result.documents[0]
        data = { name: {"value": extract_value(field), "confidence": field.confidence}
                 for name, field in (doc.fields or {}).items() }

        logging.info("✅ Champs extraits: " + json.dumps(data, ensure_ascii=False))

        # 1) Archive JSON d’audit
        save_json_to_archive(myblob.name, data)

        # 2) Insert en base (sql)
        account_url = blob_service.url
        save_to_db(data, myblob.name, account_url, container="eem-training")

    except Exception as e:
        logging.exception(f"Erreur traitement blob: {e}")
