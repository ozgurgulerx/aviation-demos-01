#!/usr/bin/env python3
"""Upload using REST API with explicit timeouts."""
import os, sys, json, hashlib, math, re, time, urllib.request, urllib.error
from pathlib import Path

PROJECT = "/Users/ozgurguler/Developer/Projects/aviation-demos-01"
os.chdir(PROJECT)

for line in Path(PROJECT, "src", ".env.local").read_text().splitlines():
    line = line.strip()
    if not line or line.startswith("#") or "=" not in line: continue
    key, _, val = line.partition("=")
    os.environ.setdefault(key.strip(), val.strip().strip('"'))

ENDPOINT = os.environ["AZURE_SEARCH_ENDPOINT"].rstrip("/")
API_KEY = os.environ.get("AZURE_SEARCH_ADMIN_KEY") or os.environ.get("AZURE_SEARCH_KEY")
VECTOR_DIMS = 1536
UNSAFE = re.compile(r"[^A-Za-z0-9_\-=]")
API_VERSION = "2024-07-01"

def hash_embed(text, dims=VECTOR_DIMS):
    vec = [0.0] * dims
    for t in text.lower().split():
        d = hashlib.sha256(t.encode()).digest()
        idx = int.from_bytes(d[:4], "big") % dims
        sign = -1.0 if d[4] % 2 else 1.0
        vec[idx] += sign * (0.5 + d[5] / 255.0)
    norm = math.sqrt(sum(v * v for v in vec)) or 1.0
    return [v / norm for v in vec]

def upload_batch_rest(index_name, batch, timeout=60):
    """Upload batch using REST API with explicit timeout."""
    url = "%s/indexes/%s/docs/index?api-version=%s" % (ENDPOINT, index_name, API_VERSION)
    payload = json.dumps({"value": [{"@search.action": "mergeOrUpload", **doc} for doc in batch]}).encode()
    req = urllib.request.Request(url, data=payload, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("api-key", API_KEY)
    for attempt in range(3):
        try:
            resp = urllib.request.urlopen(req, timeout=timeout)
            data = json.loads(resp.read().decode())
            ok = sum(1 for v in data.get("value", []) if v.get("status"))
            return ok, len(batch) - ok
        except urllib.error.HTTPError as e:
            body = e.read().decode()[:200] if hasattr(e, "read") else ""
            print("  HTTP %d attempt %d: %s" % (e.code, attempt+1, body), flush=True)
            if e.code == 503 or e.code == 429:
                time.sleep(15 * (attempt + 1))
            else:
                time.sleep(5)
        except Exception as e:
            print("  ERROR attempt %d: %s" % (attempt+1, str(e)[:100]), flush=True)
            time.sleep(10 * (attempt + 1))
    print("  FAILED batch after 3 retries", flush=True)
    return 0, len(batch)

def upload_index(index_name, jsonl_file, batch_size=100, start_offset=0):
    path = Path("data/vector_docs") / jsonl_file
    batch, uploaded, skipped, failed, line_no = [], 0, 0, 0, 0
    for raw in path.open("r"):
        raw = raw.strip()
        if not raw: continue
        line_no += 1
        if line_no <= start_offset: continue
        doc = json.loads(raw)
        content = str(doc.get("content", "")).strip()
        if not content:
            skipped += 1
            continue
        doc["id"] = UNSAFE.sub("_", str(doc["id"]).strip())[:1024]
        doc["content"] = content
        doc["title"] = str(doc.get("title", "")).strip()
        doc["source"] = str(doc.get("source", "ASRS")).strip()
        doc["content_vector"] = hash_embed(content)
        batch.append(doc)
        if len(batch) >= batch_size:
            ok, nok = upload_batch_rest(index_name, batch)
            uploaded += ok
            failed += nok
            if uploaded % 500 < batch_size:
                print("  %d uploaded, %d failed, %d skipped (line %d)" % (uploaded, failed, skipped, line_no), flush=True)
            batch = []
    if batch:
        ok, nok = upload_batch_rest(index_name, batch)
        uploaded += ok
        failed += len(batch) - ok
    print("Done: %d uploaded, %d failed, %d skipped" % (uploaded, failed, skipped), flush=True)

UPLOADS = [
    ("idx_ops_narratives", "ops_narratives_docs.jsonl", 100, 198239),
    ("idx_airport_ops_docs", "airport_ops_docs.jsonl", 100, 0),
]

for name, f, bs, off in UPLOADS:
    print(">>> %s (offset=%d, batch=%d)" % (name, off, bs), flush=True)
    upload_index(name, f, bs, off)

print("All done!", flush=True)
