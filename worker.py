#!/usr/bin/env python3
# Edge-proxied IIIF fetch -> WebP -> object store -> index. Idempotent (skip on .done marker).
# Single item:  worker.py --id <id> --manifest <url> --title "<t>"
# Sharded run:  worker.py --shard N --total M   (reads worklist.csv from private bucket)
import os, sys, io, re, csv, json, time, argparse, urllib.parse, requests, boto3

PROXY = os.environ["EDGE_PROXY"]                      # e.g. https://x.example.dev/fetch?url=
EP, AK, SK = os.environ["R2_ENDPOINT"], os.environ["R2_KEY"], os.environ["R2_SECRET"]
ACC, DB, TOK = os.environ["CF_ACCOUNT"], os.environ["CF_D1_DB"], os.environ["CF_D1_TOKEN"]
DST = os.environ.get("DST_BUCKET", "guyaofang-lib")
LIB = os.environ.get("LIB_NAME", "")
LIST_BUCKET = os.environ.get("LIST_BUCKET", "")
LIST_KEY = os.environ.get("LIST_KEY", "worklist.csv")
Q = int(os.environ.get("WEBP_Q", "85"))

s3 = boto3.client("s3", endpoint_url=EP, aws_access_key_id=AK, aws_secret_access_key=SK, region_name="auto")

# transcode: prefer libvips (fast), fall back to Pillow
try:
    import pyvips
    def to_webp(buf): return pyvips.Image.new_from_buffer(buf, "").webpsave_buffer(Q=Q)
    ENGINE = "vips"
except Exception:
    from PIL import Image
    def to_webp(buf):
        out = io.BytesIO(); Image.open(io.BytesIO(buf)).convert("RGB").save(out, "WEBP", quality=Q); return out.getvalue()
    ENGINE = "pillow"

def gfetch(url, timeout=90):
    r = requests.get(PROXY + urllib.parse.quote(url, safe=""), timeout=timeout); r.raise_for_status(); return r

def d1(sql, params=None):
    body = {"sql": sql} | ({"params": params} if params else {})
    return requests.post(f"https://api.cloudflare.com/client/v4/accounts/{ACC}/d1/database/{DB}/query",
                         headers={"Authorization": f"Bearer {TOK}"}, json=body, timeout=30)

def page_urls(m):
    seqs = m.get("sequences") or []
    if seqs and seqs[0].get("canvases"):                 # IIIF v2
        urls = []
        for c in seqs[0]["canvases"]:
            res = (c.get("images") or [{}])[0].get("resource", {})
            svc = res.get("service") or {}
            sid = svc.get("@id") if isinstance(svc, dict) else None
            urls.append(f"{sid}/full/max/0/default.jpg" if sid else res.get("@id"))
        return urls
    urls = []                                            # IIIF v3
    for c in m.get("items", []):
        for ap in c.get("items", []):
            for an in ap.get("items", []):
                b = an.get("body", {}); svc = (b.get("service") or [{}])
                sid = svc[0].get("id") if svc and isinstance(svc[0], dict) else None
                urls.append(f"{sid}/full/max/0/default.jpg" if sid else b.get("id"))
    return urls

def extract_meta(m, req):
    # capture ALL descriptive metadata in the same pass (no later backfill)
    def norm(x):
        if isinstance(x, list): x = x[0] if x else ""
        if isinstance(x, dict):
            v = x.get("@value") or x.get("value")
            if v is None:
                for vv in x.values():
                    if isinstance(vv, list) and vv: return str(vv[0])
                return ""
            return str(v)
        return str(x or "")
    fields = {}
    for it in (m.get("metadata") or []):
        lab, val = norm(it.get("label")), norm(it.get("value"))
        if lab: fields[lab] = val
    author = dynasty = ""
    for lab, val in fields.items():
        if "Creator" in lab or "Author" in lab:
            author = val.strip()
            break
    return {"req": req, "source_url": m.get("@id") or m.get("id") or "",
            "author": author, "dynasty": dynasty, "fields": fields}

def process(book):
    bid = book["book_id"]; prefix = f"book/{bid}/"; marker = f"{prefix}.done"
    try:
        s3.head_object(Bucket=DST, Key=marker); return "skip"            # idempotent
    except Exception:
        pass
    m = gfetch(book["manifest"]).json()
    urls = [u for u in page_urls(m) if u]
    n = len(urls)
    if n == 0:
        return "fail:no_pages"
    meta = extract_meta(m, book.get("req", ""))
    for i, u in enumerate(urls, 1):                                       # all-or-nothing: any failure aborts (no partial register)
        s3.put_object(Bucket=DST, Key=f"{prefix}page_{i:04d}.webp",
                      Body=to_webp(gfetch(u).content), ContentType="image/webp")
    sql = ("INSERT OR REPLACE INTO books_assets_v2 (book_id, book_title, source_root, source_relative_path, "
           "webp_prefix, page_count, upload_status, webp_status, frontend_visible, rights_status, collection, "
           "req_no, part, category_code, library, author, dynasty, meta_json, created_at, updated_at) "
           "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,strftime('%s','now'),strftime('%s','now'))")
    p = [bid, book.get("title", bid), "lineb", f"lineb/{bid}", prefix, n, "done", "done", 1,
         "public_domain", "overseas", book.get("req", ""), book.get("part", ""), book.get("cat", ""), book.get("lib", LIB),
         meta.get("author", ""), meta.get("dynasty", ""), json.dumps(meta, ensure_ascii=False)]
    r = d1(sql, p)
    if not (r.status_code == 200 and r.json().get("success")):
        return f"fail:d1 {r.text[:120]}"
    s3.put_object(Bucket=DST, Key=marker, Body=b"1")
    return f"ok:{n}p"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--id"); ap.add_argument("--manifest"); ap.add_argument("--title")
    ap.add_argument("--shard", type=int, default=0); ap.add_argument("--total", type=int, default=1)
    a = ap.parse_args()
    print(f"engine={ENGINE}", flush=True)
    if a.id and a.manifest:
        print(process({"book_id": a.id, "manifest": a.manifest, "title": a.title or a.id}), flush=True); return
    raw = s3.get_object(Bucket=LIST_BUCKET, Key=LIST_KEY)["Body"].read().decode("utf-8-sig")
    rows = list(csv.DictReader(io.StringIO(raw)))[a.shard::a.total]
    print(f"shard {a.shard}/{a.total}: {len(rows)} items", flush=True)
    ok = 0
    for b in rows:
        try:
            r = process(b); ok += r.startswith("ok") or r == "skip"; print(f"  {b['book_id']}: {r}", flush=True)
        except Exception as e:
            print(f"  {b.get('book_id','?')}: err {e}", flush=True)
    print(f"done shard {a.shard}: {ok}/{len(rows)}", flush=True)

if __name__ == "__main__":
    main()
