"""
test_anac_lista.py — Trova endpoint lista bandi ANAC pubblicitalegale

Confermato: https://pubblicitalegale.anticorruzione.it/bandi/{uuid}/json
restituisce JSON completo con CIG, importo, scadenza, ente.

Obiettivo: trovare l'endpoint che restituisce la lista degli UUID
per una data — così scarichiamo tutti i bandi del giorno.

NON inserisce dati.
"""

import requests, json, re
from datetime import datetime, date, timedelta

TIMEOUT = 20
BASE = "https://pubblicitalegale.anticorruzione.it"

HEADERS_BROWSER = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "it-IT,it;q=0.9",
}
HEADERS_JSON = {**HEADERS_BROWSER, "Accept": "application/json"}

def get(url, json_mode=False):
    h = HEADERS_JSON if json_mode else HEADERS_BROWSER
    try:
        r = requests.get(url, headers=h, timeout=TIMEOUT)
        return r
    except Exception as e:
        print(f"   ❌ {e}")
        return None

def post(url, payload):
    try:
        r = requests.post(url, headers=HEADERS_JSON, json=payload, timeout=TIMEOUT)
        return r
    except Exception as e:
        print(f"   ❌ {e}")
        return None

def sep(t): print(f"\n{'='*60}\n🔍 {t}")

oggi       = date.today().isoformat()             # 2026-04-26
ieri       = (date.today()-timedelta(days=1)).isoformat()  # 2026-04-25
due_giorni = (date.today()-timedelta(days=2)).isoformat()
uuid_test  = "d6a94bed-77ce-4fdd-8ee7-a4da6d210753"  # UUID confermato

print(f"🚀 Test lista bandi ANAC — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
print(f"   UUID di test: {uuid_test}\n")

# ── 1. Conferma che il dettaglio JSON funziona ─────────────────────────────────
sep("1. Conferma endpoint dettaglio JSON")
r = get(f"{BASE}/bandi/{uuid_test}/json", json_mode=True)
if r:
    print(f"   HTTP: {r.status_code}, Size: {len(r.content)/1024:.1f} KB")
    if r.status_code == 200:
        try:
            d = r.json()
            print(f"   ✅ JSON confermato! Sezioni: {[s.get('name') for s in d]}")
        except:
            print(f"   Raw: {r.text[:200]}")

# ── 2. Prova URI endpoint ──────────────────────────────────────────────────────
sep("2. Endpoint URI (pulsante accanto a JSON)")
for url in [
    f"{BASE}/bandi/{uuid_test}/uri",
    f"{BASE}/bandi/{uuid_test}",
    f"{BASE}/bandi/{uuid_test}/detail",
]:
    r = get(url, json_mode=True)
    if r:
        print(f"\n   {url} → HTTP {r.status_code}, CT: {r.headers.get('Content-Type','?')[:50]}")
        if r.status_code == 200 and "json" in r.headers.get("Content-Type",""):
            print(f"   ✅ {r.text[:300]}")

# ── 3. Endpoint lista bandi per data ──────────────────────────────────────────
sep("3. Lista bandi per data (endpoint principale)")
for url in [
    # Pattern REST classico
    f"{BASE}/api/bandi?data={ieri}",
    f"{BASE}/api/bandi?dataPubblicazione={ieri}",
    f"{BASE}/api/bandi?date={ieri}",
    f"{BASE}/api/publications?date={ieri}",
    # Pattern Angular con /api/v1
    f"{BASE}/api/v1/bandi?data={ieri}",
    f"{BASE}/api/v1/publications?date={ieri}",
    # Pattern con /bandi come lista
    f"{BASE}/bandi?data={ieri}",
    f"{BASE}/bandi?dataPubblicazione={ieri}",
    # Pattern OpenData
    f"{BASE}/opendata/bandi?data={ieri}",
    f"{BASE}/opendata/publications?date={ieri}",
]:
    r = get(url, json_mode=True)
    if r and r.status_code not in (404,):
        ct = r.headers.get("Content-Type","")
        print(f"\n   {url}")
        print(f"   HTTP: {r.status_code}, CT: {ct[:50]}, Size: {len(r.content)/1024:.1f} KB")
        if r.status_code == 200 and "json" in ct:
            try:
                d = r.json()
                print(f"   ✅ JSON! Keys/tipo: {list(d.keys()) if isinstance(d,dict) else f'array {len(d)}'}")
                print(f"   Contenuto: {json.dumps(d, ensure_ascii=False)[:400]}")
            except:
                print(f"   Raw: {r.text[:200]}")

# ── 4. Prova ricerca POST ──────────────────────────────────────────────────────
sep("4. Ricerca POST con data")
for url in [
    f"{BASE}/api/bandi/search",
    f"{BASE}/api/bandi/ricerca",
    f"{BASE}/api/publications/search",
    f"{BASE}/api/v1/bandi/search",
]:
    for payload in [
        {"dataPubblicazione": ieri, "page": 0, "size": 5},
        {"data": ieri, "page": 0, "size": 5},
        {"from": ieri, "to": oggi, "size": 5},
        {"dataInizio": ieri, "dataFine": oggi},
    ]:
        r = post(url, payload)
        if r and r.status_code not in (404, 405):
            ct = r.headers.get("Content-Type","")
            print(f"\n   POST {url}")
            print(f"   Payload: {payload}")
            print(f"   HTTP: {r.status_code}, CT: {ct[:50]}")
            if r.status_code == 200 and "json" in ct:
                try:
                    d = r.json()
                    print(f"   ✅ JSON! {json.dumps(d, ensure_ascii=False)[:400]}")
                except:
                    print(f"   Raw: {r.text[:200]}")
            break  # prova solo il primo payload che non dà 404

# ── 5. Intercetta chiamate Angular dal sorgente JS ────────────────────────────
sep("5. Analisi sorgente JS (cerca URL API hardcoded)")
r = get(BASE)
if r and r.status_code == 200:
    # Cerca tutti i file JS caricati
    js_files = re.findall(r'src=["\']([^"\']+\.js)["\']', r.text)
    print(f"   File JS trovati: {len(js_files)}")
    print(f"   Files: {js_files[:5]}")
    # Cerca URL API nell'HTML
    api_urls = re.findall(r'["\']\/(?:api|rest|public)[^\s"\'<>]{3,60}["\']', r.text)
    print(f"   URL API nell'HTML: {api_urls[:10]}")

# Carica il main JS e cerca gli endpoint
sep("6. Carica main.js e cerca endpoint API")
r = get(BASE)
if r and r.status_code == 200:
    js_files = re.findall(r'src=["\']([^"\']+\.js)["\']', r.text)
    for js_file in js_files[:3]:
        url = js_file if js_file.startswith("http") else BASE + "/" + js_file.lstrip("/")
        rj = get(url)
        if rj and rj.status_code == 200:
            js = rj.text
            # Cerca pattern API URL
            api_patterns = re.findall(r'["\`]\/(?:api|rest|bandi|public)[^"\'`\s]{5,80}["\`]', js)
            env_urls = re.findall(r'(?:apiUrl|baseUrl|endpoint|API_URL)["\s:=]+["\`]([^"\'`]{10,80})["\`]', js, re.IGNORECASE)
            bandi_urls = re.findall(r'bandi[^"\'`\s]{0,60}', js)[:20]
            if api_patterns or env_urls:
                print(f"\n   {url[:80]}")
                print(f"   API patterns: {api_patterns[:10]}")
                print(f"   Env URLs: {env_urls[:5]}")
                print(f"   Bandi references: {list(set(bandi_urls))[:10]}")

print(f"\n{'='*60}")
print("✅ Test completato")
