"""
import_gare.py v2 — Gare Intelligence
- Scarica ZIP ANAC via Cloudflare Worker (bypass blocco IP)
- Elabora CSV localmente con Python
- Scarica TED via Worker
- Inserisce in Supabase
"""

import os, io, csv, json, zipfile, requests
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://efhdooeqscqncgvhqfyu.supabase.co")
SERVICE_KEY  = os.environ.get("SUPABASE_SERVICE_KEY", "")
WORKER_URL   = os.environ.get("WORKER_URL", "https://gare-relay.finellimanuel.workers.dev")

HEADERS_SB = {
    "apikey":        SERVICE_KEY,
    "Authorization": f"Bearer {SERVICE_KEY}",
    "Content-Type":  "application/json",
    "Prefer":        "resolution=ignore-duplicates,return=minimal",
}

CPV_TARGET = ["7999","5070","9091","7131","4521","5153","7200","4500","3913","7132","5050","9000"]
KEYWORDS   = [
    "facility management","facility",
    "manutenzione ordinaria","manutenzione straordinaria","manutenzione",
    "pulizia","sanificazione","disinfezione","igienizzazione",
    "impianti termici","impianti elettrici","impianti hvac","hvac",
    "gestione edifici","gestione immobili","gestione impianti",
    "ristrutturazione","riqualificazione",
    "climatizzazione","condizionamento aria",
    "antincendio","energy service","efficienza energetica",
    "verde pubblico","sfalcio","vigilanza","portierato","ascensori","elevatori",
]
IMPORTO_MIN = 40_000
BATCH_SIZE  = 100

def cpv_ok(cpv): return any((cpv or "")[:4].startswith(p) for p in CPV_TARGET)
def kw_ok(txt):  return any(k in (txt or "").lower() for k in KEYWORDS)

def to_ts(d):
    if not d: return None
    d = str(d).strip()
    if "T" in d: return d if ("+" in d or d.endswith("Z")) else d + "+00:00"
    return d + "T00:00:00+00:00"

def riga_to_gara(r):
    try:    importo = float(r.get("importo_complessivo_gara") or 0)
    except: importo = 0
    if importo < IMPORTO_MIN: return None
    if (r.get("stato") or "").upper() in ("ANNULLATO","CANCELLATO"): return None
    if not cpv_ok(r.get("cod_cpv")) and not kw_ok(r.get("oggetto_gara")): return None
    regione = (r.get("sezione_regionale") or "").replace("SEZIONE REGIONALE ","").strip() or None
    return {
        "codice_cig":   r.get("cig") or None,
        "titolo":       r.get("oggetto_gara") or "(oggetto non disponibile)",
        "descrizione":  None, "riassunto_ai": None, "keywords_ai": [], "settore_ai": None,
        "ente":         r.get("denominazione_amministrazione_appaltante") or None,
        "regione":      regione,
        "provincia":    r.get("provincia") or None, "comune": None,
        "categoria_cpv":   r.get("cod_cpv") or None,
        "categoria_label": r.get("descrizione_cpv") or None,
        "procedura":    r.get("tipo_scelta_contraente") or None,
        "criterio_aggiudicazione": None,
        "importo_min": None, "importo_max": None,
        "importo_totale": importo if importo > 0 else None,
        "scadenza":           to_ts(r.get("data_scadenza_offerta")),
        "data_pubblicazione": to_ts(r.get("data_pubblicazione")),
        "stato":    "PUBBLICATA", "fonte": "ANAC",
        "url_bando":   f"https://api.anticorruzione.it/apicig/1.0.0/getSmartCig/{r.get('cig')}",
        "url_portale": "https://dati.anticorruzione.it",
        "id_sintel": None, "codice_gara": r.get("numero_gara") or None, "rup": None,
    }

def insert_batch(gare):
    inserite = 0
    for i in range(0, len(gare), BATCH_SIZE):
        batch = gare[i:i+BATCH_SIZE]
        r = requests.post(f"{SUPABASE_URL}/rest/v1/gare",
            headers=HEADERS_SB, json=batch, timeout=30)
        if r.status_code in (200, 201): inserite += len(batch)
        else: print(f"  ⚠️  Batch {r.status_code}: {r.text[:150]}")
    return inserite

def import_anac():
    print("🇮🇹 ANAC — scarico via Cloudflare Worker")
    url = f"{WORKER_URL}/anac"
    print(f"  Worker: {url}")
    r = requests.get(url, timeout=180, stream=True)  # 3 minuti timeout
    if r.status_code != 200:
        print(f"  ❌ Worker HTTP {r.status_code}: {r.text[:200]}")
        return {"fonte":"ANAC","inserite":0,"errore":f"Worker HTTP {r.status_code}"}

    anac_url = r.headers.get("X-Anac-Url", "n/d")
    zip_bytes = r.content
    print(f"  ✅ ZIP scaricato: {len(zip_bytes)/1e6:.1f} MB ({anac_url})")

    gare = []
    righe = 0
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        csv_name = next((n for n in zf.namelist() if n.endswith(".csv")), None)
        if not csv_name:
            return {"fonte":"ANAC","inserite":0,"errore":"CSV non trovato nel ZIP"}
        with zf.open(csv_name) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding="iso-8859-1"))
            for row in reader:
                righe += 1
                g = riga_to_gara(row)
                if g: gare.append(g)

    print(f"  📊 {righe} righe CSV → {len(gare)} gare filtrate")
    inserite = insert_batch(gare)
    print(f"  ✅ {inserite} gare inserite in Supabase")
    return {"fonte":"ANAC","url":anac_url,"righe_csv":righe,"filtrate":len(gare),"inserite":inserite}

def import_ted():
    print("🇪🇺 TED — scarico via Cloudflare Worker")
    url = f"{WORKER_URL}/ted"
    try:
        r = requests.post(url, json={}, timeout=60)
        data    = r.json()
        notices = data.get("notices", data.get("results", []))
    except Exception as e:
        print(f"  ❌ Errore: {e}")
        return {"fonte":"TED_EU","inserite":0,"errore":str(e)}

    print(f"  📡 {len(notices)} notices ricevute da TED")
    oggi = datetime.now().strftime("%Y-%m-%dT00:00:00+00:00")
    gare = []
    for n in notices:
        cpv    = n.get("cpv-code","")
        titolo = n.get("title","")
        if not cpv_ok(cpv) and not kw_ok(titolo): continue
        importo = (n.get("estimated-value") or {}).get("amount", 0)
        pub_num = (n.get("publication-number") or "").strip()
        pub_url = pub_num.replace("/","-").replace(" ","-")
        scad    = n.get("deadline-receipt-request") or ""
        if scad and "+" not in scad and not scad.endswith("Z"): scad += "+00:00"
        gare.append({
            "codice_cig": None, "titolo": titolo or "(titolo non disponibile)",
            "descrizione": None, "riassunto_ai": None, "keywords_ai": [], "settore_ai": None,
            "ente": n.get("contracting-authority-name") or None,
            "regione": "ITALIA", "provincia": n.get("place-of-performance") or None, "comune": None,
            "categoria_cpv": cpv or None, "categoria_label": None,
            "procedura": "Procedura aperta (EU)", "criterio_aggiudicazione": None,
            "importo_min": None, "importo_max": None,
            "importo_totale": float(importo) if importo else None,
            "scadenza": scad or None, "data_pubblicazione": oggi,
            "stato": "PUBBLICATA", "fonte": "TED_EU",
            "url_bando": f"https://ted.europa.eu/en/notice/-/detail/{pub_url}",
            "url_portale": "https://ted.europa.eu",
            "id_sintel": None, "codice_gara": pub_num or None, "rup": None,
        })

    print(f"  📊 {len(gare)} gare filtrate")
    inserite = insert_batch(gare)
    print(f"  ✅ {inserite} gare inserite in Supabase")
    return {"fonte":"TED_EU","notices":len(notices),"filtrate":len(gare),"inserite":inserite}

if __name__ == "__main__":
    print(f"🚀 Gare Intelligence Import — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    risultati = [import_anac(), import_ted()]
    tot = sum(r.get("inserite",0) for r in risultati)
    print(f"\n✅ TOTALE: {tot} gare inserite")
    print(json.dumps(risultati, indent=2, ensure_ascii=False))
