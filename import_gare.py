"""
import_gare.py — Gare Intelligence FINAL
Fix: parsing importo formato italiano (1.250.000,00)
"""

import os, io, csv, json, zipfile, requests
from datetime import datetime

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

def cpv_ok(cpv):  return any((cpv or "")[:4].startswith(p) for p in CPV_TARGET)
def kw_ok(txt):   return any(k in (txt or "").lower() for k in KEYWORDS)

def to_ts(d):
    if not d: return None
    d = str(d).strip()
    if "T" in d: return d if ("+" in d or d.endswith("Z")) else d + "+00:00"
    return d + "T00:00:00+00:00"

def parse_importo(val):
    """Gestisce sia formato italiano (1.250.000,00) che internazionale (1250000.00)"""
    if not val: return 0
    val = val.strip()
    # Formato italiano: punto = migliaia, virgola = decimale
    if "," in val and "." in val:
        val = val.replace(".", "").replace(",", ".")
    elif "," in val and "." not in val:
        val = val.replace(",", ".")
    # Rimuove eventuali spazi e caratteri non numerici residui
    val = val.replace(" ", "")
    try:    return float(val)
    except: return 0

def riga_to_gara(r):
    # Prova importo_complessivo_gara, poi importo_lotto come fallback
    importo = parse_importo(r.get("importo_complessivo_gara"))
    if importo == 0:
        importo = parse_importo(r.get("importo_lotto"))
    if importo < IMPORTO_MIN: return None

    stato = (r.get("stato") or "").upper()
    if stato in ("ANNULLATO", "CANCELLATO"): return None

    cpv    = r.get("cod_cpv") or ""
    oggetto = r.get("oggetto_gara") or ""
    if not cpv_ok(cpv) and not kw_ok(oggetto): return None

    regione = (r.get("sezione_regionale") or "").replace("SEZIONE REGIONALE ", "").strip() or None
    cig     = r.get("cig") or None

    return {
        "codice_cig":   cig,
        "titolo":       oggetto or r.get("oggetto_lotto") or "(oggetto non disponibile)",
        "descrizione":  None, "riassunto_ai": None, "keywords_ai": [], "settore_ai": None,
        "ente":         r.get("denominazione_amministrazione_appaltante") or None,
        "regione":      regione,
        "provincia":    r.get("provincia") or None, "comune": None,
        "categoria_cpv":   cpv or None,
        "categoria_label": r.get("descrizione_cpv") or None,
        "procedura":    r.get("tipo_scelta_contraente") or None,
        "criterio_aggiudicazione": None,
        "importo_min":  None, "importo_max": None,
        "importo_totale": importo if importo > 0 else None,
        "scadenza":           to_ts(r.get("data_scadenza_offerta")),
        "data_pubblicazione": to_ts(r.get("data_pubblicazione")),
        "stato":    "PUBBLICATA", "fonte": "ANAC",
        "url_bando":   f"https://api.anticorruzione.it/apicig/1.0.0/getSmartCig/{cig}",
        "url_portale": "https://dati.anticorruzione.it",
        "id_sintel":   None,
        "codice_gara": r.get("numero_gara") or None,
        "rup":         None,
    }

def insert_batch(gare):
    inserite = 0
    for i in range(0, len(gare), BATCH_SIZE):
        batch = gare[i:i+BATCH_SIZE]
        r = requests.post(f"{SUPABASE_URL}/rest/v1/gare",
            headers=HEADERS_SB, json=batch, timeout=30)
        if r.status_code in (200, 201): inserite += len(batch)
        else: print(f"  ⚠️  Batch {r.status_code}: {r.text[:200]}")
    return inserite

def import_anac():
    print("🇮🇹 ANAC — scarico via Cloudflare Worker")
    r = requests.get(f"{WORKER_URL}/anac", timeout=180, stream=True)
    if r.status_code != 200:
        return {"fonte":"ANAC","inserite":0,"errore":f"HTTP {r.status_code}"}

    anac_url  = r.headers.get("X-Anac-Url", "n/d")
    zip_bytes = r.content
    print(f"  ✅ ZIP: {len(zip_bytes)/1e6:.1f} MB ({anac_url})")

    gare   = []
    righe  = 0

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        csv_name = next((n for n in zf.namelist() if n.endswith(".csv")), None)
        if not csv_name:
            return {"fonte":"ANAC","inserite":0,"errore":"CSV non trovato"}

        with zf.open(csv_name) as f:
            raw        = f.read()
            first_line = raw.split(b"\n")[0].decode("iso-8859-1", "replace")
            sep        = ";" if first_line.count(";") > first_line.count(",") else ","
            print(f"  📄 Separatore: '{sep}'")

            # Debug: mostra importo della prima riga dati
            lines = raw.split(b"\n")
            if len(lines) > 1:
                sample = lines[1].decode("iso-8859-1","replace").split(sep)
                headers_list = first_line.split(sep)
                if "importo_complessivo_gara" in headers_list:
                    idx = headers_list.index("importo_complessivo_gara")
                    if idx < len(sample):
                        print(f"  🔍 Esempio importo raw: '{sample[idx]}' → parsed: {parse_importo(sample[idx])}")

            reader = csv.DictReader(
                io.TextIOWrapper(io.BytesIO(raw), encoding="iso-8859-1"),
                delimiter=sep
            )
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
    r = requests.post(f"{WORKER_URL}/ted", json={}, timeout=60)
    print(f"  HTTP {r.status_code} | risposta: {r.text[:300]}")
    try:
        data    = r.json()
        notices = data.get("notices", data.get("results", []))
    except Exception as e:
        return {"fonte":"TED_EU","inserite":0,"errore":str(e)}

    oggi = datetime.now().strftime("%Y-%m-%dT00:00:00+00:00")
    gare = []
    for n in notices:
        cpv    = n.get("cpv-code", "")
        titolo = n.get("title", "")
        if not cpv_ok(cpv) and not kw_ok(titolo): continue
        importo = (n.get("estimated-value") or {}).get("amount", 0)
        pub_num = (n.get("publication-number") or "").strip()
        pub_url = pub_num.replace("/", "-").replace(" ", "-")
        scad    = n.get("deadline-receipt-request") or ""
        if scad and "+" not in scad and not scad.endswith("Z"): scad += "+00:00"
        gare.append({
            "codice_cig": None, "titolo": titolo or "(n/d)",
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

    print(f"  📊 {len(notices)} notices → {len(gare)} filtrate")
    inserite = insert_batch(gare)
    print(f"  ✅ {inserite} gare inserite")
    return {"fonte":"TED_EU","notices":len(notices),"filtrate":len(gare),"inserite":inserite}

if __name__ == "__main__":
    print(f"🚀 Gare Intelligence — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    risultati = [import_anac(), import_ted()]
    tot = sum(r.get("inserite", 0) for r in risultati)
    print(f"\n✅ TOTALE: {tot} gare inserite")
    print(json.dumps(risultati, indent=2, ensure_ascii=False))
