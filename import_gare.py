"""
import_gare.py — Solo gare ATTIVE e IN_SCADENZA + fix batch duplicati
"""
import os, io, csv, json, zipfile, requests
from datetime import datetime, date

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
BATCH_SIZE  = 50

def cpv_ok(cpv):  return any((cpv or "")[:4].startswith(p) for p in CPV_TARGET)
def kw_ok(txt):   return any(k in (txt or "").lower() for k in KEYWORDS)

def parse_data(d):
    if not d: return None
    d = str(d).strip()
    try:
        if len(d) >= 10 and d[2] == "/":
            dd, mm, yyyy = d[:10].split("/")
            return f"{yyyy}-{mm}-{dd}T00:00:00+00:00"
        if len(d) >= 10 and d[4] == "-":
            return d[:10] + "T00:00:00+00:00"
        if "T" in d:
            return d if ("+" in d or d.endswith("Z")) else d + "+00:00"
    except: pass
    return None

def parse_scad_date(d):
    if not d: return None
    d = str(d).strip()
    try:
        if len(d) >= 10 and d[2] == "/":
            dd, mm, yyyy = d[:10].split("/")
            return date(int(yyyy), int(mm), int(dd))
        if len(d) >= 10 and d[4] == "-":
            return date.fromisoformat(d[:10])
    except: pass
    return None

def mappa_stato(stato_anac, scadenza_raw):
    stato = (stato_anac or "").upper()
    if stato in ("AGGIUDICATA","AGGIUDICATO","ESITATA","ESITATO"):
        return "aggiudicata"
    if stato in ("ANNULLATO","CANCELLATO"):
        return None
    scad = parse_scad_date(scadenza_raw)
    if not scad:
        return "attiva"
    diff = (scad - date.today()).days
    if diff < 0:  return "scaduta"      # ← ESCLUSA dal filtro sotto
    if diff <= 7: return "in_scadenza"
    return "attiva"

def parse_importo(val):
    if not val: return 0
    val = str(val).strip().replace(" ","")
    try:
        if "," in val and "." in val:
            val = val.replace(".","").replace(",",".")
        elif "," in val:
            val = val.replace(",",".")
        elif val.count(".") > 1:
            val = val.replace(".","")
        return float(val)
    except: return 0

def riga_to_gara(r):
    importo = parse_importo(r.get("importo_complessivo_gara"))
    if importo == 0:
        importo = parse_importo(r.get("importo_lotto"))
    if importo < IMPORTO_MIN: return None

    scad_raw = r.get("data_scadenza_offerta","")
    stato    = mappa_stato(r.get("stato",""), scad_raw)

    # ── SOLO attive e in_scadenza ──────────────────────────────────────────
    if stato not in ("attiva", "in_scadenza"):
        return None

    cpv     = r.get("cod_cpv") or ""
    oggetto = r.get("oggetto_gara") or ""
    if not cpv_ok(cpv) and not kw_ok(oggetto): return None

    regione = (r.get("sezione_regionale") or "").replace("SEZIONE REGIONALE ","").strip() or None
    cig     = r.get("cig") or None

    return {
        "codice_cig":   cig,
        "titolo":       (oggetto or r.get("oggetto_lotto") or "(n/d)")[:500],
        "descrizione":  None,
        "riassunto_ai": None,
        "keywords_ai":  [],
        "settore_ai":   None,
        "ente":         r.get("denominazione_amministrazione_appaltante") or None,
        "regione":      regione,
        "provincia":    r.get("provincia") or None,
        "comune":       None,
        "categoria_cpv":   cpv[:20] if cpv else None,
        "categoria_label": r.get("descrizione_cpv") or None,
        "procedura":    r.get("tipo_scelta_contraente") or None,
        "criterio_aggiudicazione": None,
        "importo_min":  None,
        "importo_max":  None,
        "importo_totale": round(importo, 2),
        "scadenza":           parse_data(scad_raw),
        "data_pubblicazione": parse_data(r.get("data_pubblicazione")),
        "stato":        stato,
        "fonte":        "ANAC",
        "url_bando":    f"https://dati.anticorruzione.it/superset/dashboard/dettaglio_cig/?cig={cig}",
        "url_portale":  None,
        "id_sintel":    None,
        "codice_gara":  r.get("numero_gara") or None,
        "rup":          None,
    }

def insert_batch(gare):
    """Insert batch — se fallisce riprova riga per riga (gestisce duplicati)"""
    inserite = 0
    for i in range(0, len(gare), BATCH_SIZE):
        batch = gare[i:i+BATCH_SIZE]
        r = requests.post(f"{SUPABASE_URL}/rest/v1/gare",
            headers=HEADERS_SB, json=batch, timeout=30)
        if r.status_code in (200, 201):
            inserite += len(batch)
        else:
            # Batch fallito — prova riga per riga
            for gara in batch:
                r2 = requests.post(f"{SUPABASE_URL}/rest/v1/gare",
                    headers=HEADERS_SB, json=[gara], timeout=15)
                if r2.status_code in (200, 201):
                    inserite += 1
    return inserite

def import_anac():
    print("🇮🇹 ANAC — scarico via Cloudflare Worker")
    r = requests.get(f"{WORKER_URL}/anac", timeout=180, stream=True)
    if r.status_code != 200:
        return {"fonte":"ANAC","inserite":0,"errore":f"HTTP {r.status_code}"}

    anac_url  = r.headers.get("X-Anac-Url","n/d")
    zip_bytes = r.content
    print(f"  ✅ ZIP: {len(zip_bytes)/1e6:.1f} MB ({anac_url})")

    gare  = []
    righe = 0
    stati = {}

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        csv_name = next((n for n in zf.namelist() if n.endswith(".csv")), None)
        if not csv_name:
            return {"fonte":"ANAC","inserite":0,"errore":"CSV non trovato"}
        with zf.open(csv_name) as f:
            raw  = f.read()
            fl   = raw.split(b"\n")[0].decode("iso-8859-1","replace")
            sep  = ";" if fl.count(";") > fl.count(",") else ","
            reader = csv.DictReader(
                io.TextIOWrapper(io.BytesIO(raw), encoding="iso-8859-1"),
                delimiter=sep
            )
            for row in reader:
                righe += 1
                g = riga_to_gara(row)
                if g:
                    gare.append(g)
                    stati[g["stato"]] = stati.get(g["stato"],0) + 1

    print(f"  📊 {righe} righe → {len(gare)} gare attive/in_scadenza: {stati}")
    inserite = insert_batch(gare)
    print(f"  ✅ {inserite} nuove gare inserite")
    return {"fonte":"ANAC","url":anac_url,"righe_csv":righe,"filtrate":len(gare),"inserite":inserite}

def import_ted():
    print("🇪🇺 TED — scarico via Cloudflare Worker")
    r = requests.post(f"{WORKER_URL}/ted", json={}, timeout=60)
    print(f"  HTTP {r.status_code}: {r.text[:200]}")
    try:
        data    = r.json()
        notices = data.get("notices", data.get("results", []))
    except Exception as e:
        return {"fonte":"TED_EU","inserite":0,"errore":str(e)}

    print(f"  📡 {len(notices)} notices ricevute")
    oggi = datetime.now().strftime("%Y-%m-%dT00:00:00+00:00")
    gare = []
    for n in notices:
        cpv    = n.get("cpv-code","")
        titolo = n.get("title","")
        if not cpv_ok(cpv) and not kw_ok(titolo): continue

        scad = n.get("deadline-receipt-request") or ""
        if scad and "+" not in scad and not scad.endswith("Z"): scad += "+00:00"

        # Calcola stato
        scad_date = parse_scad_date(scad[:10] if scad else "")
        if scad_date:
            diff = (scad_date - date.today()).days
            if diff < 0:   continue           # scaduta → salta
            elif diff <= 7: stato_ted = "in_scadenza"
            else:           stato_ted = "attiva"
        else:
            stato_ted = "attiva"

        importo = (n.get("estimated-value") or {}).get("amount",0)
        pub_num = (n.get("publication-number") or "").strip()
        pub_url = pub_num.replace("/","-").replace(" ","-")

        gare.append({
            "codice_cig":None,
            "titolo":(titolo or "(n/d)")[:500],
            "descrizione":None,"riassunto_ai":None,"keywords_ai":[],"settore_ai":None,
            "ente":n.get("contracting-authority-name") or None,
            "regione":"ITALIA",
            "provincia":n.get("place-of-performance") or None,
            "comune":None,
            "categoria_cpv":cpv[:20] if cpv else None,
            "categoria_label":None,
            "procedura":"Procedura aperta (EU)",
            "criterio_aggiudicazione":None,
            "importo_min":None,"importo_max":None,
            "importo_totale":float(importo) if importo else None,
            "scadenza":scad or None,
            "data_pubblicazione":oggi,
            "stato":stato_ted,
            "fonte":"TED_EU",
            "url_bando":f"https://ted.europa.eu/en/notice/-/detail/{pub_url}",
            "url_portale":None,
            "id_sintel":None,
            "codice_gara":pub_num or None,
            "rup":None,
        })

    print(f"  📊 {len(gare)} gare attive/in_scadenza filtrate")
    inserite = insert_batch(gare)
    print(f"  ✅ {inserite} nuove gare inserite")
    return {"fonte":"TED_EU","notices":len(notices),"filtrate":len(gare),"inserite":inserite}

if __name__ == "__main__":
    print(f"🚀 Gare Intelligence — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    risultati = [import_anac(), import_ted()]
    tot = sum(r.get("inserite",0) for r in risultati)
    print(f"\n✅ TOTALE: {tot} nuove gare inserite")
    print(json.dumps(risultati, indent=2, ensure_ascii=False))
