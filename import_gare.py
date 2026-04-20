"""
import_gare.py — Fix date DD/MM/YYYY + importo
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

def parse_data(d):
    """Converte DD/MM/YYYY o YYYY-MM-DD in timestamptz ISO"""
    if not d: return None
    d = str(d).strip()
    if not d: return None
    try:
        # Formato ANAC: DD/MM/YYYY
        if len(d) == 10 and d[2] == "/" and d[5] == "/":
            dd, mm, yyyy = d.split("/")
            return f"{yyyy}-{mm}-{dd}T00:00:00+00:00"
        # Formato ISO: YYYY-MM-DD
        if len(d) == 10 and d[4] == "-":
            return d + "T00:00:00+00:00"
        # Già timestamp
        if "T" in d:
            return d if ("+" in d or d.endswith("Z")) else d + "+00:00"
    except:
        pass
    return None

def parse_importo(val):
    """Gestisce 631146.08 e 1.250.000,00"""
    if not val: return 0
    val = str(val).strip().replace(" ", "")
    try:
        # Se c'è sia punto che virgola: formato italiano
        if "," in val and "." in val:
            val = val.replace(".", "").replace(",", ".")
        # Solo virgola: decimale italiano
        elif "," in val:
            val = val.replace(",", ".")
        # Solo punto: già formato corretto (o migliaia senza decimali)
        # Se ci sono più punti è formato migliaia (es: 1.000.000)
        elif val.count(".") > 1:
            val = val.replace(".", "")
        return float(val)
    except:
        return 0

def riga_to_gara(r):
    importo = parse_importo(r.get("importo_complessivo_gara"))
    if importo == 0:
        importo = parse_importo(r.get("importo_lotto"))
    if importo < IMPORTO_MIN: return None
    stato = (r.get("stato") or "").upper()
    if stato in ("ANNULLATO","CANCELLATO"): return None
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
        "scadenza":           parse_data(r.get("data_scadenza_offerta")),
        "data_pubblicazione": parse_data(r.get("data_pubblicazione")),
        "stato":        "PUBBLICATA",
        "fonte":        "ANAC",
        "url_bando":    f"https://api.anticorruzione.it/apicig/1.0.0/getSmartCig/{cig}",
        "url_portale":  "https://dati.anticorruzione.it",
        "id_sintel":    None,
        "codice_gara":  r.get("numero_gara") or None,
        "rup":          None,
    }

def insert_batch(gare):
    inserite = 0
    errori   = 0
    for i in range(0, len(gare), BATCH_SIZE):
        batch = gare[i:i+BATCH_SIZE]
        r = requests.post(f"{SUPABASE_URL}/rest/v1/gare",
            headers=HEADERS_SB, json=batch, timeout=30)
        if r.status_code in (200, 201):
            inserite += len(batch)
        else:
            errori += len(batch)
            if errori <= BATCH_SIZE:  # stampa solo il primo errore
                print(f"  ⚠️  Batch errore {r.status_code}: {r.text[:300]}")
    return inserite, errori

def import_anac():
    print("🇮🇹 ANAC — scarico via Cloudflare Worker")
    r = requests.get(f"{WORKER_URL}/anac", timeout=180, stream=True)
    if r.status_code != 200:
        return {"fonte":"ANAC","inserite":0,"errore":f"HTTP {r.status_code}"}

    anac_url  = r.headers.get("X-Anac-Url","n/d")
    zip_bytes = r.content
    print(f"  ✅ ZIP: {len(zip_bytes)/1e6:.1f} MB")

    # Debug: mostra parse date su prima riga
    gare  = []
    righe = 0
    data_sample_shown = False

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        csv_name = next((n for n in zf.namelist() if n.endswith(".csv")), None)
        if not csv_name:
            return {"fonte":"ANAC","inserite":0,"errore":"CSV non trovato"}
        with zf.open(csv_name) as f:
            raw  = f.read()
            fl   = raw.split(b"\n")[0].decode("iso-8859-1","replace")
            sep  = ";" if fl.count(";") > fl.count(",") else ","
            print(f"  📄 Separatore: '{sep}'")
            reader = csv.DictReader(
                io.TextIOWrapper(io.BytesIO(raw), encoding="iso-8859-1"),
                delimiter=sep
            )
            for row in reader:
                righe += 1
                # Debug prima riga
                if not data_sample_shown:
                    data_sample_shown = True
                    imp_raw = row.get("importo_complessivo_gara","")
                    dat_raw = row.get("data_pubblicazione","")
                    print(f"  🔍 Debug prima riga:")
                    print(f"     importo raw: '{imp_raw}' → {parse_importo(imp_raw)}")
                    print(f"     data_pub raw: '{dat_raw}' → {parse_data(dat_raw)}")
                    print(f"     scadenza raw: '{row.get('data_scadenza_offerta','')}' → {parse_data(row.get('data_scadenza_offerta',''))}")
                g = riga_to_gara(row)
                if g: gare.append(g)

    print(f"  📊 {righe} righe → {len(gare)} filtrate")
    inserite, errori = insert_batch(gare)
    print(f"  ✅ {inserite} inserite, {errori} errori")
    return {"fonte":"ANAC","url":anac_url,"righe_csv":righe,"filtrate":len(gare),"inserite":inserite,"errori":errori}

def import_ted():
    print("🇪🇺 TED — scarico via Cloudflare Worker")
    # Nessun pageSize — TED v3 non lo supporta
    r = requests.post(f"{WORKER_URL}/ted", json={}, timeout=60)
    print(f"  HTTP {r.status_code}: {r.text[:200]}")
    try:
        data    = r.json()
        notices = data.get("notices", data.get("results", []))
    except Exception as e:
        return {"fonte":"TED_EU","inserite":0,"errore":str(e)}

    print(f"  📡 {len(notices)} notices")
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
            "codice_cig":None,"titolo":(titolo or "(n/d)")[:500],
            "descrizione":None,"riassunto_ai":None,"keywords_ai":[],"settore_ai":None,
            "ente":n.get("contracting-authority-name") or None,
            "regione":"ITALIA","provincia":n.get("place-of-performance") or None,"comune":None,
            "categoria_cpv":cpv[:20] if cpv else None,"categoria_label":None,
            "procedura":"Procedura aperta (EU)","criterio_aggiudicazione":None,
            "importo_min":None,"importo_max":None,
            "importo_totale":float(importo) if importo else None,
            "scadenza":scad or None,"data_pubblicazione":oggi,
            "stato":"PUBBLICATA","fonte":"TED_EU",
            "url_bando":f"https://ted.europa.eu/en/notice/-/detail/{pub_url}",
            "url_portale":"https://ted.europa.eu",
            "id_sintel":None,"codice_gara":pub_num or None,"rup":None,
        })

    print(f"  📊 {len(gare)} filtrate")
    inserite, errori = insert_batch(gare)
    print(f"  ✅ {inserite} inserite")
    return {"fonte":"TED_EU","notices":len(notices),"filtrate":len(gare),"inserite":inserite}

if __name__ == "__main__":
    print(f"🚀 Gare Intelligence — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    risultati = [import_anac(), import_ted()]
    tot = sum(r.get("inserite",0) for r in risultati)
    print(f"\n✅ TOTALE: {tot} gare inserite")
    print(json.dumps(risultati, indent=2, ensure_ascii=False))
