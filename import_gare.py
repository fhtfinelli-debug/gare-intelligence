"""
import_gare.py — Gare Intelligence FINAL
Fonti: ANAC (daily delta + monthly ZIP) + TED EU + ARIA Lombardia
"""
import os, io, csv, json, zipfile, requests, base64
from datetime import datetime, date, timezone

SUPABASE_URL  = os.environ.get("SUPABASE_URL",  "https://efhdooeqscqncgvhqfyu.supabase.co")
SERVICE_KEY   = os.environ.get("SUPABASE_SERVICE_KEY", "")
WORKER_URL    = os.environ.get("WORKER_URL",    "https://gare-relay.finellimanuel.workers.dev")
ARIA_CLIENT_ID     = os.environ.get("ARIA_CLIENT_ID", "")
ARIA_CLIENT_SECRET = os.environ.get("ARIA_CLIENT_SECRET", "")
MODE          = os.environ.get("MODE", "daily")

HEADERS_SB = {
    "apikey":        SERVICE_KEY,
    "Authorization": f"Bearer {SERVICE_KEY}",
    "Content-Type":  "application/json",
    "Prefer":        "resolution=ignore-duplicates,return=minimal",
}

# ── Filtri ANAC ───────────────────────────────────────────────────────────────
CPV_TARGET = [
    # Facility management, manutenzione, pulizia, HVAC, impianti
    "7999","5070","9091","7131","4521","5153","7200","4500","3913","7132","5050","9000",
    # Edilizia e costruzioni
    "4510","4511","4512","4513","4514","4515","4516","4517","4518","4519",
    "4520","4522","4523","4524","4525","4526","4527","4528","4529",
    "4530","4531","4532","4533","4534","4540","4541","4542","4543","4544","4545",
    "4550","4551","4552","4553","4554","4560","4561","4570","4580","4590",
    # Progettazione, architettura, ingegneria
    "7111","7112","7120","7121","7122","7123","7124","7125","7126","7127","7128","7129",
    "7130","7133","7134","7140","7141","7142","7143","7144","7148",
]
KEYWORDS = [
    "facility management","facility",
    "manutenzione ordinaria","manutenzione straordinaria","manutenzione",
    "pulizia","sanificazione","disinfezione","igienizzazione",
    "impianti termici","impianti elettrici","impianti hvac","hvac",
    "gestione edifici","gestione immobili","gestione impianti",
    "ristrutturazione","riqualificazione",
    "climatizzazione","condizionamento aria",
    "antincendio","energy service","efficienza energetica",
    "verde pubblico","sfalcio","vigilanza","portierato","ascensori","elevatori",
    "lavori edili","lavori edilizi","opere edili","costruzione",
    "ristrutturazione edilizia","recupero edilizio","restauro",
    "manutenzione stradale","opere stradali","pavimentazione",
    "impermeabilizzazione","coperture","tetti","facciate",
    "consolidamento","adeguamento sismico","miglioramento sismico",
    "bonifica","demolizione","ampliamento",
    "progettazione","progetto esecutivo","progetto definitivo",
    "direzione lavori","collaudo","verifica progetto",
    "architettura","architetto","ingegneria","ingegnere",
    "studio di fattibilita","relazione tecnica",
    "rilievo topografico","indagini geotecniche",
]
IMPORTO_MIN = 1_000
BATCH_SIZE  = 50

def cpv_ok(cpv):  return any((cpv or "")[:4].startswith(p) for p in CPV_TARGET)
def kw_ok(txt):   return any(k in (txt or "").lower() for k in KEYWORDS)

# ── Utility date ──────────────────────────────────────────────────────────────
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

def epoch_ms_to_iso(ms):
    """Converte EPOCH millisecondi in stringa ISO"""
    if not ms: return None
    try:
        dt = datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")
    except: return None

def epoch_ms_to_date(ms):
    if not ms: return None
    try:
        return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).date()
    except: return None

def mappa_stato_anac(stato_anac, scadenza_raw):
    stato = (stato_anac or "").upper()
    if stato in ("AGGIUDICATA","AGGIUDICATO","ESITATA","ESITATO"): return "aggiudicata"
    if stato in ("ANNULLATO","CANCELLATO"): return None
    scad = parse_scad_date(scadenza_raw)
    if not scad: return "attiva"
    diff = (scad - date.today()).days
    if diff < 0:  return "scaduta"
    if diff <= 7: return "in_scadenza"
    return "attiva"

def parse_importo(val):
    if not val: return 0
    val = str(val).strip().replace(" ","")
    try:
        if "," in val and "." in val: val = val.replace(".","").replace(",",".")
        elif "," in val: val = val.replace(",",".")
        elif val.count(".") > 1: val = val.replace(".","")
        return float(val)
    except: return 0

# ── Insert Supabase ───────────────────────────────────────────────────────────
def insert_batch(gare):
    inserite = 0
    for i in range(0, len(gare), BATCH_SIZE):
        batch = gare[i:i+BATCH_SIZE]
        r = requests.post(f"{SUPABASE_URL}/rest/v1/gare",
            headers=HEADERS_SB, json=batch, timeout=30)
        if r.status_code in (200, 201):
            inserite += len(batch)
        else:
            for gara in batch:
                r2 = requests.post(f"{SUPABASE_URL}/rest/v1/gare",
                    headers=HEADERS_SB, json=[gara], timeout=15)
                if r2.status_code in (200, 201):
                    inserite += 1
    return inserite

# ── ANAC: mappa riga CSV ──────────────────────────────────────────────────────
def riga_to_gara(r):
    importo = parse_importo(r.get("importo_complessivo_gara"))
    if importo == 0: importo = parse_importo(r.get("importo_lotto"))
    if importo < IMPORTO_MIN: return None
    scad_raw = r.get("data_scadenza_offerta","")
    stato    = mappa_stato_anac(r.get("stato",""), scad_raw)
    if stato not in ("attiva","in_scadenza"): return None
    cpv     = r.get("cod_cpv") or ""
    oggetto = r.get("oggetto_gara") or ""
    if not cpv_ok(cpv) and not kw_ok(oggetto): return None
    regione = (r.get("sezione_regionale") or "").replace("SEZIONE REGIONALE ","").strip() or None
    cig     = r.get("cig") or None
    return {
        "codice_cig":   cig,
        "titolo":       (oggetto or r.get("oggetto_lotto") or "(n/d)")[:500],
        "descrizione":  None, "riassunto_ai": None, "keywords_ai": [], "settore_ai": None,
        "ente":         r.get("denominazione_amministrazione_appaltante") or None,
        "regione":      regione, "provincia": r.get("provincia") or None, "comune": None,
        "categoria_cpv":   cpv[:20] if cpv else None,
        "categoria_label": r.get("descrizione_cpv") or None,
        "procedura":    r.get("tipo_scelta_contraente") or None,
        "criterio_aggiudicazione": None,
        "importo_min":  None, "importo_max":  None,
        "importo_totale": round(importo, 2),
        "scadenza":           parse_data(scad_raw),
        "data_pubblicazione": parse_data(r.get("data_pubblicazione")),
        "stato":        stato, "fonte": "ANAC",
        "url_bando":    f"https://dettaglio-cig.anticorruzione.it/cig/{cig}",
        "url_portale":  None, "id_sintel": None,
        "codice_gara":  r.get("numero_gara") or None, "rup": None,
    }

def processa_csv(raw_bytes):
    fl  = raw_bytes.split(b"\n")[0].decode("iso-8859-1","replace")
    sep = ";" if fl.count(";") > fl.count(",") else ","
    reader = csv.DictReader(
        io.TextIOWrapper(io.BytesIO(raw_bytes), encoding="iso-8859-1"),
        delimiter=sep
    )
    gare  = []
    righe = 0
    stati = {}
    for row in reader:
        righe += 1
        g = riga_to_gara(row)
        if g:
            gare.append(g)
            stati[g["stato"]] = stati.get(g["stato"],0) + 1
    return righe, gare, stati

# ── ANAC delta giornaliero ────────────────────────────────────────────────────
def import_anac_daily():
    print("🇮🇹 ANAC DELTA (giornaliero)")
    r = requests.get(f"{WORKER_URL}/anac-delta", timeout=60)
    if r.status_code != 200:
        return {"fonte":"ANAC_DELTA","inserite":0,"errore":f"HTTP {r.status_code}"}
    print(f"  ✅ CSV delta: {len(r.content)/1e3:.0f} KB")
    righe, gare, stati = processa_csv(r.content)
    print(f"  📊 {righe} righe → {len(gare)} attive/in_scadenza: {stati}")
    inserite = insert_batch(gare)
    print(f"  ✅ {inserite} nuove gare inserite")
    return {"fonte":"ANAC_DELTA","righe":righe,"filtrate":len(gare),"inserite":inserite}

# ── ANAC ZIP mensile ──────────────────────────────────────────────────────────
def import_anac_monthly():
    print("🇮🇹 ANAC ZIP (mensile)")
    r = requests.get(f"{WORKER_URL}/anac", timeout=180, stream=True)
    if r.status_code != 200:
        return {"fonte":"ANAC_ZIP","inserite":0,"errore":f"HTTP {r.status_code}"}
    anac_url  = r.headers.get("X-Anac-Url","n/d")
    zip_bytes = r.content
    print(f"  ✅ ZIP: {len(zip_bytes)/1e6:.1f} MB ({anac_url})")
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        csv_name = next((n for n in zf.namelist() if n.endswith(".csv")), None)
        if not csv_name:
            return {"fonte":"ANAC_ZIP","inserite":0,"errore":"CSV non trovato"}
        with zf.open(csv_name) as f:
            raw = f.read()
    righe, gare, stati = processa_csv(raw)
    print(f"  📊 {righe} righe → {len(gare)} attive/in_scadenza: {stati}")
    inserite = insert_batch(gare)
    print(f"  ✅ {inserite} nuove gare inserite")
    return {"fonte":"ANAC_ZIP","url":anac_url,"righe":righe,"filtrate":len(gare),"inserite":inserite}

# ── TED EU ────────────────────────────────────────────────────────────────────
def import_ted():
    print("🇪🇺 TED EU")
    oggi      = datetime.now().strftime("%Y-%m-%dT00:00:00+00:00")
    gare      = []
    pagina    = 1
    totale    = 0
    max_pagine = 1000

    while pagina <= max_pagine:
        try:
            r = requests.post(f"{WORKER_URL}/ted",
                json={"page": pagina}, timeout=60)
            if r.status_code != 200:
                print(f"  ⚠️  HTTP {r.status_code} a pagina {pagina}")
                break
            data    = r.json()
            notices = data.get("notices", [])
            if not notices: break
            if pagina == 1:
                totale = data.get("totalNoticeCount", 0)
                print(f"  📡 TED: {totale} notice totali, scarico fino a {max_pagine*10}")
            for n in notices:
                scad_list = n.get("deadline-receipt-request") or []
                scad = scad_list[0] if scad_list else ""
                if scad and "+" not in scad and not scad.endswith("Z"): scad += "+00:00"
                scad_date = parse_scad_date(scad[:10] if scad else "")
                if scad_date:
                    diff = (scad_date - date.today()).days
                    if diff < 0:    continue
                    elif diff <= 7: stato_ted = "in_scadenza"
                    else:           stato_ted = "attiva"
                else:
                    stato_ted = "attiva"
                titolo_dict = n.get("notice-title") or {}
                titolo = titolo_dict.get("ita") or titolo_dict.get("eng") or ""
                if isinstance(titolo, list): titolo = titolo[0] if titolo else ""
                ente_dict = n.get("buyer-name") or {}
                ente = ente_dict.get("ita") or ente_dict.get("eng") or ""
                if isinstance(ente, list): ente = ente[0] if ente else ""
                cpv_list = n.get("classification-cpv") or []
                cpv = cpv_list[0] if cpv_list else ""
                pub_num = (n.get("publication-number") or "").strip()
                links     = n.get("links") or {}
                html_link = (links.get("html") or {}).get("ITA") or \
                            f"https://ted.europa.eu/it/notice/-/detail/{pub_num}"
                pdf_link  = (links.get("pdf") or {}).get("ITA") or None
                pop       = n.get("place-of-performance") or []
                provincia = pop[0] if pop else None
                gare.append({
                    "codice_cig":   None,
                    "titolo":       titolo[:500] if titolo else "(n/d)",
                    "descrizione":  None, "riassunto_ai": None, "keywords_ai": [], "settore_ai": None,
                    "ente":         ente or None,
                    "regione":      "ITALIA", "provincia": provincia, "comune": None,
                    "categoria_cpv":   cpv[:20] if cpv else None, "categoria_label": None,
                    "procedura":    "Procedura aperta (EU)", "criterio_aggiudicazione": None,
                    "importo_min":  None, "importo_max": None, "importo_totale": None,
                    "scadenza":     scad or None, "data_pubblicazione": oggi,
                    "stato":        stato_ted, "fonte": "TED_EU",
                    "url_bando":    html_link, "url_portale": pdf_link,
                    "id_sintel":    None, "codice_gara": pub_num or None, "rup": None,
                })
            pagina += 1
        except Exception as e:
            print(f"  ❌ Errore pagina {pagina}: {e}")
            break

    print(f"  📊 {len(gare)} gare attive/in_scadenza su {pagina-1} pagine")
    inserite = insert_batch(gare)
    print(f"  ✅ {inserite} nuove gare inserite")
    return {"fonte":"TED_EU","totale":totale,"pagine":pagina-1,"filtrate":len(gare),"inserite":inserite}

# ── ARIA Lombardia ────────────────────────────────────────────────────────────
def import_aria_lombardia():
    print("🟢 ARIA LOMBARDIA — Catalogo Bandi")

    if not ARIA_CLIENT_ID or not ARIA_CLIENT_SECRET:
        print("  ⚠️  Credenziali ARIA non configurate — skip")
        return {"fonte":"ARIA_LOMBARDIA","inserite":0,"errore":"Credenziali mancanti"}

    BASE_URL  = "https://api.servizirl.it/c/servizi.rl/agora_catalogo/v2.0.0"
    TOKEN_URL = "https://api.servizirl.it/oauth2/token"

    # 1. Ottieni token OAuth2
    credentials = base64.b64encode(f"{ARIA_CLIENT_ID}:{ARIA_CLIENT_SECRET}".encode()).decode()
    try:
        r = requests.post(TOKEN_URL,
            headers={
                "Content-Type":  "application/x-www-form-urlencoded",
                "Authorization": f"Basic {credentials}",
            },
            data="grant_type=client_credentials&scope=agora_catalogo_bandi",
            timeout=30
        )
        token_data = r.json()
        token = token_data.get("access_token")
        if not token:
            print(f"  ❌ Token non ottenuto: {r.text[:200]}")
            return {"fonte":"ARIA_LOMBARDIA","inserite":0,"errore":"Token fallito"}
        print(f"  ✅ Token ottenuto")
    except Exception as e:
        print(f"  ❌ Errore token: {e}")
        return {"fonte":"ARIA_LOMBARDIA","inserite":0,"errore":str(e)}

    headers_api = {
        "Content-Type":  "application/json",
        "Authorization": f"Bearer {token}",
    }

    # 2. Scarica bandi con paginazione
    gare   = []
    start  = 0
    count  = 100
    totale = None
    oggi   = datetime.now().strftime("%Y-%m-%dT00:00:00+00:00")

    while True:
        body = {
            "Stato": ["APERTO", "IN APERTURA"],
            "Ordinamento": [{"Campo": "DataFine", "Tipo": "ASC"}]
        }
        try:
            r = requests.post(
                f"{BASE_URL}/catalogo/ricerca?start={start}&count={count}",
                headers=headers_api, json=body, timeout=30
            )
            data = r.json()
        except Exception as e:
            print(f"  ❌ Errore pagina start={start}: {e}")
            break

        lista    = data.get("Lista") or data.get("lista") or []
        n_totale = data.get("NumeroRisultati") or data.get("numeroRisultati") or 0

        if totale is None:
            totale = n_totale
            print(f"  📊 {totale} bandi trovati")

        if not lista:
            break

        for b in lista:
            # Date EPOCH ms
            data_fine   = b.get("DataFine")
            data_inizio = b.get("DataInizio")
            scadenza    = epoch_ms_to_iso(data_fine)
            data_pub    = epoch_ms_to_iso(data_inizio) or oggi
            stato_db    = "attiva"

            scad_date = epoch_ms_to_date(data_fine)
            if scad_date:
                diff = (scad_date - date.today()).days
                if diff < 0:    stato_db = "scaduta"
                elif diff <= 7: stato_db = "in_scadenza"

            stato_api = (b.get("Stato") or "").upper()
            if stato_api == "CHIUSO" or stato_db == "scaduta":
                continue

            # Ente
            ente_obj = b.get("EnteResponsabile") or {}
            if isinstance(ente_obj, dict):
                ente = ente_obj.get("Descrizione") or ente_obj.get("denominazione") or "Regione Lombardia"
            else:
                ente = "Regione Lombardia"

            codice    = b.get("Codice") or b.get("ID") or None
            url_bando = b.get("LinkPiattaforma") or b.get("RefUrl") or None
            url_portale = b.get("RefUrl") or None

            # Evita url_bando == url_portale (unique constraint)
            if url_bando == url_portale:
                url_portale = None

            gare.append({
                "codice_cig":   None,
                "titolo":       (b.get("Titolo") or "(n/d)")[:500],
                "descrizione":  b.get("Abstract") or None,
                "riassunto_ai": None, "keywords_ai": [], "settore_ai": None,
                "ente":         ente,
                "regione":      "LOMBARDIA", "provincia": None, "comune": None,
                "categoria_cpv":   None, "categoria_label": None,
                "procedura":    None, "criterio_aggiudicazione": None,
                "importo_min":  None, "importo_max": None, "importo_totale": None,
                "scadenza":           scadenza,
                "data_pubblicazione": data_pub,
                "stato":        stato_db, "fonte": "ARIA_LOMBARDIA",
                "url_bando":    url_bando, "url_portale": url_portale,
                "id_sintel":    None, "codice_gara": codice, "rup": None,
            })

        start += count
        if start >= (totale or 0):
            break

    gare = [g for g in gare if g["stato"] in ("attiva","in_scadenza")]
    print(f"  📊 {len(gare)} bandi attivi/in scadenza")
    inserite = insert_batch(gare)
    print(f"  ✅ {inserite} nuove gare inserite")
    return {"fonte":"ARIA_LOMBARDIA","totale":totale,"filtrate":len(gare),"inserite":inserite}

# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"🚀 Gare Intelligence [{MODE.upper()}] — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    risultati = []

    if MODE == "monthly":
        risultati.append(import_anac_monthly())
    else:
        risultati.append(import_anac_daily())

    risultati.append(import_ted())
    risultati.append(import_aria_lombardia())

    tot = sum(r.get("inserite",0) for r in risultati)
    print(f"\n✅ TOTALE: {tot} nuove gare inserite")
    print(json.dumps(risultati, indent=2, ensure_ascii=False))
