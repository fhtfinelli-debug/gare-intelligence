"""
import_gare.py — FINAL
- Giornaliero: CSV delta ANAC (gare nuove di giornata) + TED
- Mensile: ZIP CSV ANAC (recupero gare mancate)
Modalità: env var MODE = 'daily' (default) o 'monthly'
"""
import os, io, csv, json, zipfile, requests
from datetime import datetime, date

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://efhdooeqscqncgvhqfyu.supabase.co")
SERVICE_KEY  = os.environ.get("SUPABASE_SERVICE_KEY", "")
WORKER_URL   = os.environ.get("WORKER_URL", "https://gare-relay.finellimanuel.workers.dev")
MODE         = os.environ.get("MODE", "daily")  # daily o monthly

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
IMPORTO_MIN = 1_000
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
    if not scad: return "attiva"
    diff = (scad - date.today()).days
    if diff < 0:  return "scaduta"
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
    if stato not in ("attiva","in_scadenza"):
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
        "url_bando":    f"https://dettaglio-cig.anticorruzione.it/cig/{cig}",
        "url_portale":  None,
        "id_sintel":    None,
        "codice_gara":  r.get("numero_gara") or None,
        "rup":          None,
    }

def processa_csv(raw_bytes, sep=None):
    """Parse CSV bytes → lista gare filtrate"""
    fl  = raw_bytes.split(b"\n")[0].decode("iso-8859-1","replace")
    if sep is None:
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

def import_anac_daily():
    """Delta giornaliero — gare nuove/modificate oggi"""
    print("🇮🇹 ANAC DELTA (giornaliero)")
    url = f"{WORKER_URL}/anac-delta"
    r   = requests.get(url, timeout=60)
    if r.status_code != 200:
        return {"fonte":"ANAC_DELTA","inserite":0,"errore":f"HTTP {r.status_code}: {r.text[:100]}"}

    print(f"  ✅ CSV delta: {len(r.content)/1e3:.0f} KB")
    righe, gare, stati = processa_csv(r.content)
    print(f"  📊 {righe} righe → {len(gare)} attive/in_scadenza: {stati}")
    inserite = insert_batch(gare)
    print(f"  ✅ {inserite} nuove gare inserite")
    return {"fonte":"ANAC_DELTA","righe":righe,"filtrate":len(gare),"inserite":inserite}

def import_anac_monthly():
    """ZIP mensile — recupero completo mese precedente"""
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

def import_ted():
    print("🇪🇺 TED EU")
    oggi  = datetime.now().strftime("%Y-%m-%dT00:00:00+00:00")
    gare  = []
    pagina = 1
    totale = 0
    max_pagine = 30  # max 300 notice per run (aumentabile)

    while pagina <= max_pagine:
        try:
            r = requests.post(
                f"{WORKER_URL}/ted",
                json={"page": pagina},
                timeout=60
            )
            if r.status_code != 200:
                print(f"  ⚠️  HTTP {r.status_code} a pagina {pagina}")
                break
            data    = r.json()
            notices = data.get("notices", [])
            if not notices:
                break

            if pagina == 1:
                totale = data.get("totalNoticeCount", 0)
                print(f"  📡 TED: {totale} notice totali, scarico fino a {max_pagine*10}")

            for n in notices:
                # Scadenza — array
                scad_list = n.get("deadline-receipt-request") or []
                scad = scad_list[0] if scad_list else ""
                if scad and "+" not in scad and not scad.endswith("Z"): scad += "+00:00"

                # Salta se scaduta
                scad_date = parse_scad_date(scad[:10] if scad else "")
                if scad_date:
                    diff = (scad_date - date.today()).days
                    if diff < 0:    continue
                    elif diff <= 7: stato_ted = "in_scadenza"
                    else:           stato_ted = "attiva"
                else:
                    stato_ted = "attiva"

                # Titolo — dizionario multilingua
                titolo_dict = n.get("notice-title") or {}
                titolo = titolo_dict.get("ita") or titolo_dict.get("eng") or ""
                if isinstance(titolo, list): titolo = titolo[0] if titolo else ""

                # Ente — dizionario multilingua
                ente_dict = n.get("buyer-name") or {}
                ente = ente_dict.get("ita") or ente_dict.get("eng") or ""
                if isinstance(ente, list): ente = ente[0] if ente else ""

                # CPV
                cpv_list = n.get("classification-cpv") or []
                cpv = cpv_list[0] if cpv_list else ""

                pub_num = (n.get("publication-number") or "").strip()

                # URL bando italiano
                links     = n.get("links") or {}
                html_link = (links.get("html") or {}).get("ITA") or                             f"https://ted.europa.eu/it/notice/-/detail/{pub_num}"

                pop       = n.get("place-of-performance") or []
                provincia = pop[0] if pop else None

                gare.append({
                    "codice_cig":None,
                    "titolo":titolo[:500] if titolo else "(n/d)",
                    "descrizione":None,"riassunto_ai":None,"keywords_ai":[],"settore_ai":None,
                    "ente":ente or None,
                    "regione":"ITALIA","provincia":provincia,"comune":None,
                    "categoria_cpv":cpv[:20] if cpv else None,"categoria_label":None,
                    "procedura":"Procedura aperta (EU)","criterio_aggiudicazione":None,
                    "importo_min":None,"importo_max":None,"importo_totale":None,
                    "scadenza":scad or None,"data_pubblicazione":oggi,
                    "stato":stato_ted,"fonte":"TED_EU",
                    "url_bando":html_link,
                    "url_portale":None,"id_sintel":None,"codice_gara":pub_num or None,"rup":None,
                })

            pagina += 1

        except Exception as e:
            print(f"  ❌ Errore pagina {pagina}: {e}")
            break

    print(f"  📊 {len(gare)} gare attive/in_scadenza su {pagina-1} pagine")
    inserite = insert_batch(gare)
    print(f"  ✅ {inserite} nuove gare inserite")
    return {"fonte":"TED_EU","totale":totale,"pagine":pagina-1,"filtrate":len(gare),"inserite":inserite}


if __name__ == "__main__":
    print(f"🚀 Gare Intelligence [{MODE.upper()}] — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    risultati = []

    if MODE == "monthly":
        risultati.append(import_anac_monthly())
    else:
        risultati.append(import_anac_daily())

    risultati.append(import_ted())

    tot = sum(r.get("inserite",0) for r in risultati)
    print(f"\n✅ TOTALE: {tot} nuove gare inserite")
    print(json.dumps(risultati, indent=2, ensure_ascii=False))
