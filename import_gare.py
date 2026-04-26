"""
import_gare.py — Gare Intelligence FINAL
Fonti:
  - ANAC NAZIONALE: pubblicitalegale.anticorruzione.it (tutta Italia, giornaliero)
  - ANAC ZIP mensile: via Cloudflare Worker (solo il 2 del mese)
  - TED EU: gare europee con buyer-country=ITA
  - ARIA Lombardia: catalogo bandi SINTEL via API OAuth2

Daily:  ANAC Nazionale + TED + ARIA
Monthly: ANAC ZIP + ANAC Nazionale + TED + ARIA
"""

import os, io, csv, json, zipfile, requests, base64, time
from datetime import datetime, date, timedelta, timezone

# ── Configurazione ─────────────────────────────────────────────────────────────
SUPABASE_URL       = os.environ.get("SUPABASE_URL",  "https://efhdooeqscqncgvhqfyu.supabase.co")
SERVICE_KEY        = os.environ.get("SUPABASE_SERVICE_KEY", "")
WORKER_URL         = os.environ.get("WORKER_URL",    "https://gare-relay.finellimanuel.workers.dev")
ARIA_CLIENT_ID     = os.environ.get("ARIA_CLIENT_ID", "")
ARIA_CLIENT_SECRET = os.environ.get("ARIA_CLIENT_SECRET", "")
MODE               = os.environ.get("MODE", "daily")

IMPORTO_MIN = 1_000
BATCH_SIZE  = 50

HEADERS_SB = {
    "apikey":        SERVICE_KEY,
    "Authorization": f"Bearer {SERVICE_KEY}",
    "Content-Type":  "application/json",
    "Prefer":        "resolution=merge-duplicates,return=minimal",
}

# ── Filtri ANAC/TED ───────────────────────────────────────────────────────────
CPV_TARGET = [
    "7999","5070","9091","7131","4521","5153","7200","4500","3913","7132","5050","9000",
    "4510","4511","4512","4513","4514","4515","4516","4517","4518","4519",
    "4520","4522","4523","4524","4525","4526","4527","4528","4529",
    "4530","4531","4532","4533","4534","4540","4541","4542","4543","4544","4545",
    "4550","4551","4552","4553","4554","4560","4561","4570","4580","4590",
    "7111","7112","7120","7121","7122","7123","7124","7125","7126","7127","7128","7129",
    "7130","7133","7134","7140","7141","7142","7143","7144","7148",
]
KEYWORDS = [
    "facility management","facility","manutenzione ordinaria","manutenzione straordinaria",
    "manutenzione","pulizia","sanificazione","disinfezione","igienizzazione",
    "impianti termici","impianti elettrici","impianti hvac","hvac",
    "gestione edifici","gestione immobili","gestione impianti","ristrutturazione",
    "riqualificazione","climatizzazione","condizionamento aria","antincendio",
    "energy service","efficienza energetica","verde pubblico","sfalcio","vigilanza",
    "portierato","ascensori","elevatori","lavori edili","lavori edilizi","opere edili",
    "costruzione","ristrutturazione edilizia","recupero edilizio","restauro",
    "manutenzione stradale","opere stradali","pavimentazione","impermeabilizzazione",
    "coperture","tetti","facciate","consolidamento","adeguamento sismico",
    "miglioramento sismico","bonifica","demolizione","ampliamento","progettazione",
    "progetto esecutivo","progetto definitivo","direzione lavori","collaudo",
    "verifica progetto","architettura","architetto","ingegneria","ingegnere",
    "studio di fattibilita","relazione tecnica","rilievo topografico","indagini geotecniche",
]

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

def oggi_iso():
    return date.today().isoformat()

# ── Insert Supabase ───────────────────────────────────────────────────────────
def insert_batch(gare, on_conflict="codice_gara"):
    inserite = 0
    for i in range(0, len(gare), BATCH_SIZE):
        batch = gare[i:i+BATCH_SIZE]
        url = f"{SUPABASE_URL}/rest/v1/gare?on_conflict={on_conflict}"
        r = requests.post(url, headers=HEADERS_SB, json=batch, timeout=30)
        if r.status_code in (200, 201, 204):
            inserite += len(batch)
        else:
            print(f"  ⚠️  Batch {i//BATCH_SIZE+1} errore {r.status_code}: {r.text[:100]}")
            for gara in batch:
                r2 = requests.post(url, headers=HEADERS_SB, json=[gara], timeout=15)
                if r2.status_code in (200, 201, 204):
                    inserite += 1
    return inserite

# ══════════════════════════════════════════════════════════════════════════════
# ANAC NAZIONALE — pubblicitalegale.anticorruzione.it
# ══════════════════════════════════════════════════════════════════════════════

ANAC_BASE    = "https://pubblicitalegale.anticorruzione.it"
ANAC_API_URL = f"{ANAC_BASE}/api/v0/avvisi"
ANAC_HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0",
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "it-IT,it;q=0.9",
    "Cache-Control":   "no-cache",
    "Referer":         f"{ANAC_BASE}/bandi",
    "Sec-Fetch-Mode":  "cors",
    "Sec-Fetch-Site":  "same-origin",
}

PROV_REG = {
    "Aosta":"VALLE D'AOSTA",
    "Torino":"PIEMONTE","Vercelli":"PIEMONTE","Novara":"PIEMONTE","Cuneo":"PIEMONTE",
    "Asti":"PIEMONTE","Alessandria":"PIEMONTE","Biella":"PIEMONTE",
    "Verbano-Cusio-Ossola":"PIEMONTE","Verbano Cusio Ossola":"PIEMONTE",
    "Genova":"LIGURIA","Savona":"LIGURIA","La Spezia":"LIGURIA","Imperia":"LIGURIA",
    "Milano":"LOMBARDIA","Bergamo":"LOMBARDIA","Brescia":"LOMBARDIA","Como":"LOMBARDIA",
    "Cremona":"LOMBARDIA","Lecco":"LOMBARDIA","Lodi":"LOMBARDIA","Mantova":"LOMBARDIA",
    "Monza e della Brianza":"LOMBARDIA","Monza":"LOMBARDIA","Pavia":"LOMBARDIA",
    "Sondrio":"LOMBARDIA","Varese":"LOMBARDIA",
    "Trento":"TRENTINO-ALTO ADIGE","Bolzano":"TRENTINO-ALTO ADIGE","Bozen":"TRENTINO-ALTO ADIGE",
    "Venezia":"VENETO","Verona":"VENETO","Vicenza":"VENETO","Padova":"VENETO",
    "Treviso":"VENETO","Rovigo":"VENETO","Belluno":"VENETO",
    "Trieste":"FRIULI-VENEZIA GIULIA","Udine":"FRIULI-VENEZIA GIULIA",
    "Pordenone":"FRIULI-VENEZIA GIULIA","Gorizia":"FRIULI-VENEZIA GIULIA",
    "Bologna":"EMILIA-ROMAGNA","Modena":"EMILIA-ROMAGNA","Ferrara":"EMILIA-ROMAGNA",
    "Ravenna":"EMILIA-ROMAGNA","Forlì-Cesena":"EMILIA-ROMAGNA","Forli-Cesena":"EMILIA-ROMAGNA",
    "Rimini":"EMILIA-ROMAGNA","Parma":"EMILIA-ROMAGNA","Piacenza":"EMILIA-ROMAGNA",
    "Reggio nell'Emilia":"EMILIA-ROMAGNA","Reggio Emilia":"EMILIA-ROMAGNA",
    "Firenze":"TOSCANA","Pisa":"TOSCANA","Siena":"TOSCANA","Arezzo":"TOSCANA",
    "Grosseto":"TOSCANA","Livorno":"TOSCANA","Lucca":"TOSCANA",
    "Massa-Carrara":"TOSCANA","Massa Carrara":"TOSCANA","Pistoia":"TOSCANA","Prato":"TOSCANA",
    "Perugia":"UMBRIA","Terni":"UMBRIA",
    "Ancona":"MARCHE","Pesaro e Urbino":"MARCHE","Pesaro":"MARCHE",
    "Macerata":"MARCHE","Ascoli Piceno":"MARCHE","Fermo":"MARCHE",
    "Roma":"LAZIO","Latina":"LAZIO","Frosinone":"LAZIO","Viterbo":"LAZIO","Rieti":"LAZIO",
    "L'Aquila":"ABRUZZO","Pescara":"ABRUZZO","Chieti":"ABRUZZO","Teramo":"ABRUZZO",
    "Campobasso":"MOLISE","Isernia":"MOLISE",
    "Napoli":"CAMPANIA","Salerno":"CAMPANIA","Caserta":"CAMPANIA",
    "Avellino":"CAMPANIA","Benevento":"CAMPANIA",
    "Bari":"PUGLIA","Lecce":"PUGLIA","Taranto":"PUGLIA","Brindisi":"PUGLIA",
    "Foggia":"PUGLIA","Barletta-Andria-Trani":"PUGLIA",
    "Potenza":"BASILICATA","Matera":"BASILICATA",
    "Reggio di Calabria":"CALABRIA","Reggio Calabria":"CALABRIA",
    "Catanzaro":"CALABRIA","Cosenza":"CALABRIA","Crotone":"CALABRIA","Vibo Valentia":"CALABRIA",
    "Palermo":"SICILIA","Catania":"SICILIA","Messina":"SICILIA","Agrigento":"SICILIA",
    "Caltanissetta":"SICILIA","Enna":"SICILIA","Ragusa":"SICILIA",
    "Siracusa":"SICILIA","Trapani":"SICILIA",
    "Cagliari":"SARDEGNA","Sassari":"SARDEGNA","Nuoro":"SARDEGNA","Oristano":"SARDEGNA",
    "Sud Sardegna":"SARDEGNA",
}

def trova_regione(prov):
    if not prov: return None
    r = PROV_REG.get(prov)
    if r: return r
    plow = prov.lower()
    for k, v in PROV_REG.items():
        if k.lower() in plow or plow in k.lower():
            return v
    return None

def parse_anac_record(rec):
    id_avviso = rec.get("idAvviso", "")
    data_scad = rec.get("dataScadenza", "")
    data_pub  = rec.get("dataPubblicazione", "")
    tipo      = rec.get("tipo", "avviso")
    templates = rec.get("template", [])
    if not templates: return None
    tmpl     = templates[0].get("template", {})
    sections = tmpl.get("sections", [])
    descrizione = (tmpl.get("metadata", {}).get("descrizione") or "").strip()

    ente = None
    for s in sections:
        if "SEZ. A" in s.get("name", ""):
            soggetti = s.get("fields", {}).get("soggetti_sa", [])
            if soggetti:
                nomi = [sg.get("denominazione_amministrazione","") for sg in soggetti if sg.get("denominazione_amministrazione")]
                ente = " / ".join(nomi) if nomi else None
            break

    url_documenti = None
    tipo_procedura = None
    for s in sections:
        if "SEZ. B" in s.get("name", ""):
            f = s.get("fields", {})
            url_documenti  = f.get("documenti_di_gara_link")
            tipo_procedura = f.get("tipo_procedura_aggiudicazione")
            break

    cig = importo_totale = 0.0
    cig = cpv_label = provincia = comune = scadenza_lotto = None
    natura_set = set()
    for s in sections:
        if "SEZ. C" in s.get("name", ""):
            for idx, lotto in enumerate(s.get("items", [])):
                if cig is None: cig = lotto.get("cig")
                try: importo_totale += float(lotto.get("valore_complessivo_stimato") or 0)
                except: pass
                natura = lotto.get("natura_principale")
                if natura: natura_set.add(natura)
                if idx == 0:
                    cpv_label      = lotto.get("cpv")
                    provincia      = lotto.get("luogo_nuts")
                    comune         = lotto.get("luogo_istat")
                    scadenza_lotto = lotto.get("termine_ricezione") or data_scad
            break

    importo_val = round(importo_totale, 2) if importo_totale > 0 else None
    regione     = trova_regione(provincia)
    scad_iso    = scadenza_lotto or data_scad or None
    if scad_iso:
        scad_iso = str(scad_iso).strip()
        if "+" not in scad_iso and not scad_iso.endswith("Z"):
            scad_iso += "+00:00"

    stato = "attiva"
    if scad_iso:
        try:
            diff = (datetime.fromisoformat(scad_iso[:10]).date() - date.today()).days
            if diff < 0: stato = "scaduta"
            elif diff <= 7: stato = "in_scadenza"
        except: pass

    return {
        "codice_cig":   cig,
        "titolo":       (descrizione or "(n/d)")[:500],
        "descrizione":  " / ".join(sorted(natura_set)) if natura_set else None,
        "riassunto_ai": None, "keywords_ai": [], "settore_ai": None,
        "ente":         ente, "regione": regione, "provincia": provincia,
        "comune":       comune, "categoria_cpv": None, "categoria_label": cpv_label,
        "procedura":    tipo_procedura or tipo, "criterio_aggiudicazione": None,
        "importo_min":  None, "importo_max": None, "importo_totale": importo_val,
        "scadenza":     scad_iso, "data_pubblicazione": data_pub or None,
        "stato":        stato, "fonte": "ANAC_NAZIONALE",
        "url_bando":    f"{ANAC_BASE}/bandi/{id_avviso}?ricercaArchivio=false" if id_avviso else None,
        "url_portale":  url_documenti, "id_sintel": None,
        "codice_gara":  id_avviso, "rup": None,
    }

def insert_anac_nazionale(gare):
    inserite = doc_persi = 0
    con_cig   = [g for g in gare if g.get("codice_cig")]
    senza_cig = [g for g in gare if not g.get("codice_cig")]
    print(f"  📋 {len(con_cig)} con CIG, {len(senza_cig)} senza CIG")

    for gruppo, conflict in [(con_cig, "codice_cig"), (senza_cig, "codice_gara")]:
        for i in range(0, len(gruppo), BATCH_SIZE):
            batch = gruppo[i:i+BATCH_SIZE]
            url   = f"{SUPABASE_URL}/rest/v1/gare?on_conflict={conflict}"
            r = requests.post(url, headers=HEADERS_SB, json=batch, timeout=30)
            if r.status_code in (200, 201, 204):
                inserite += len(batch)
            else:
                for g in batch:
                    r2 = requests.post(url, headers=HEADERS_SB, json=[g], timeout=15)
                    if r2.status_code in (200, 201, 204):
                        inserite += 1
                    elif r2.status_code == 409 and "url_portale" in r2.text:
                        g_clean = {**g, "url_portale": None}
                        r3 = requests.post(url, headers=HEADERS_SB, json=[g_clean], timeout=15)
                        if r3.status_code in (200, 201, 204):
                            inserite += 1
                            doc_persi += 1

    if doc_persi:
        print(f"  ℹ️  {doc_persi} bandi senza link documenti (url_portale già in uso)")
    return inserite

def import_anac_nazionale():
    print("🇮🇹 ANAC NAZIONALE — pubblicitalegale.anticorruzione.it")
    ieri    = date.today() - timedelta(days=1)
    ieri_it = ieri.strftime("%d/%m/%Y")
    print(f"  📅 {ieri_it}")

    gare   = []
    pagina = 0
    while True:
        params = {
            "dataPubblicazioneStart": ieri_it,
            "dataPubblicazioneEnd":   ieri_it,
            "page": pagina, "size": 100,
            "codiceScheda": "2,4",
            "sortField": "dataPubblicazione",
            "sortDirection": "desc",
        }
        try:
            r = requests.get(ANAC_API_URL, headers=ANAC_HEADERS, params=params, timeout=30)
            if r.status_code == 429:
                print(f"  ⚠️  Rate limit — attendo 30s")
                time.sleep(30); continue
            if r.status_code != 200:
                print(f"  ⚠️  HTTP {r.status_code}: {r.text[:100]}")
                break
            d = r.json()
        except Exception as e:
            print(f"  ❌ Errore p.{pagina}: {e}"); break

        records = d.get("content", [])
        tot     = d.get("totalElements", 0)
        tot_pag = d.get("totalPages", 1)

        if pagina == 0:
            if tot == 0:
                print(f"  ℹ️  0 bandi — giorno festivo o weekend")
                break
            print(f"  📊 {tot} bandi totali, {tot_pag} pagine")

        if not records: break

        for rec in records:
            g = parse_anac_record(rec)
            if g and g["stato"] != "scaduta":
                gare.append(g)

        pagina += 1
        if pagina >= tot_pag: break
        time.sleep(0.3)

    print(f"  📊 {len(gare)} bandi attivi/in_scadenza")
    if not gare:
        return {"fonte": "ANAC_NAZIONALE", "filtrate": 0, "inserite": 0}

    inserite = insert_anac_nazionale(gare)
    print(f"  ✅ {inserite} gare inserite/aggiornate")
    return {"fonte": "ANAC_NAZIONALE", "filtrate": len(gare), "inserite": inserite}

# ══════════════════════════════════════════════════════════════════════════════
# ANAC: mappa riga CSV (per ZIP mensile)
# ══════════════════════════════════════════════════════════════════════════════
def riga_to_gara(r, debug_counters=None):
    importo = parse_importo(r.get("importo_complessivo_gara"))
    if importo == 0: importo = parse_importo(r.get("importo_lotto"))
    if importo < IMPORTO_MIN:
        if debug_counters is not None: debug_counters["scartate_importo"] += 1
        return None
    scad_raw = r.get("data_scadenza_offerta","")
    stato    = mappa_stato_anac(r.get("stato",""), scad_raw)
    if stato not in ("attiva","in_scadenza"):
        if debug_counters is not None:
            chiave = f"scartate_stato_{stato or 'None'}"
            debug_counters[chiave] = debug_counters.get(chiave, 0) + 1
        return None
    cpv     = r.get("cod_cpv") or ""
    oggetto = r.get("oggetto_gara") or ""
    if not cpv_ok(cpv) and not kw_ok(oggetto):
        if debug_counters is not None: debug_counters["scartate_cpv_kw"] += 1
        return None
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
        "importo_min":  None, "importo_max": None, "importo_totale": round(importo, 2),
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
        io.TextIOWrapper(io.BytesIO(raw_bytes), encoding="iso-8859-1"), delimiter=sep)
    gare = []; righe = 0; stati = {}
    debug = {"scartate_importo": 0, "scartate_cpv_kw": 0}
    for row in reader:
        righe += 1
        g = riga_to_gara(row, debug_counters=debug)
        if g:
            gare.append(g)
            stati[g["stato"]] = stati.get(g["stato"],0) + 1
    print(f"  🔍 Scartate importo: {debug['scartate_importo']}, CPV/kw: {debug['scartate_cpv_kw']}, ok: {len(gare)}")
    return righe, gare, stati

def import_anac_monthly():
    print("🇮🇹 ANAC ZIP (mensile)")
    r = requests.get(f"{WORKER_URL}/anac", timeout=180, stream=True)
    if r.status_code != 200:
        return {"fonte":"ANAC_ZIP","inserite":0,"errore":f"HTTP {r.status_code}"}
    anac_url  = r.headers.get("X-Anac-Url","n/d")
    zip_bytes = r.content
    print(f"  ✅ ZIP: {len(zip_bytes)/1e6:.1f} MB")
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        csv_name = next((n for n in zf.namelist() if n.endswith(".csv")), None)
        if not csv_name:
            return {"fonte":"ANAC_ZIP","inserite":0,"errore":"CSV non trovato"}
        with zf.open(csv_name) as f:
            raw = f.read()
    righe, gare, stati = processa_csv(raw)
    print(f"  📊 {righe} righe → {len(gare)} attive/in_scadenza: {stati}")
    inserite = insert_batch(gare, on_conflict="codice_cig")
    print(f"  ✅ {inserite} gare inserite/aggiornate")
    return {"fonte":"ANAC_ZIP","url":anac_url,"righe":righe,"filtrate":len(gare),"inserite":inserite}

# ══════════════════════════════════════════════════════════════════════════════
# TED EU
# ══════════════════════════════════════════════════════════════════════════════
def import_ted():
    print("🇪🇺 TED EU")
    oggi      = datetime.now().strftime("%Y-%m-%dT00:00:00+00:00")
    gare      = []
    pagina    = 1
    totale    = 0
    max_pagine = 1000

    while pagina <= max_pagine:
        try:
            r = requests.post(f"{WORKER_URL}/ted", json={"page": pagina}, timeout=60)
            if r.status_code != 200:
                print(f"  ⚠️  HTTP {r.status_code} a pagina {pagina}"); break
            data    = r.json()
            notices = data.get("notices", [])
            if not notices: break
            if pagina == 1:
                totale = data.get("totalNoticeCount", 0)
                print(f"  📡 TED: {totale} notice totali")
            for n in notices:
                scad_list = n.get("deadline-receipt-request") or []
                scad = scad_list[0] if scad_list else ""
                if scad and "+" not in scad and not scad.endswith("Z"): scad += "+00:00"
                scad_date = parse_scad_date(scad[:10] if scad else "")
                if scad_date:
                    diff = (scad_date - date.today()).days
                    if diff < 0: continue
                    elif diff <= 7: stato_ted = "in_scadenza"
                    else: stato_ted = "attiva"
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
                importo_obj = n.get("estimated-value-lot") or n.get("estimated-value") or {}
                if isinstance(importo_obj, list) and importo_obj:
                    importo_obj = importo_obj[0]
                if isinstance(importo_obj, dict):
                    importo_val = importo_obj.get("amount") or importo_obj.get("value") or 0
                elif isinstance(importo_obj, (int, float)):
                    importo_val = importo_obj
                else:
                    importo_val = 0
                try: importo_val = float(importo_val)
                except: importo_val = 0
                gare.append({
                    "codice_cig": None, "titolo": titolo[:500] if titolo else "(n/d)",
                    "descrizione": None, "riassunto_ai": None, "keywords_ai": [], "settore_ai": None,
                    "ente": ente or None, "regione": "ITALIA", "provincia": provincia, "comune": None,
                    "categoria_cpv": cpv[:20] if cpv else None, "categoria_label": None,
                    "procedura": "Procedura aperta (EU)", "criterio_aggiudicazione": None,
                    "importo_min": None, "importo_max": None,
                    "importo_totale": round(importo_val, 2) if importo_val > 0 else None,
                    "scadenza": scad or None, "data_pubblicazione": oggi,
                    "stato": stato_ted, "fonte": "TED_EU",
                    "url_bando": html_link, "url_portale": pdf_link,
                    "id_sintel": None, "codice_gara": pub_num or None, "rup": None,
                })
            pagina += 1
        except Exception as e:
            print(f"  ❌ Errore pagina {pagina}: {e}"); break

    print(f"  📊 {len(gare)} gare su {pagina-1} pagine")
    inserite = insert_batch(gare, on_conflict="url_portale")
    print(f"  ✅ {inserite} gare inserite/aggiornate")
    return {"fonte":"TED_EU","totale":totale,"pagine":pagina-1,"filtrate":len(gare),"inserite":inserite}

# ══════════════════════════════════════════════════════════════════════════════
# ARIA Lombardia
# ══════════════════════════════════════════════════════════════════════════════
def import_aria_lombardia():
    print("🟢 ARIA LOMBARDIA — Catalogo Bandi")
    if not ARIA_CLIENT_ID or not ARIA_CLIENT_SECRET:
        print("  ⚠️  Credenziali ARIA non configurate — skip")
        return {"fonte":"ARIA_LOMBARDIA","inserite":0,"errore":"Credenziali mancanti"}

    BASE_URL  = "https://api.servizirl.it/c/servizi.rl/agora_catalogo/v2.0.0"
    TOKEN_URL = "https://api.servizirl.it/oauth2/token"
    credentials = base64.b64encode(f"{ARIA_CLIENT_ID}:{ARIA_CLIENT_SECRET}".encode()).decode()
    try:
        r = requests.post(TOKEN_URL,
            headers={"Content-Type":"application/x-www-form-urlencoded","Authorization":f"Basic {credentials}"},
            data="grant_type=client_credentials&scope=agora_catalogo_bandi", timeout=30)
        token = r.json().get("access_token")
        if not token:
            print(f"  ❌ Token non ottenuto: {r.text[:200]}")
            return {"fonte":"ARIA_LOMBARDIA","inserite":0,"errore":"Token fallito"}
        print("  ✅ Token ottenuto")
    except Exception as e:
        print(f"  ❌ Errore token: {e}")
        return {"fonte":"ARIA_LOMBARDIA","inserite":0,"errore":str(e)}

    headers_api = {"Content-Type":"application/json","Authorization":f"Bearer {token}"}
    gare = []; start = 0; count = 100; totale = None
    oggi = datetime.now().strftime("%Y-%m-%dT00:00:00+00:00")

    while True:
        body = {"Ordinamento": [{"Campo": "DataFine", "Tipo": "ASC"}]}
        try:
            r = requests.post(f"{BASE_URL}/catalogo/ricerca?start={start}&count={count}",
                headers=headers_api, json=body, timeout=30)
            data = r.json()
        except Exception as e:
            print(f"  ❌ Errore start={start}: {e}"); break

        lista    = data.get("Lista") or data.get("lista") or []
        n_totale = data.get("NumeroRisultati") or data.get("numeroRisultati") or 0
        if totale is None:
            totale = n_totale
            print(f"  📊 {totale} bandi trovati")
        if not lista: break

        for b in lista:
            stato_raw = (b.get("StatoProcedura") or b.get("stato") or "").upper()
            if any(x in stato_raw for x in ["ANNULL","REVOC","CHIUS","SCAD"]): continue
            scad_raw = b.get("DataFineRicezioneOfferte") or b.get("scadenza") or ""
            scadenza = epoch_ms_to_iso(b.get("DataFine"))
            data_pub = epoch_ms_to_iso(b.get("DataInizio")) or oggi
            stato_db = "attiva"
            scad_date = epoch_ms_to_date(b.get("DataFine"))
            if scad_date:
                diff = (scad_date - date.today()).days
                if diff < 0: stato_db = "scaduta"
                elif diff <= 7: stato_db = "in_scadenza"
            if stato_db == "scaduta": continue
            importo_val = None
            try:
                raw = b.get("ValoreEconomico") or b.get("DotazioneFinanziaria") or b.get("importo") or 0
                importo_val = float(str(raw).replace(",",".").replace(" ","") or 0) or None
            except: pass
            ente_obj = b.get("EnteResponsabile") or {}
            ente = (ente_obj.get("Descrizione") or ente_obj.get("denominazione") or "Regione Lombardia") if isinstance(ente_obj, dict) else "Regione Lombardia"
            codice  = b.get("Codice") or b.get("ID") or None
            url_p   = b.get("LinkPiattaforma") or b.get("RefUrl") or None
            importo_val_det = None
            descrizione_det = b.get("Abstract") or None
            if codice:
                try:
                    rd = requests.get(f"{BASE_URL}/catalogo/dettaglio/{codice}", headers=headers_api, timeout=15)
                    if rd.status_code == 200:
                        det = rd.json()
                        dot = det.get("DotazioneFinanziaria") or ""
                        if dot:
                            import re
                            nums = re.findall(r"[\d]+(?:\.\d+)?", dot.replace("€","").replace(".","").replace(",",".").strip())
                            if nums:
                                try: importo_val_det = float(nums[0])
                                except: pass
                        if det.get("Descrizione"): descrizione_det = det["Descrizione"][:1000]
                except: pass
            gare.append({
                "codice_cig": None, "titolo": (b.get("Titolo") or "(n/d)")[:500],
                "descrizione": descrizione_det, "riassunto_ai": None, "keywords_ai": [], "settore_ai": None,
                "ente": ente, "regione": "LOMBARDIA", "provincia": None, "comune": None,
                "categoria_cpv": None, "categoria_label": b.get("CategorieMerceologiche") or None,
                "procedura": b.get("TipoProcedura") or None, "criterio_aggiudicazione": None,
                "importo_min": None, "importo_max": None,
                "importo_totale": round(importo_val_det or importo_val, 2) if (importo_val_det or importo_val) else None,
                "scadenza": scadenza, "data_pubblicazione": data_pub,
                "stato": stato_db, "fonte": "ARIA_LOMBARDIA",
                "url_bando": url_p, "url_portale": url_p,
                "id_sintel": str(b.get("IdProcedura") or "") or None,
                "codice_gara": str(codice) if codice else None, "rup": b.get("RUP") or None,
            })

        start += count
        if start >= (totale or 0): break

    print(f"  📊 {len(gare)} bandi da inserire")
    inserite = insert_batch(gare, on_conflict="codice_gara")
    print(f"  ✅ {inserite} gare inserite/aggiornate")
    return {"fonte":"ARIA_LOMBARDIA","totale":totale,"filtrate":len(gare),"inserite":inserite}

# ══════════════════════════════════════════════════════════════════════════════
# Aggiornamento stati
# ══════════════════════════════════════════════════════════════════════════════
def aggiorna_stati():
    print("🔄 Aggiornamento stati gare esistenti")
    oggi    = date.today()
    tra_7gg = (oggi + timedelta(days=7)).isoformat()
    oggi_s  = oggi.isoformat()

    for params, body, label in [
        ({"stato":"eq.attiva","scadenza":f"lt.{oggi_s}T00:00:00+00:00"},{"stato":"scaduta"},"attiva → scaduta"),
        ({"stato":"eq.in_scadenza","scadenza":f"lt.{oggi_s}T00:00:00+00:00"},{"stato":"scaduta"},"in_scadenza → scaduta"),
        ({"stato":"eq.attiva","scadenza":f"gte.{oggi_s}T00:00:00+00:00","and":f"(scadenza.lte.{tra_7gg}T23:59:59+00:00)"},{"stato":"in_scadenza"},"attiva → in_scadenza"),
        ({"stato":"eq.in_scadenza","scadenza":f"gt.{tra_7gg}T23:59:59+00:00"},{"stato":"attiva"},"in_scadenza → attiva"),
    ]:
        r = requests.patch(f"{SUPABASE_URL}/rest/v1/gare",
            headers={**HEADERS_SB,"Prefer":"return=minimal"},
            params=params, json=body, timeout=30)
        print(f"  {label}: HTTP {r.status_code}")

    print("  ✅ Aggiornamento stati completato")
    return {"aggiornamento_stati":"ok"}

# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print(f"🚀 Gare Intelligence [{MODE.upper()}] — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    risultati = []

    if MODE == "monthly":
        print("📅 Modalità MONTHLY — ANAC ZIP + ANAC Nazionale + TED + ARIA")
        risultati.append(import_anac_monthly())
    else:
        print("📅 Modalità DAILY — ANAC Nazionale + TED + ARIA")

    # ANAC Nazionale: tutta Italia ogni giorno
    risultati.append(import_anac_nazionale())

    # TED EU: gare europee
    risultati.append(import_ted())

    # ARIA Lombardia: catalogo SINTEL
    risultati.append(import_aria_lombardia())

    # Aggiorna stati
    aggiorna_stati()

    tot = sum(r.get("inserite",0) for r in risultati)
    print(f"\n✅ TOTALE: {tot} gare inserite/aggiornate")
    print(json.dumps(risultati, indent=2, ensure_ascii=False))
