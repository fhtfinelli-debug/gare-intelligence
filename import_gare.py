"""
import_gare.py — Gare Intelligence
Fonti: ANAC (ZIP mensile) + TED EU + ARIA Lombardia

Fix 2026-04-25:
- ANAC delta rimosso: il log CSV non ha importo, inutile
- TED: sleep 2s tra pagine + retry su 429
- ARIA/TED: insert_batch accetta HTTP 204 come successo (merge-duplicates)
"""

import os, io, csv, json, zipfile, requests, base64, time
from datetime import datetime, date, timedelta, timezone

# ── Configurazione ─────────────────────────────────────────────────────────────
SUPABASE_URL       = os.environ.get("SUPABASE_URL", "https://efhdooeqscqncgvhqfyu.supabase.co")
SERVICE_KEY        = os.environ.get("SUPABASE_SERVICE_KEY", "")
WORKER_URL         = os.environ.get("WORKER_URL", "https://gare-relay.finellimanuel.workers.dev")
ARIA_CLIENT_ID     = os.environ.get("ARIA_CLIENT_ID", "")
ARIA_CLIENT_SECRET = os.environ.get("ARIA_CLIENT_SECRET", "")
MODE               = os.environ.get("MODE", "daily")

IMPORTO_MIN = 1_000
BATCH_SIZE  = 100

HEADERS_SB = {
    "apikey":        SERVICE_KEY,
    "Authorization": f"Bearer {SERVICE_KEY}",
    "Content-Type":  "application/json",
    "Prefer":        "resolution=merge-duplicates,return=minimal",
}

# ── Filtri CPV e keyword ───────────────────────────────────────────────────────
CPV_TARGET = [
    "7999","5070","9091","7131","4521","5153","7200","4500","3913","7132",
    "5050","9000","4510","4511","4512","4513","4514","4515","4516","4517","4518",
    "4530","4540","4550","6311","7221","7222","7223","7224","7225","7226",
    "6000","6010","6020","6030","6100","6110","6120","6130","6200","6300",
    "8000","8010","8020","8030","8040","8050","8060","8070","8090","8100",
]

KW_TARGET = [
    "manutenzione","pulizia","facility","giardinaggio","verde","facchinaggio",
    "ristorazione","vigilanza","sicurezza","portierato","lavanderia","trasloco",
    "edilizia","costruzione","ristrutturazione","lavori","opere","impianti",
    "elettrico","idraulico","termico","condizionamento","ascensore","elevatore",
    "informatica","software","hardware","digitalizzazione","servizi informatici",
    "consulenza","formazione","progettazione","ingegneria","architettura",
    "trasporto","logistica","fornitura","noleggio","affitto",
]

def cpv_ok(cpv):
    if not cpv:
        return False
    cpv_clean = cpv.replace(".","").replace("-","")[:4]
    return any(cpv_clean.startswith(t) for t in CPV_TARGET)

def kw_ok(testo):
    if not testo:
        return False
    t = testo.lower()
    return any(k in t for k in KW_TARGET)

# ── Utility date ───────────────────────────────────────────────────────────────
def parse_data(val):
    if not val:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d",
                "%d/%m/%Y %H:%M:%S", "%d/%m/%Y"):
        try:
            return datetime.strptime(str(val).strip()[:19], fmt).isoformat()
        except:
            pass
    return None

def parse_importo(val):
    if not val:
        return 0
    try:
        val = str(val).strip()
        if "." in val and "," in val:
            val = val.replace(".","").replace(",",".")
        elif "," in val:
            val = val.replace(",",".")
        elif val.count(".") > 1:
            val = val.replace(".","")
        return float(val)
    except:
        return 0

def oggi_iso():
    return date.today().isoformat()

def mappa_stato_anac(stato_raw, scad_raw):
    s = (stato_raw or "").upper()
    if any(x in s for x in ["ANNULL","REVOC","CANCEL"]):
        return "annullata"
    if any(x in s for x in ["AGGIUD","AGGIUDICATA","CONCLUS"]):
        return "aggiudicata"
    if any(x in s for x in ["SCAD","CHIUS","ESIT"]):
        return "scaduta"
    if scad_raw:
        try:
            scad = datetime.strptime(scad_raw.strip()[:10], "%Y-%m-%d").date()
            oggi = date.today()
            if scad < oggi:
                return "scaduta"
            if scad <= oggi + timedelta(days=7):
                return "in_scadenza"
        except:
            pass
    return "attiva"

# ── Insert Supabase ────────────────────────────────────────────────────────────
def insert_batch(gare):
    """
    FIX CRITICO: merge-duplicates restituisce HTTP 204 (no content) quando
    aggiorna un record esistente invece di 200/201. Accettiamo tutti e tre.
    """
    inserite = 0
    for i in range(0, len(gare), BATCH_SIZE):
        batch = gare[i:i+BATCH_SIZE]
        try:
            r = requests.post(
                f"{SUPABASE_URL}/rest/v1/gare",
                headers=HEADERS_SB,
                json=batch,
                timeout=30
            )
            # FIX: accetta 200 (insert), 201 (created), 204 (merge-duplicates ok)
            if r.status_code in (200, 201, 204):
                inserite += len(batch)
            else:
                print(f"  ⚠️  Batch errore {r.status_code}: {r.text[:100]}")
                # Retry gara per gara
                for gara in batch:
                    try:
                        r2 = requests.post(
                            f"{SUPABASE_URL}/rest/v1/gare",
                            headers=HEADERS_SB,
                            json=[gara],
                            timeout=15
                        )
                        if r2.status_code in (200, 201, 204):
                            inserite += 1
                    except Exception as e2:
                        print(f"  ❌ Singola gara fallita: {e2}")
        except Exception as e:
            print(f"  ❌ Errore batch: {e}")
    return inserite

# ── ANAC: mappa riga CSV ───────────────────────────────────────────────────────
def riga_to_gara(r):
    importo = parse_importo(r.get("importo_complessivo_gara"))
    if importo == 0:
        importo = parse_importo(r.get("importo_lotto"))
    if importo < IMPORTO_MIN:
        return None
    scad_raw = r.get("data_scadenza_offerta", "")
    stato    = mappa_stato_anac(r.get("stato", ""), scad_raw)
    if stato not in ("attiva", "in_scadenza"):
        return None
    cpv     = r.get("cod_cpv") or ""
    oggetto = r.get("oggetto_gara") or ""
    if not cpv_ok(cpv) and not kw_ok(oggetto):
        return None
    regione = (r.get("sezione_regionale") or "").replace("SEZIONE REGIONALE ","").strip() or None
    cig     = r.get("cig") or None
    scadenza = parse_data(scad_raw)
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
        "procedura":    r.get("modalita_realizzazione") or None,
        "criterio_aggiudicazione": None,
        "importo_min":  None,
        "importo_max":  None,
        "importo_totale": round(importo, 2),
        "scadenza":     scadenza,
        "data_pubblicazione": parse_data(r.get("data_pubblicazione")),
        "stato":        stato,
        "fonte":        "ANAC",
        "url_bando":    f"https://dettaglio-cig.anticorruzione.it/cig/{cig}" if cig else None,
        "url_portale":  None,
        "id_sintel":    None,
        "codice_gara":  r.get("numero_gara") or None,
        "rup":          None,
    }

def processa_csv(raw_bytes):
    fl  = raw_bytes.split(b"\n")[0].decode("iso-8859-1", "replace")
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
            stati[g["stato"]] = stati.get(g["stato"], 0) + 1
    return righe, gare, stati

# ── ANAC ZIP mensile ───────────────────────────────────────────────────────────
def import_anac_monthly():
    """
    FIX: il delta giornaliero (cig_csv_logCsv.csv) è un log di modifiche
    senza campo importo — inutile. Questo metodo viene chiamato
    sia in modalità daily che monthly.
    """
    print("🇮🇹 ANAC ZIP MENSILE")
    try:
        r = requests.get(f"{WORKER_URL}/anac", timeout=120)
        if r.status_code != 200:
            print(f"  ❌ Worker error {r.status_code}")
            return {"fonte": "ANAC", "inserite": 0, "errore": f"Worker {r.status_code}"}

        zip_bytes = r.content
        print(f"  ✅ ZIP scaricato: {len(zip_bytes)//1024} KB")

        total_righe = 0
        total_gare  = []

        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            csv_files = [f for f in zf.namelist() if f.endswith(".csv")]
            print(f"  📦 CSV nel ZIP: {len(csv_files)}")
            for fname in csv_files:
                raw = zf.read(fname)
                righe, gare, stati = processa_csv(raw)
                total_righe += righe
                total_gare.extend(gare)
                print(f"     {fname}: {righe} righe → {len(gare)} filtrate {stati}")

        inserite = insert_batch(total_gare)
        print(f"  ✅ {inserite} gare inserite/aggiornate su {total_righe} righe totali")
        return {"fonte": "ANAC", "righe": total_righe, "filtrate": len(total_gare), "inserite": inserite}

    except Exception as e:
        print(f"  ❌ Errore ANAC: {e}")
        return {"fonte": "ANAC", "inserite": 0, "errore": str(e)}

# ── TED EU ─────────────────────────────────────────────────────────────────────
def import_ted():
    print("🇪🇺 TED EU")
    pagina  = 1
    gare    = []
    totale  = None

    while True:
        try:
            payload = {
                "query": "buyer-country=ITA AND notice-type=cn-standard",
                "scope": "ACTIVE",
                "page":  pagina,
                "limit": 250,
            }
            r = requests.post(f"{WORKER_URL}/ted", json=payload, timeout=60)

            # FIX: gestione rate limit TED
            if r.status_code == 429:
                print(f"  ⚠️  HTTP 429 a pagina {pagina} — attendo 30s")
                time.sleep(30)
                continue

            if r.status_code != 200:
                print(f"  ❌ TED error {r.status_code} a pagina {pagina}")
                break

            data = r.json()
            if totale is None:
                totale = data.get("total", 0)
                print(f"  📡 TED: {totale} notice totali")

            notices = data.get("notices", [])
            if not notices:
                break

            for notice in notices:
                pub_num    = notice.get("publication-number", "")
                titolo     = notice.get("title", {}).get("ITA") or notice.get("title", {}).get("ENG") or "(n/d)"
                ente       = notice.get("buyer", [{}])[0].get("officialName") if notice.get("buyer") else None
                cpv_list   = notice.get("cpv", [])
                cpv        = cpv_list[0].get("code","") if cpv_list else ""
                importo_val = 0
                try:
                    lots = notice.get("lots",[])
                    if lots:
                        importo_val = float(lots[0].get("estimated-value-lot", 0) or 0)
                    if importo_val == 0:
                        importo_val = float(notice.get("estimated-value", 0) or 0)
                except:
                    pass
                if importo_val < IMPORTO_MIN:
                    continue
                if not cpv_ok(cpv) and not kw_ok(titolo):
                    continue
                scad_raw = notice.get("submission-deadline","")
                scadenza = parse_data(scad_raw)
                oggi     = date.today()
                stato_ted = "attiva"
                if scad_raw:
                    try:
                        scad_d = datetime.strptime(scad_raw[:10], "%Y-%m-%d").date()
                        if scad_d < oggi:
                            stato_ted = "scaduta"
                        elif scad_d <= oggi + timedelta(days=7):
                            stato_ted = "in_scadenza"
                    except:
                        pass
                if stato_ted not in ("attiva","in_scadenza"):
                    continue
                provincia = None
                try:
                    nuts = notice.get("place-of-performance",[])
                    if nuts:
                        provincia = nuts[0].get("nutName") or nuts[0].get("nuts3")
                except:
                    pass
                html_link = f"https://ted.europa.eu/en/notice/-/detail/{pub_num}"
                pdf_link  = notice.get("links",{}).get("pdf") or html_link
                gare.append({
                    "codice_cig":   None,
                    "titolo":       titolo[:500],
                    "descrizione":  None,
                    "riassunto_ai": None,
                    "keywords_ai":  [],
                    "settore_ai":   None,
                    "ente":         ente or None,
                    "regione":      "ITALIA",
                    "provincia":    provincia,
                    "comune":       None,
                    "categoria_cpv":   cpv[:20] if cpv else None,
                    "categoria_label": None,
                    "procedura":    "Procedura aperta (EU)",
                    "criterio_aggiudicazione": None,
                    "importo_min":  None,
                    "importo_max":  None,
                    "importo_totale": round(importo_val, 2) if importo_val > 0 else None,
                    "scadenza":     scadenza,
                    "data_pubblicazione": oggi_iso(),
                    "stato":        stato_ted,
                    "fonte":        "TED_EU",
                    "url_bando":    html_link,
                    "url_portale":  pdf_link,
                    "id_sintel":    None,
                    "codice_gara":  pub_num or None,
                    "rup":          None,
                })

            pagina += 1
            # FIX: pausa 2 secondi tra le pagine per evitare rate limit
            time.sleep(2)

            if totale and pagina > (totale // 250 + 2):
                break

        except Exception as e:
            print(f"  ❌ Errore pagina {pagina}: {e}")
            break

    print(f"  📊 {len(gare)} gare attive/in_scadenza su {pagina-1} pagine")
    inserite = insert_batch(gare)
    print(f"  ✅ {inserite} gare inserite/aggiornate")
    return {"fonte": "TED_EU", "totale": totale, "pagine": pagina-1, "filtrate": len(gare), "inserite": inserite}

# ── ARIA Lombardia ─────────────────────────────────────────────────────────────
def import_aria_lombardia():
    print("🟢 ARIA LOMBARDIA — Catalogo Bandi")

    if not ARIA_CLIENT_ID or not ARIA_CLIENT_SECRET:
        print("  ⚠️  Credenziali ARIA non configurate — skip")
        return {"fonte": "ARIA_LOMBARDIA", "inserite": 0, "errore": "Credenziali mancanti"}

    BASE_URL  = "https://api.servizirl.it/c/servizi.rl/agora_catalogo/v2.0.0"
    TOKEN_URL = "https://api.servizirl.it/oauth2/token"

    # 1. Token OAuth2
    credentials = base64.b64encode(f"{ARIA_CLIENT_ID}:{ARIA_CLIENT_SECRET}".encode()).decode()
    try:
        r = requests.post(
            TOKEN_URL,
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
            return {"fonte": "ARIA_LOMBARDIA", "inserite": 0, "errore": "Token fallito"}
        print(f"  ✅ Token ottenuto")
    except Exception as e:
        print(f"  ❌ Errore token: {e}")
        return {"fonte": "ARIA_LOMBARDIA", "inserite": 0, "errore": str(e)}

    headers_api = {
        "Content-Type":  "application/json",
        "Authorization": f"Bearer {token}",
    }

    # 2. Lista bandi (paginata)
    pagina   = 1
    per_pag  = 100
    tutti    = []
    while True:
        try:
            r = requests.get(
                f"{BASE_URL}/bandi",
                headers=headers_api,
                params={"page": pagina, "size": per_pag},
                timeout=30
            )
            if r.status_code != 200:
                print(f"  ❌ Errore lista bandi p.{pagina}: HTTP {r.status_code}")
                break
            data  = r.json()
            items = data.get("content", data if isinstance(data, list) else [])
            if not items:
                break
            tutti.extend(items)
            total = data.get("totalElements", len(tutti))
            print(f"  📄 Pagina {pagina}: {len(items)} bandi (tot {len(tutti)}/{total})")
            if len(tutti) >= total:
                break
            pagina += 1
            time.sleep(0.5)
        except Exception as e:
            print(f"  ❌ Errore pagina {pagina}: {e}")
            break

    print(f"  📊 {len(tutti)} bandi trovati")

    # 3. Filtra e normalizza
    gare  = []
    oggi  = date.today()
    for b in tutti:
        stato_raw = (b.get("StatoProcedura") or b.get("stato") or "").upper()
        if any(x in stato_raw for x in ["ANNULL","REVOC","CHIUS","AGGIUD","SCAD"]):
            if "AGGIUD" not in stato_raw:
                continue
        scad_raw = b.get("DataFineRicezioneOfferte") or b.get("scadenza") or ""
        scadenza = parse_data(scad_raw)
        stato_db = "attiva"
        if scad_raw:
            try:
                scad_d = datetime.strptime(scad_raw[:10], "%Y-%m-%d").date()
                if scad_d < oggi:
                    stato_db = "scaduta"
                elif scad_d <= oggi + timedelta(days=7):
                    stato_db = "in_scadenza"
            except:
                pass
        if stato_db == "scaduta":
            continue
        importo_val = 0
        try:
            raw = b.get("ValoreEconomico") or b.get("DotazioneFinanziaria") or b.get("importo") or 0
            importo_val = float(str(raw).replace(",",".").replace(" ","") or 0)
        except:
            pass
        titolo = b.get("NomeProcedura") or b.get("titolo") or "(n/d)"
        ente   = b.get("StazioneAppaltante") or b.get("ente") or None
        cod_g  = str(b.get("CodiceProcedura") or b.get("codice_gara") or b.get("id") or "")
        id_s   = str(b.get("IdProcedura") or b.get("id_sintel") or "")
        url_p  = b.get("UrlProcedura") or b.get("url_portale") or None
        cpv    = b.get("AmbitorProcedura") or b.get("categoria_cpv") or ""
        cat_lb = b.get("CategorieMerceologiche") or b.get("categoria_label") or None
        pub_raw = b.get("DataInizioPubblicazione") or b.get("data_pubblicazione") or None
        rup    = b.get("RUP") or b.get("rup") or None
        proc   = b.get("TipoProcedura") or b.get("procedura") or None
        gare.append({
            "codice_cig":   b.get("CIG") or b.get("codice_cig") or None,
            "titolo":       titolo[:500],
            "descrizione":  b.get("Descrizione") or None,
            "riassunto_ai": None,
            "keywords_ai":  [],
            "settore_ai":   None,
            "ente":         ente,
            "regione":      "LOMBARDIA",
            "provincia":    None,
            "comune":       None,
            "categoria_cpv":   cpv[:20] if cpv else None,
            "categoria_label": cat_lb,
            "procedura":    proc,
            "criterio_aggiudicazione": None,
            "importo_min":  None,
            "importo_max":  None,
            "importo_totale": round(importo_val, 2) if importo_val > 0 else None,
            "scadenza":     scadenza,
            "data_pubblicazione": parse_data(pub_raw),
            "stato":        stato_db,
            "fonte":        "ARIA_LOMBARDIA",
            "url_bando":    url_p,
            "url_portale":  url_p,
            "id_sintel":    id_s or None,
            "codice_gara":  cod_g or None,
            "rup":          rup,
        })

    print(f"  📊 {len(gare)} bandi da inserire")
    inserite = insert_batch(gare)
    print(f"  ✅ {inserite} gare inserite/aggiornate")
    return {"fonte": "ARIA_LOMBARDIA", "totale": len(tutti), "filtrate": len(gare), "inserite": inserite}

# ── Aggiornamento stati ────────────────────────────────────────────────────────
def aggiorna_stati():
    print("🔄 Aggiornamento stati gare esistenti")
    oggi    = date.today()
    ieri    = (oggi - timedelta(days=1)).isoformat()
    tra_7gg = (oggi + timedelta(days=7)).isoformat()
    oggi_s  = oggi.isoformat()

    # attiva → scaduta
    r = requests.patch(
        f"{SUPABASE_URL}/rest/v1/gare",
        headers={**HEADERS_SB, "Prefer": "return=minimal"},
        params={"stato": "eq.attiva", "scadenza": f"lt.{oggi_s}T00:00:00+00:00"},
        json={"stato": "scaduta"},
        timeout=30
    )
    print(f"  attiva → scaduta: HTTP {r.status_code}")

    # in_scadenza → scaduta
    r = requests.patch(
        f"{SUPABASE_URL}/rest/v1/gare",
        headers={**HEADERS_SB, "Prefer": "return=minimal"},
        params={"stato": "eq.in_scadenza", "scadenza": f"lt.{oggi_s}T00:00:00+00:00"},
        json={"stato": "scaduta"},
        timeout=30
    )
    print(f"  in_scadenza → scaduta: HTTP {r.status_code}")

    # attiva → in_scadenza (scade entro 7gg)
    r = requests.patch(
        f"{SUPABASE_URL}/rest/v1/gare",
        headers={**HEADERS_SB, "Prefer": "return=minimal"},
        params={
            "stato":    "eq.attiva",
            "scadenza": f"gte.{oggi_s}T00:00:00+00:00",
            "and":      f"(scadenza.lte.{tra_7gg}T23:59:59+00:00)",
        },
        json={"stato": "in_scadenza"},
        timeout=30
    )
    print(f"  attiva → in_scadenza (entro 7gg): HTTP {r.status_code}")

    # in_scadenza → attiva (proroga)
    r = requests.patch(
        f"{SUPABASE_URL}/rest/v1/gare",
        headers={**HEADERS_SB, "Prefer": "return=minimal"},
        params={"stato": "eq.in_scadenza", "scadenza": f"gt.{tra_7gg}T23:59:59+00:00"},
        json={"stato": "attiva"},
        timeout=30
    )
    print(f"  in_scadenza → attiva (proroga): HTTP {r.status_code}")

    print("  ✅ Aggiornamento stati completato")
    return {"aggiornamento_stati": "ok"}

# ── Main ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"🚀 Gare Intelligence [{MODE.upper()}] — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    risultati = []

    # ANAC: sia daily che monthly usano il ZIP completo
    # (il delta giornaliero non ha il campo importo — inutile)
    risultati.append(import_anac_monthly())

    risultati.append(import_ted())
    risultati.append(import_aria_lombardia())
    aggiorna_stati()

    tot = sum(r.get("inserite", 0) for r in risultati)
    print(f"\n✅ TOTALE: {tot} gare inserite/aggiornate")
    print(json.dumps(risultati, indent=2, ensure_ascii=False))
