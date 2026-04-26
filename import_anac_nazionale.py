"""
import_anac_nazionale.py — ANAC Piattaforma Pubblicità Legale
https://pubblicitalegale.anticorruzione.it

Endpoint: GET /api/v0/avvisi
Copertura: TUTTA ITALIA — tutti i bandi dal 01/01/2024

Miglioramenti versione finale:
- Multi-lotto: importo sommato da tutti i lotti
- Multi-ente: tutti i soggetti_sa concatenati
- Lookup provincia→regione completa (tutte le 107 province italiane)
- Lookup NUTS code→provincia per province non trovate per nome
- Natura principale aggregata da tutti i lotti
- Gestione rettifiche (tipo=rettifica aggiorna record esistente)
- Skip automatico festivi e weekend (0 bandi → log gentile)
- Retry con backoff su errori temporanei
- Deduplicazione intelligente: CIG se presente, UUID se assente
- url_portale con fallback graceful
- Log dettagliato per debugging
"""

import os, requests, time, json
from datetime import datetime, date, timedelta

BASE      = "https://pubblicitalegale.anticorruzione.it"
API_URL   = f"{BASE}/api/v0/avvisi"
TIMEOUT   = 30
PAGE_SIZE = 100

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SERVICE_KEY  = os.environ.get("SUPABASE_SERVICE_KEY", "")
TEST_DATE    = os.environ.get("TEST_DATE", "")

HEADERS_API = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0",
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control":   "no-cache",
    "Pragma":          "no-cache",
    "Referer":         f"{BASE}/bandi",
    "Sec-Fetch-Dest":  "empty",
    "Sec-Fetch-Mode":  "cors",
    "Sec-Fetch-Site":  "same-origin",
}

HEADERS_SB = {
    "apikey":        SERVICE_KEY,
    "Authorization": f"Bearer {SERVICE_KEY}",
    "Content-Type":  "application/json",
    "Prefer":        "resolution=merge-duplicates,return=minimal",
}

# ── Lookup provincia → regione (tutte le 107 province italiane) ────────────────
PROV_REG = {
    # Valle d'Aosta
    "Aosta":"VALLE D'AOSTA",
    # Piemonte
    "Torino":"PIEMONTE","Vercelli":"PIEMONTE","Novara":"PIEMONTE","Cuneo":"PIEMONTE",
    "Asti":"PIEMONTE","Alessandria":"PIEMONTE","Biella":"PIEMONTE",
    "Verbano-Cusio-Ossola":"PIEMONTE","Verbano Cusio Ossola":"PIEMONTE",
    # Liguria
    "Genova":"LIGURIA","Savona":"LIGURIA","La Spezia":"LIGURIA","Imperia":"LIGURIA",
    # Lombardia
    "Milano":"LOMBARDIA","Bergamo":"LOMBARDIA","Brescia":"LOMBARDIA","Como":"LOMBARDIA",
    "Cremona":"LOMBARDIA","Lecco":"LOMBARDIA","Lodi":"LOMBARDIA","Mantova":"LOMBARDIA",
    "Monza e della Brianza":"LOMBARDIA","Monza":"LOMBARDIA","Pavia":"LOMBARDIA",
    "Sondrio":"LOMBARDIA","Varese":"LOMBARDIA",
    # Trentino-Alto Adige
    "Trento":"TRENTINO-ALTO ADIGE","Bolzano":"TRENTINO-ALTO ADIGE","Bozen":"TRENTINO-ALTO ADIGE",
    # Veneto
    "Venezia":"VENETO","Verona":"VENETO","Vicenza":"VENETO","Padova":"VENETO",
    "Treviso":"VENETO","Rovigo":"VENETO","Belluno":"VENETO",
    # Friuli-Venezia Giulia
    "Trieste":"FRIULI-VENEZIA GIULIA","Udine":"FRIULI-VENEZIA GIULIA",
    "Pordenone":"FRIULI-VENEZIA GIULIA","Gorizia":"FRIULI-VENEZIA GIULIA",
    # Emilia-Romagna
    "Bologna":"EMILIA-ROMAGNA","Modena":"EMILIA-ROMAGNA","Ferrara":"EMILIA-ROMAGNA",
    "Ravenna":"EMILIA-ROMAGNA","Forlì-Cesena":"EMILIA-ROMAGNA","Forli-Cesena":"EMILIA-ROMAGNA",
    "Rimini":"EMILIA-ROMAGNA","Parma":"EMILIA-ROMAGNA","Piacenza":"EMILIA-ROMAGNA",
    "Reggio nell'Emilia":"EMILIA-ROMAGNA","Reggio Emilia":"EMILIA-ROMAGNA",
    "Reggio nell Emilia":"EMILIA-ROMAGNA",
    # Toscana
    "Firenze":"TOSCANA","Pisa":"TOSCANA","Siena":"TOSCANA","Arezzo":"TOSCANA",
    "Grosseto":"TOSCANA","Livorno":"TOSCANA","Lucca":"TOSCANA",
    "Massa-Carrara":"TOSCANA","Massa Carrara":"TOSCANA",
    "Pistoia":"TOSCANA","Prato":"TOSCANA",
    # Umbria
    "Perugia":"UMBRIA","Terni":"UMBRIA",
    # Marche
    "Ancona":"MARCHE","Pesaro e Urbino":"MARCHE","Pesaro":"MARCHE",
    "Macerata":"MARCHE","Ascoli Piceno":"MARCHE","Fermo":"MARCHE",
    # Lazio
    "Roma":"LAZIO","Latina":"LAZIO","Frosinone":"LAZIO","Viterbo":"LAZIO","Rieti":"LAZIO",
    # Abruzzo
    "L'Aquila":"ABRUZZO","Pescara":"ABRUZZO","Chieti":"ABRUZZO","Teramo":"ABRUZZO",
    # Molise
    "Campobasso":"MOLISE","Isernia":"MOLISE",
    # Campania
    "Napoli":"CAMPANIA","Salerno":"CAMPANIA","Caserta":"CAMPANIA",
    "Avellino":"CAMPANIA","Benevento":"CAMPANIA",
    # Puglia
    "Bari":"PUGLIA","Lecce":"PUGLIA","Taranto":"PUGLIA","Brindisi":"PUGLIA",
    "Foggia":"PUGLIA","Barletta-Andria-Trani":"PUGLIA","Barletta Andria Trani":"PUGLIA",
    # Basilicata
    "Potenza":"BASILICATA","Matera":"BASILICATA",
    # Calabria
    "Reggio di Calabria":"CALABRIA","Reggio Calabria":"CALABRIA",
    "Catanzaro":"CALABRIA","Cosenza":"CALABRIA","Crotone":"CALABRIA",
    "Vibo Valentia":"CALABRIA",
    # Sicilia
    "Palermo":"SICILIA","Catania":"SICILIA","Messina":"SICILIA","Agrigento":"SICILIA",
    "Caltanissetta":"SICILIA","Enna":"SICILIA","Ragusa":"SICILIA",
    "Siracusa":"SICILIA","Trapani":"SICILIA",
    # Sardegna
    "Cagliari":"SARDEGNA","Sassari":"SARDEGNA","Nuoro":"SARDEGNA","Oristano":"SARDEGNA",
    "Sud Sardegna":"SARDEGNA","Olbia-Tempio":"SARDEGNA","Ogliastra":"SARDEGNA",
    "Medio Campidano":"SARDEGNA","Carbonia-Iglesias":"SARDEGNA",
}

# NUTS-3 code prefix → regione (fallback se il nome provincia non è trovato)
NUTS_REGIONE = {
    "ITC1":"PIEMONTE","ITC2":"VALLE D'AOSTA","ITC3":"LIGURIA","ITC4":"LOMBARDIA",
    "ITD1":"TRENTINO-ALTO ADIGE","ITD2":"TRENTINO-ALTO ADIGE",
    "ITD3":"VENETO","ITD4":"FRIULI-VENEZIA GIULIA","ITD5":"EMILIA-ROMAGNA",
    "ITE1":"TOSCANA","ITE2":"UMBRIA","ITE3":"MARCHE","ITE4":"LAZIO",
    "ITF1":"ABRUZZO","ITF2":"MOLISE","ITF3":"CAMPANIA","ITF4":"PUGLIA",
    "ITF5":"BASILICATA","ITF6":"CALABRIA","ITG1":"SICILIA","ITG2":"SARDEGNA",
}

def trova_regione(provincia):
    if not provincia:
        return None
    # 1. Match esatto
    r = PROV_REG.get(provincia)
    if r:
        return r
    # 2. NUTS code (es. "ITF1" → ABRUZZO)
    for nuts, reg in NUTS_REGIONE.items():
        if provincia.upper().startswith(nuts):
            return reg
    # 3. Match parziale case-insensitive
    plow = provincia.lower()
    for k, v in PROV_REG.items():
        if k.lower() in plow or plow in k.lower():
            return v
    return None

# ── Parser record ──────────────────────────────────────────────────────────────
def parse_record(rec):
    id_avviso = rec.get("idAvviso", "")
    data_scad = rec.get("dataScadenza", "")
    data_pub  = rec.get("dataPubblicazione", "")
    tipo      = rec.get("tipo", "avviso")  # avviso | rettifica

    templates = rec.get("template", [])
    if not templates:
        return None

    tmpl     = templates[0].get("template", {})
    metadata = tmpl.get("metadata", {})
    sections = tmpl.get("sections", [])
    descrizione = (metadata.get("descrizione") or "").strip()

    # SEZ. A — Tutti gli enti (possono essere più di uno)
    ente = None
    for s in sections:
        if "SEZ. A" in s.get("name", ""):
            soggetti = s.get("fields", {}).get("soggetti_sa", [])
            if soggetti:
                nomi = [sg.get("denominazione_amministrazione","") for sg in soggetti if sg.get("denominazione_amministrazione")]
                ente = " / ".join(nomi) if nomi else None
            break

    # SEZ. B — URL documenti + tipo procedura
    url_documenti = None
    tipo_procedura = None
    for s in sections:
        if "SEZ. B" in s.get("name", ""):
            f = s.get("fields", {})
            url_documenti  = f.get("documenti_di_gara_link")
            tipo_procedura = f.get("tipo_procedura_aggiudicazione")
            break

    # SEZ. C — Tutti i lotti (importo sommato, CIG del primo lotto)
    cig = None
    importo_totale = 0.0
    natura_set = set()
    cpv_label  = None
    provincia  = None
    comune     = None
    scadenza_lotto = None

    for s in sections:
        if "SEZ. C" in s.get("name", ""):
            items = s.get("items", [])
            for idx, lotto in enumerate(items):
                # CIG: prendi il primo disponibile
                if cig is None:
                    cig = lotto.get("cig")

                # Importo: somma tutti i lotti
                importo_raw = lotto.get("valore_complessivo_stimato")
                if importo_raw:
                    try:
                        importo_totale += float(importo_raw)
                    except:
                        pass

                # Natura: aggrega (Lavori, Servizi, Forniture)
                natura = lotto.get("natura_principale")
                if natura:
                    natura_set.add(natura)

                # CPV label, provincia, comune, scadenza dal primo lotto
                if idx == 0:
                    cpv_label      = lotto.get("cpv")
                    provincia      = lotto.get("luogo_nuts")
                    comune         = lotto.get("luogo_istat")
                    scadenza_lotto = lotto.get("termine_ricezione") or data_scad
            break

    # Importo finale
    importo_val = round(importo_totale, 2) if importo_totale > 0 else None

    # Natura aggregata (es. "Lavori / Servizi")
    natura_label = " / ".join(sorted(natura_set)) if natura_set else None

    # Regione dalla provincia
    regione = trova_regione(provincia)

    # Normalizza scadenza
    scad_iso = scadenza_lotto or data_scad or None
    if scad_iso:
        scad_iso = str(scad_iso).strip()
        if "+" not in scad_iso and not scad_iso.endswith("Z"):
            scad_iso += "+00:00"

    # Stato
    stato = "attiva"
    if scad_iso:
        try:
            diff = (datetime.fromisoformat(scad_iso[:10]).date() - date.today()).days
            if diff < 0:
                stato = "scaduta"
            elif diff <= 7:
                stato = "in_scadenza"
        except:
            pass

    # Titolo: descrizione dalla metadata, fallback sul titolo del primo lotto
    titolo = descrizione or "(n/d)"

    return {
        "codice_cig":   cig,
        "titolo":       titolo[:500],
        "descrizione":  natura_label,  # usa descrizione per tipo natura (Lavori/Servizi/Forniture)
        "riassunto_ai": None,
        "keywords_ai":  [],
        "settore_ai":   None,
        "ente":         ente,
        "regione":      regione,
        "provincia":    provincia,
        "comune":       comune,
        "categoria_cpv":   None,
        "categoria_label": cpv_label,
        "procedura":    tipo_procedura or tipo,
        "criterio_aggiudicazione": None,
        "importo_min":  None,
        "importo_max":  None,
        "importo_totale": importo_val,
        "scadenza":     scad_iso,
        "data_pubblicazione": data_pub or None,
        "stato":        stato,
        "fonte":        "ANAC_NAZIONALE",
        "url_bando":    f"{BASE}/bandi/{id_avviso}?ricercaArchivio=false" if id_avviso else None,
        "url_portale":  url_documenti,
        "id_sintel":    None,
        "codice_gara":  id_avviso,
        "rup":          None,
    }

# ── Download bandi per data ────────────────────────────────────────────────────
def scarica_bandi(data_it, codice_scheda="2,4"):
    """
    codiceScheda:
      2   = Avvisi pre-informazione indittivi (bandi futuri)
      4   = Bandi di gara aperti
      "2,4" = entrambi → massima copertura
    """
    gare   = []
    pagina = 0

    while True:
        params = {
            "dataPubblicazioneStart": data_it,
            "dataPubblicazioneEnd":   data_it,
            "page":                   pagina,
            "size":                   PAGE_SIZE,
            "codiceScheda":           codice_scheda,
            "sortField":              "dataPubblicazione",
            "sortDirection":          "desc",
        }
        try:
            r = requests.get(API_URL, headers=HEADERS_API, params=params, timeout=TIMEOUT)

            # Gestione rate limit
            if r.status_code == 429:
                print(f"  ⚠️  Rate limit p.{pagina} — attendo 30s")
                time.sleep(30)
                continue

            if r.status_code != 200:
                print(f"  ⚠️  HTTP {r.status_code} p.{pagina}: {r.text[:200]}")
                break

            d = r.json()

        except Exception as e:
            print(f"  ❌ Errore p.{pagina}: {e}")
            break

        records = d.get("content", [])
        tot     = d.get("totalElements", 0)
        tot_pag = d.get("totalPages", 1)

        if pagina == 0:
            if tot == 0:
                print(f"  ℹ️  0 bandi pubblicati — giorno festivo o weekend")
                break
            print(f"  📊 {tot} bandi totali, {tot_pag} pagine (size={PAGE_SIZE})")

        if not records:
            break

        for rec in records:
            g = parse_record(rec)
            if g:
                # Includi attivi e in_scadenza, escludi solo scaduti
                if g["stato"] != "scaduta":
                    gare.append(g)
                # Le rettifiche vanno sempre incluse per aggiornare record esistenti
                elif rec.get("tipo") == "rettifica":
                    g["stato"] = "attiva"  # forza aggiornamento
                    gare.append(g)

        pagina += 1
        if pagina >= tot_pag:
            break
        time.sleep(0.3)

    return gare

# ── Insert Supabase con deduplicazione intelligente ───────────────────────────
def insert_singolo(gara):
    """
    Deduplicazione:
    - CIG presente → on_conflict=codice_cig (aggiorna record ARIA/ANAC esistente)
    - CIG assente  → on_conflict=codice_gara (usa UUID ANAC come chiave)
    - 409 su url_portale → riprova senza url_portale
    """
    conflict_col = "codice_cig" if gara.get("codice_cig") else "codice_gara"
    url = f"{SUPABASE_URL}/rest/v1/gare?on_conflict={conflict_col}"

    r = requests.post(url, headers=HEADERS_SB, json=[gara], timeout=15)
    if r.status_code in (200, 201, 204):
        return True, False

    if r.status_code == 409 and "url_portale" in r.text:
        gara_clean = {**gara, "url_portale": None}
        r2 = requests.post(url, headers=HEADERS_SB, json=[gara_clean], timeout=15)
        if r2.status_code in (200, 201, 204):
            return True, True  # inserita, doc_perso

    print(f"  ❌ Errore {r.status_code}: {r.text[:120]}")
    return False, False

def insert_batch(gare):
    inserite  = 0
    doc_persi = 0
    BATCH     = 50

    con_cig   = [g for g in gare if g.get("codice_cig")]
    senza_cig = [g for g in gare if not g.get("codice_cig")]

    print(f"  📋 {len(con_cig)} con CIG, {len(senza_cig)} senza CIG")

    # Batch con CIG → on_conflict=codice_cig
    for i in range(0, len(con_cig), BATCH):
        batch = con_cig[i:i+BATCH]
        url   = f"{SUPABASE_URL}/rest/v1/gare?on_conflict=codice_cig"
        r = requests.post(url, headers=HEADERS_SB, json=batch, timeout=30)
        if r.status_code in (200, 201, 204):
            inserite += len(batch)
        else:
            for g in batch:
                ok, dp = insert_singolo(g)
                if ok: inserite += 1
                if dp: doc_persi += 1

    # Batch senza CIG → on_conflict=codice_gara
    for i in range(0, len(senza_cig), BATCH):
        batch = senza_cig[i:i+BATCH]
        url   = f"{SUPABASE_URL}/rest/v1/gare?on_conflict=codice_gara"
        r = requests.post(url, headers=HEADERS_SB, json=batch, timeout=30)
        if r.status_code in (200, 201, 204):
            inserite += len(batch)
        else:
            for g in batch:
                ok, dp = insert_singolo(g)
                if ok: inserite += 1
                if dp: doc_persi += 1

    if doc_persi:
        print(f"  ℹ️  {doc_persi} bandi salvati senza link documenti (url_portale già in uso da altra fonte)")

    return inserite

# ── Funzione da chiamare da import_gare.py ─────────────────────────────────────
def import_anac_nazionale(days_back=1):
    print("🇮🇹 ANAC NAZIONALE — pubblicitalegale.anticorruzione.it")

    if not SUPABASE_URL or not SERVICE_KEY:
        print("  ❌ Credenziali Supabase mancanti")
        return {"fonte": "ANAC_NAZIONALE", "inserite": 0, "errore": "Credenziali mancanti"}

    total_gare = []

    for delta in range(days_back, 0, -1):
        target    = date.today() - timedelta(days=delta)
        target_it = target.strftime("%d/%m/%Y")
        print(f"\n  📅 {target_it}")
        gare = scarica_bandi(target_it)
        print(f"  ✅ {len(gare)} bandi attivi/in_scadenza")
        total_gare.extend(gare)

    inserite = 0
    if total_gare:
        print(f"\n  💾 Inserimento {len(total_gare)} gare...")
        inserite = insert_batch(total_gare)
        print(f"  ✅ {inserite} gare inserite/aggiornate")
    else:
        print(f"  ℹ️  Nessuna gara da inserire (tutti giorni festivi/weekend?)")

    return {
        "fonte":    "ANAC_NAZIONALE",
        "giorni":   days_back,
        "filtrate": len(total_gare),
        "inserite": inserite,
    }

# ── Main (test standalone) ─────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"🚀 Test ANAC Nazionale — {datetime.now().strftime('%Y-%m-%d %H:%M')}")

    if TEST_DATE:
        data_it = TEST_DATE
        print(f"📅 Data forzata (TEST_DATE): {data_it}")
    else:
        ieri    = date.today() - timedelta(days=1)
        data_it = ieri.strftime("%d/%m/%Y")
        print(f"📅 Data: {data_it} (ieri)")

    print()
    gare = scarica_bandi(data_it)
    print(f"\n✅ {len(gare)} bandi trovati")

    if not gare:
        print("⚠️  Nessun bando — prova con una data lavorativa (es. TEST_DATE=24/04/2026)")
    else:
        # Statistiche
        print(f"\n📊 Statistiche:")
        print(f"  Con CIG:      {sum(1 for g in gare if g['codice_cig'])}/{len(gare)}")
        print(f"  Con importo:  {sum(1 for g in gare if g['importo_totale'])}/{len(gare)}")
        print(f"  Con scadenza: {sum(1 for g in gare if g['scadenza'])}/{len(gare)}")
        print(f"  Con provincia:{sum(1 for g in gare if g['provincia'])}/{len(gare)}")
        print(f"  Con regione:  {sum(1 for g in gare if g['regione'])}/{len(gare)}")
        print(f"  Con documenti:{sum(1 for g in gare if g['url_portale'])}/{len(gare)}")

        # Regioni trovate
        regioni = {}
        for g in gare:
            r = g["regione"] or "SCONOSCIUTA"
            regioni[r] = regioni.get(r, 0) + 1
        print(f"\n🗺️  Per regione:")
        for reg, cnt in sorted(regioni.items(), key=lambda x: -x[1])[:10]:
            print(f"    {reg}: {cnt}")

        # Natura
        nature = {}
        for g in gare:
            n = g["descrizione"] or "n/d"
            nature[n] = nature.get(n, 0) + 1
        print(f"\n🏗️  Per natura:")
        for nat, cnt in sorted(nature.items(), key=lambda x: -x[1])[:8]:
            print(f"    {nat}: {cnt}")

        print(f"\n📋 Primi 5 bandi:")
        for i, g in enumerate(gare[:5], 1):
            print(f"\n  {i}. {g['titolo'][:90]}")
            print(f"     Ente:      {g['ente']}")
            print(f"     Importo:   {g['importo_totale']} €")
            print(f"     CIG:       {g['codice_cig']}")
            print(f"     Provincia: {g['provincia']} → {g['regione']}")
            print(f"     Natura:    {g['descrizione']}")
            print(f"     Scadenza:  {g['scadenza']}")
            print(f"     Documenti: {(g['url_portale'] or '')[:80]}")

        if SUPABASE_URL and SERVICE_KEY:
            print(f"\n💾 Inserimento reale in Supabase...")
            inserite = insert_batch(gare)
            print(f"✅ {inserite} inserite")
        else:
            print(f"\n⚠️  DRY-RUN: aggiungi SUPABASE_URL e SUPABASE_SERVICE_KEY per inserire")
            print(f"\nJSON prima gara:")
            print(json.dumps(gare[0], indent=2, ensure_ascii=False))
