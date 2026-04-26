"""
import_anac_nazionale.py — ANAC Piattaforma Pubblicità Legale
https://pubblicitalegale.anticorruzione.it

Endpoint: GET /api/v0/avvisi
Copertura: TUTTA ITALIA — tutti i bandi dal 01/01/2024
Circa 100-200 bandi/giorno, nessuna autenticazione richiesta.

Fix 2026-04-26 v3:
- on_conflict=codice_cig quando CIG presente → evita duplicati con ARIA/TED
- on_conflict=codice_gara quando CIG null → usa UUID ANAC come chiave
- Lookup provincia → regione per popolare campo regione
- url_portale con fallback: se 409 riprova senza
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

# ── Lookup provincia NUTS → regione ───────────────────────────────────────────
# Mappa le province NUTS (come restituisce ANAC) alle regioni italiane
PROVINCIA_REGIONE = {
    # Valle d'Aosta
    "Aosta": "VALLE D'AOSTA",
    # Piemonte
    "Torino": "PIEMONTE", "Vercelli": "PIEMONTE", "Novara": "PIEMONTE",
    "Cuneo": "PIEMONTE", "Asti": "PIEMONTE", "Alessandria": "PIEMONTE",
    "Biella": "PIEMONTE", "Verbano-Cusio-Ossola": "PIEMONTE",
    # Liguria
    "Genova": "LIGURIA", "Savona": "LIGURIA", "La Spezia": "LIGURIA",
    "Imperia": "LIGURIA",
    # Lombardia
    "Milano": "LOMBARDIA", "Bergamo": "LOMBARDIA", "Brescia": "LOMBARDIA",
    "Como": "LOMBARDIA", "Cremona": "LOMBARDIA", "Lecco": "LOMBARDIA",
    "Lodi": "LOMBARDIA", "Mantova": "LOMBARDIA", "Monza e della Brianza": "LOMBARDIA",
    "Pavia": "LOMBARDIA", "Sondrio": "LOMBARDIA", "Varese": "LOMBARDIA",
    # Trentino-Alto Adige
    "Trento": "TRENTINO-ALTO ADIGE", "Bolzano": "TRENTINO-ALTO ADIGE",
    "Bozen": "TRENTINO-ALTO ADIGE",
    # Veneto
    "Venezia": "VENETO", "Verona": "VENETO", "Vicenza": "VENETO",
    "Padova": "VENETO", "Treviso": "VENETO", "Rovigo": "VENETO",
    "Belluno": "VENETO",
    # Friuli-Venezia Giulia
    "Trieste": "FRIULI-VENEZIA GIULIA", "Udine": "FRIULI-VENEZIA GIULIA",
    "Pordenone": "FRIULI-VENEZIA GIULIA", "Gorizia": "FRIULI-VENEZIA GIULIA",
    # Emilia-Romagna
    "Bologna": "EMILIA-ROMAGNA", "Modena": "EMILIA-ROMAGNA",
    "Ferrara": "EMILIA-ROMAGNA", "Ravenna": "EMILIA-ROMAGNA",
    "Forlì-Cesena": "EMILIA-ROMAGNA", "Rimini": "EMILIA-ROMAGNA",
    "Parma": "EMILIA-ROMAGNA", "Piacenza": "EMILIA-ROMAGNA",
    "Reggio nell'Emilia": "EMILIA-ROMAGNA", "Reggio Emilia": "EMILIA-ROMAGNA",
    # Toscana
    "Firenze": "TOSCANA", "Pisa": "TOSCANA", "Siena": "TOSCANA",
    "Arezzo": "TOSCANA", "Grosseto": "TOSCANA", "Livorno": "TOSCANA",
    "Lucca": "TOSCANA", "Massa-Carrara": "TOSCANA", "Pistoia": "TOSCANA",
    "Prato": "TOSCANA",
    # Umbria
    "Perugia": "UMBRIA", "Terni": "UMBRIA",
    # Marche
    "Ancona": "MARCHE", "Pesaro e Urbino": "MARCHE", "Macerata": "MARCHE",
    "Ascoli Piceno": "MARCHE", "Fermo": "MARCHE",
    # Lazio
    "Roma": "LAZIO", "Latina": "LAZIO", "Frosinone": "LAZIO",
    "Viterbo": "LAZIO", "Rieti": "LAZIO",
    # Abruzzo
    "L'Aquila": "ABRUZZO", "Pescara": "ABRUZZO", "Chieti": "ABRUZZO",
    "Teramo": "ABRUZZO",
    # Molise
    "Campobasso": "MOLISE", "Isernia": "MOLISE",
    # Campania
    "Napoli": "CAMPANIA", "Salerno": "CAMPANIA", "Caserta": "CAMPANIA",
    "Avellino": "CAMPANIA", "Benevento": "CAMPANIA",
    # Puglia
    "Bari": "PUGLIA", "Lecce": "PUGLIA", "Taranto": "PUGLIA",
    "Brindisi": "PUGLIA", "Foggia": "PUGLIA",
    "Barletta-Andria-Trani": "PUGLIA",
    # Basilicata
    "Potenza": "BASILICATA", "Matera": "BASILICATA",
    # Calabria
    "Reggio di Calabria": "CALABRIA", "Reggio Calabria": "CALABRIA",
    "Catanzaro": "CALABRIA", "Cosenza": "CALABRIA",
    "Crotone": "CALABRIA", "Vibo Valentia": "CALABRIA",
    # Sicilia
    "Palermo": "SICILIA", "Catania": "SICILIA", "Messina": "SICILIA",
    "Agrigento": "SICILIA", "Caltanissetta": "SICILIA", "Enna": "SICILIA",
    "Ragusa": "SICILIA", "Siracusa": "SICILIA", "Trapani": "SICILIA",
    # Sardegna
    "Cagliari": "SARDEGNA", "Sassari": "SARDEGNA", "Nuoro": "SARDEGNA",
    "Oristano": "SARDEGNA", "Sud Sardegna": "SARDEGNA",
    "Sassari": "SARDEGNA", "Olbia-Tempio": "SARDEGNA",
}

def provincia_to_regione(provincia):
    if not provincia:
        return None
    # Cerca corrispondenza diretta
    r = PROVINCIA_REGIONE.get(provincia)
    if r:
        return r
    # Cerca corrispondenza parziale
    prov_lower = provincia.lower()
    for k, v in PROVINCIA_REGIONE.items():
        if k.lower() in prov_lower or prov_lower in k.lower():
            return v
    return None

# ── Parser record ──────────────────────────────────────────────────────────────
def parse_record(rec):
    id_avviso = rec.get("idAvviso", "")
    data_scad = rec.get("dataScadenza", "")
    data_pub  = rec.get("dataPubblicazione", "")
    tipo      = rec.get("tipo", "avviso")

    templates = rec.get("template", [])
    if not templates:
        return None

    tmpl     = templates[0].get("template", {})
    metadata = tmpl.get("metadata", {})
    sections = tmpl.get("sections", [])

    descrizione = (metadata.get("descrizione") or "").strip()

    # SEZ. A — Ente
    ente = None
    for s in sections:
        if "SEZ. A" in s.get("name", ""):
            soggetti = s.get("fields", {}).get("soggetti_sa", [])
            if soggetti:
                ente = soggetti[0].get("denominazione_amministrazione")
            break

    # SEZ. B — URL documenti
    url_documenti = None
    for s in sections:
        if "SEZ. B" in s.get("name", ""):
            url_documenti = s.get("fields", {}).get("documenti_di_gara_link")
            break

    # SEZ. C — Primo lotto
    cig = importo_val = cpv = provincia = comune = scadenza_lotto = None
    for s in sections:
        if "SEZ. C" in s.get("name", ""):
            items = s.get("items", [])
            if items:
                lotto          = items[0]
                cig            = lotto.get("cig")
                importo_raw    = lotto.get("valore_complessivo_stimato")
                cpv            = lotto.get("cpv")
                provincia      = lotto.get("luogo_nuts")
                comune         = lotto.get("luogo_istat")
                scadenza_lotto = lotto.get("termine_ricezione") or data_scad
                if importo_raw:
                    try:
                        v = float(importo_raw)
                        if v > 0:
                            importo_val = round(v, 2)
                    except:
                        pass
            break

    # Regione dalla provincia
    regione = provincia_to_regione(provincia)

    # Normalizza scadenza
    scad_iso = scadenza_lotto or data_scad or None
    if scad_iso and "+" not in scad_iso and not scad_iso.endswith("Z"):
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

    return {
        "codice_cig":   cig,
        "titolo":       (descrizione or "(n/d)")[:500],
        "descrizione":  None,
        "riassunto_ai": None,
        "keywords_ai":  [],
        "settore_ai":   None,
        "ente":         ente,
        "regione":      regione,
        "provincia":    provincia,
        "comune":       comune,
        "categoria_cpv":   None,
        "categoria_label": cpv,
        "procedura":    tipo,
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
            print(f"  📊 {tot} bandi totali, {tot_pag} pagine (size={PAGE_SIZE})")

        if not records:
            break

        for rec in records:
            g = parse_record(rec)
            if g and g["stato"] != "scaduta":
                gare.append(g)

        pagina += 1
        if pagina >= tot_pag:
            break
        time.sleep(0.3)

    return gare

# ── Insert Supabase con deduplicazione intelligente ───────────────────────────
def insert_singolo(gara):
    """
    Inserisce una singola gara con logica di deduplicazione:
    - Se ha CIG: on_conflict=codice_cig → aggiorna record esistente da ARIA/TED
    - Se non ha CIG: on_conflict=codice_gara → usa UUID ANAC come chiave
    - Se 409 su url_portale: riprova senza url_portale
    """
    # Sceglie la chiave di conflict in base alla presenza del CIG
    if gara.get("codice_cig"):
        conflict_col = "codice_cig"
    else:
        conflict_col = "codice_gara"

    url = f"{SUPABASE_URL}/rest/v1/gare?on_conflict={conflict_col}"

    r = requests.post(url, headers=HEADERS_SB, json=[gara], timeout=15)
    if r.status_code in (200, 201, 204):
        return True, False  # (inserita, doc_perso)

    # Se 409 su url_portale, riprova senza
    if r.status_code == 409 and "url_portale" in r.text:
        gara_clean = {**gara, "url_portale": None}
        r2 = requests.post(url, headers=HEADERS_SB, json=[gara_clean], timeout=15)
        if r2.status_code in (200, 201, 204):
            return True, True  # (inserita, doc_perso)

    print(f"  ❌ Errore {r.status_code}: {r.text[:100]}")
    return False, False

def insert_batch(gare):
    """
    Prima tenta batch per CIG (bandi con CIG) e batch per codice_gara (senza CIG).
    Se un batch fallisce, ritorna al singolo con fallback.
    """
    inserite  = 0
    doc_persi = 0
    BATCH     = 50

    # Separa bandi con CIG da bandi senza CIG
    con_cig    = [g for g in gare if g.get("codice_cig")]
    senza_cig  = [g for g in gare if not g.get("codice_cig")]

    print(f"  📋 {len(con_cig)} bandi con CIG, {len(senza_cig)} senza CIG")

    # Inserisci batch con CIG
    for i in range(0, len(con_cig), BATCH):
        batch = con_cig[i:i+BATCH]
        url   = f"{SUPABASE_URL}/rest/v1/gare?on_conflict=codice_cig"
        r = requests.post(url, headers=HEADERS_SB, json=batch, timeout=30)
        if r.status_code in (200, 201, 204):
            inserite += len(batch)
        else:
            # Retry singolo con fallback
            for g in batch:
                ok, dp = insert_singolo(g)
                if ok:
                    inserite += 1
                if dp:
                    doc_persi += 1

    # Inserisci batch senza CIG
    for i in range(0, len(senza_cig), BATCH):
        batch = senza_cig[i:i+BATCH]
        url   = f"{SUPABASE_URL}/rest/v1/gare?on_conflict=codice_gara"
        r = requests.post(url, headers=HEADERS_SB, json=batch, timeout=30)
        if r.status_code in (200, 201, 204):
            inserite += len(batch)
        else:
            for g in batch:
                ok, dp = insert_singolo(g)
                if ok:
                    inserite += 1
                if dp:
                    doc_persi += 1

    if doc_persi:
        print(f"  ℹ️  {doc_persi} bandi salvati senza link documenti (url_portale già in uso)")

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
        print(f"\n📊 Statistiche:")
        print(f"  Con CIG:      {sum(1 for g in gare if g['codice_cig'])}/{len(gare)}")
        print(f"  Con importo:  {sum(1 for g in gare if g['importo_totale'])}/{len(gare)}")
        print(f"  Con scadenza: {sum(1 for g in gare if g['scadenza'])}/{len(gare)}")
        print(f"  Con provincia:{sum(1 for g in gare if g['provincia'])}/{len(gare)}")
        print(f"  Con regione:  {sum(1 for g in gare if g['regione'])}/{len(gare)}")
        print(f"  Con documenti:{sum(1 for g in gare if g['url_portale'])}/{len(gare)}")

        print(f"\n📋 Primi 5 bandi:")
        for i, g in enumerate(gare[:5], 1):
            print(f"\n  {i}. {g['titolo'][:90]}")
            print(f"     Ente:      {g['ente']}")
            print(f"     Importo:   {g['importo_totale']} €")
            print(f"     CIG:       {g['codice_cig']}")
            print(f"     Provincia: {g['provincia']}")
            print(f"     Regione:   {g['regione']}")
            print(f"     Scadenza:  {g['scadenza']}")
            print(f"     Documenti: {g['url_portale']}")

        if SUPABASE_URL and SERVICE_KEY:
            print(f"\n💾 Inserimento reale in Supabase...")
            inserite = insert_batch(gare)
            print(f"✅ {inserite} inserite")
        else:
            print(f"\n⚠️  DRY-RUN: aggiungi SUPABASE_URL e SUPABASE_SERVICE_KEY per inserire")
            print(f"\nJSON prima gara:")
            print(json.dumps(gare[0], indent=2, ensure_ascii=False))
