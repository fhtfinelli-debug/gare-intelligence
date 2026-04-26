"""
import_anac_nazionale.py — ANAC Piattaforma Pubblicità Legale
https://pubblicitalegale.anticorruzione.it

Endpoint: GET /api/v0/avvisi
Copertura: TUTTA ITALIA — tutti i bandi dal 01/01/2024
Circa 100-200 bandi/giorno, nessuna autenticazione richiesta.

Fix 2026-04-26:
- Parsing SEZ. C: usare "SEZ. C" non "C" (Committente contiene C → falso match)
- url_portale: inserito con fallback — se 409 riprova senza url_portale
- on_conflict: codice_gara (idAvviso UUID, sempre unico)
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

    # SEZ. C — Primo lotto (CIG, importo, CPV, luogo, scadenza)
    # FIX: "SEZ. C" non "C" — "Committente" in SEZ. A contiene la lettera C
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
        "regione":      None,
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
        "url_portale":  url_documenti,  # link ai documenti di gara
        "id_sintel":    None,
        "codice_gara":  id_avviso,  # idAvviso UUID — sempre unico
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

# ── Insert Supabase ────────────────────────────────────────────────────────────
def insert_singolo(url, gara):
    r = requests.post(url, headers=HEADERS_SB, json=[gara], timeout=15)
    if r.status_code in (200, 201, 204):
        return True
    # Se 409 su url_portale, riprova senza — il bando viene salvato con url_bando ANAC
    if r.status_code == 409 and "url_portale" in r.text:
        gara_clean = {**gara, "url_portale": None}
        r2 = requests.post(url, headers=HEADERS_SB, json=[gara_clean], timeout=15)
        return r2.status_code in (200, 201, 204)
    return False

def insert_batch(gare):
    inserite = 0
    doc_persi = 0
    BATCH = 50
    url = f"{SUPABASE_URL}/rest/v1/gare?on_conflict=codice_gara"

    for i in range(0, len(gare), BATCH):
        batch = gare[i:i+BATCH]
        r = requests.post(url, headers=HEADERS_SB, json=batch, timeout=30)
        if r.status_code in (200, 201, 204):
            inserite += len(batch)
        else:
            # Retry singolarmente con fallback su url_portale
            for gara in batch:
                r2 = requests.post(url, headers=HEADERS_SB, json=[gara], timeout=15)
                if r2.status_code in (200, 201, 204):
                    inserite += 1
                elif r2.status_code == 409 and "url_portale" in r2.text:
                    # url_portale già usato da altro record (es. ARIA/TED)
                    # Salva senza url_portale — url_bando ANAC rimane
                    gara_clean = {**gara, "url_portale": None}
                    r3 = requests.post(url, headers=HEADERS_SB, json=[gara_clean], timeout=15)
                    if r3.status_code in (200, 201, 204):
                        inserite += 1
                        doc_persi += 1
                    else:
                        print(f"  ❌ Fallita anche senza url_portale: {r3.status_code}")
                else:
                    print(f"  ❌ Errore {r2.status_code}: {r2.text[:100]}")

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
        print(f"  Con documenti:{sum(1 for g in gare if g['url_portale'])}/{len(gare)}")

        print(f"\n📋 Primi 5 bandi:")
        for i, g in enumerate(gare[:5], 1):
            print(f"\n  {i}. {g['titolo'][:90]}")
            print(f"     Ente:      {g['ente']}")
            print(f"     Importo:   {g['importo_totale']} €")
            print(f"     CIG:       {g['codice_cig']}")
            print(f"     Provincia: {g['provincia']}")
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
