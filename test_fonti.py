"""
test_fonti.py — Probe nuovi portali regionali
Esegui su GitHub Actions per vedere quali fonti sono accessibili e cosa restituiscono.

NON tocca Supabase, NON inserisce dati.
Solo GET/POST alle fonti e stampa dei risultati.

Uso:
  python test_fonti.py
"""

import requests, json, re
from datetime import datetime

TIMEOUT = 20
HEADERS_BROWSER = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "it-IT,it;q=0.9,en;q=0.8",
}

def probe(nome, url, method="GET", json_body=None, headers=None):
    h = headers or HEADERS_BROWSER
    print(f"\n{'='*60}")
    print(f"🔍 {nome}")
    print(f"   URL: {url}")
    try:
        if method == "POST":
            r = requests.post(url, json=json_body, headers=h, timeout=TIMEOUT)
        else:
            r = requests.get(url, headers=h, timeout=TIMEOUT)

        print(f"   HTTP: {r.status_code}")
        print(f"   Content-Type: {r.headers.get('Content-Type','?')}")
        print(f"   Size: {len(r.content)/1024:.1f} KB")

        ct = r.headers.get("Content-Type","")
        if r.status_code == 200:
            if "json" in ct:
                try:
                    data = r.json()
                    if isinstance(data, list):
                        print(f"   ✅ JSON array: {len(data)} elementi")
                        if data:
                            print(f"   Primo elemento keys: {list(data[0].keys())[:10]}")
                            print(f"   Primo elemento: {json.dumps(data[0], ensure_ascii=False)[:300]}")
                    elif isinstance(data, dict):
                        print(f"   ✅ JSON object keys: {list(data.keys())[:10]}")
                        print(f"   Contenuto: {json.dumps(data, ensure_ascii=False)[:300]}")
                except Exception as e:
                    print(f"   ⚠️  JSON parse error: {e}")
                    print(f"   Raw: {r.text[:200]}")
            elif "html" in ct:
                html = r.text
                # Conta link che sembrano gare
                links = re.findall(r'href=["\']([^"\']*(?:bando|gara|appalto|procedura|avviso)[^"\']*)["\']', html, re.IGNORECASE)
                titoli = re.findall(r'<(?:h[1-4]|td|li|a)[^>]*>([^<]{20,150})</(?:h[1-4]|td|li|a)>', html)
                print(f"   ✅ HTML: {len(html)} chars")
                print(f"   Link con 'gara/bando/appalto': {len(links)}")
                if links:
                    print(f"   Primi 3 link: {links[:3]}")
                print(f"   Possibili titoli ({len(titoli)} trovati):")
                for t in titoli[:5]:
                    t = t.strip()
                    if len(t) > 15:
                        print(f"     - {t[:100]}")
            else:
                print(f"   Raw: {r.text[:200]}")
        else:
            print(f"   ❌ Errore: {r.text[:200]}")

    except requests.exceptions.Timeout:
        print(f"   ❌ TIMEOUT dopo {TIMEOUT}s")
    except Exception as e:
        print(f"   ❌ Eccezione: {e}")


if __name__ == "__main__":
    print(f"🚀 Test fonti regionali — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("Questo script NON inserisce dati — solo probe delle fonti\n")

    # ── 1. Dati Lombardia Open Data (Socrata JSON API) ────────────────────────
    probe(
        "DATI LOMBARDIA — Open Data Socrata (JSON API)",
        "https://www.dati.lombardia.it/resource/cjgj-du8b.json?$limit=5",
        headers={"Accept": "application/json"}
    )

    # Prova anche senza filtro stato
    probe(
        "DATI LOMBARDIA — senza filtro stato",
        "https://www.dati.lombardia.it/resource/cjgj-du8b.json?$limit=3&$order=data_pubblicazione+DESC",
        headers={"Accept": "application/json"}
    )

    # ── 2. SATER / Intercenter Emilia-Romagna ────────────────────────────────
    probe(
        "SATER — Emilia-Romagna (Intercenter bandi)",
        "https://intercenter.regione.emilia-romagna.it/bandi-e-strumenti-di-acquisto/bandi-intercenter/bandi-e-procedure-di-gara"
    )

    probe(
        "SATER — Emilia-Romagna (bandi altri enti)",
        "https://intercenter.regione.emilia-romagna.it/servizi-imprese/bandi-altri-enti/bandi-e-avvisi-altri-enti"
    )

    # ── 3. EmPULIA — Puglia ──────────────────────────────────────────────────
    probe(
        "EmPULIA — Puglia",
        "http://www.empulia.it/tno-a/empulia/Empulia/SitePages/Bandi%20di%20gara%20new.aspx"
    )

    # ── 4. Portale Gare Campania ─────────────────────────────────────────────
    probe(
        "PORTALE GARE CAMPANIA",
        "https://pgt.regione.campania.it/portalegare/index.php/bandi"
    )

    # ── 5. START — Toscana ───────────────────────────────────────────────────
    probe(
        "START — Toscana",
        "https://start.toscana.it/bandi"
    )

    probe(
        "START — Toscana (bandi aperti)",
        "https://start.toscana.it/bandi?stato=PUBBLICATO"
    )

    # ── 6. STELLA — Piemonte ─────────────────────────────────────────────────
    probe(
        "STELLA — Piemonte",
        "https://www.regione.piemonte.it/web/temi/pubblica-amministrazione-politiche-istituzionali/appalti-gare-bandi"
    )

    # ── 7. Bonus: MIT Portale Appalti ────────────────────────────────────────
    probe(
        "MIT — Portale Appalti (ministero infrastrutture)",
        "https://portaletrasparenza.anticorruzione.it/microstrategy/html/index.htm"
    )

    # ── 8. Bonus: OpenAPPALTI / dataset CSV ANAC alternativo ─────────────────
    probe(
        "ANAC — Dataset opendata alternativo",
        "https://dati.anticorruzione.it/opendata/dataset/cig"
    )

    probe(
        "ANAC — API smartCIG (cerca bandi aperti)",
        "https://api.anticorruzione.it/apicpvt/v1.0/bandi?stato=PUBBLICATO&size=3",
        headers={"Accept": "application/json", "User-Agent": "Mozilla/5.0"}
    )

    print(f"\n{'='*60}")
    print("✅ Test completato")
