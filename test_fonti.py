"""
test_fonti_finale.py — pubblicitalegale.anticorruzione.it + Campania
NON inserisce dati.
"""

import requests, json, re, html as html_module
from datetime import datetime, date, timedelta
from urllib.parse import parse_qs

TIMEOUT = 20
BASE_ANAC = "https://pubblicitalegale.anticorruzione.it"
BASE_CAMPANIA = "https://pgt.regione.campania.it"

HEADERS_BROWSER = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "it-IT,it;q=0.9",
}
HEADERS_JSON = {**HEADERS_BROWSER, "Accept": "application/json"}

def get(url, json_mode=False):
    h = HEADERS_JSON if json_mode else HEADERS_BROWSER
    try:
        r = requests.get(url, headers=h, timeout=TIMEOUT)
        return r
    except Exception as e:
        print(f"   ❌ Eccezione: {e}")
        return None

def post(url, payload):
    try:
        r = requests.post(url, headers=HEADERS_JSON, json=payload, timeout=TIMEOUT)
        return r
    except Exception as e:
        print(f"   ❌ Eccezione: {e}")
        return None

def sep(titolo):
    print(f"\n{'='*60}")
    print(f"🔍 {titolo}")

def mostra_json(r, max_chars=600):
    try:
        d = r.json()
        print(f"   ✅ JSON valido!")
        print(f"   Keys: {list(d.keys()) if isinstance(d, dict) else f'Array di {len(d)} elementi'}")
        print(f"   Contenuto: {json.dumps(d, ensure_ascii=False)[:max_chars]}")
        return d
    except:
        print(f"   Raw: {r.text[:300]}")
        return None

oggi = date.today().isoformat()
settimana_fa = (date.today() - timedelta(days=7)).isoformat()
anno = date.today().year
mese = date.today().month
mese_prec = mese - 1 if mese > 1 else 12
anno_prec = anno if mese > 1 else anno - 1

print(f"🚀 Test fonti finale — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
print("NON inserisce dati\n")

# ══════════════════════════════════════════════════════════════
# PARTE 1 — PUBBLICITALEGALE.ANTICORRUZIONE.IT
# ══════════════════════════════════════════════════════════════

print("\n" + "█"*60)
print("█ PARTE 1 — pubblicitalegale.anticorruzione.it")
print("█"*60)

sep("1.1 Homepage")
r = get(BASE_ANAC)
if r:
    print(f"   HTTP: {r.status_code}, Size: {len(r.content)/1024:.1f} KB")
    if r.status_code == 200:
        apis = re.findall(r'href=["\']([^"\']*(?:api|swagger|openapi|json|data)[^"\']*)["\']', r.text, re.IGNORECASE)
        print(f"   Link API: {apis[:5]}")

sep("1.2 Swagger / API docs")
for url in [
    f"{BASE_ANAC}/swagger-ui.html",
    f"{BASE_ANAC}/swagger-ui/",
    f"{BASE_ANAC}/v3/api-docs",
    f"{BASE_ANAC}/openapi.json",
    f"{BASE_ANAC}/api/v1",
]:
    r = get(url, json_mode=True)
    if r:
        print(f"\n   {url} → HTTP {r.status_code}")
        if r.status_code == 200:
            print(f"   ✅ Size: {len(r.content)/1024:.1f} KB")
            print(f"   Raw: {r.text[:400]}")

sep("1.3 Endpoint API bandi")
for url in [
    f"{BASE_ANAC}/api/bandi",
    f"{BASE_ANAC}/api/bandi/search",
    f"{BASE_ANAC}/api/notices",
    f"{BASE_ANAC}/api/publications",
    f"{BASE_ANAC}/rest/bandi",
    f"{BASE_ANAC}/public/api/bandi",
    f"{BASE_ANAC}/api/bandi?stato=PUBBLICATO&size=5",
    f"{BASE_ANAC}/api/bandi?dataDal={settimana_fa}&dataAl={oggi}",
]:
    r = get(url, json_mode=True)
    if r and r.status_code not in (404,):
        print(f"\n   {url}")
        print(f"   HTTP: {r.status_code}, CT: {r.headers.get('Content-Type','?')[:50]}")
        if r.status_code == 200:
            mostra_json(r)

sep("1.4 Ricerca POST")
for url in [f"{BASE_ANAC}/api/bandi/search", f"{BASE_ANAC}/api/bandi/ricerca", f"{BASE_ANAC}/api/notices/search"]:
    r = post(url, {"stato": "PUBBLICATO", "dataDal": settimana_fa, "page": 0, "size": 5})
    if r and r.status_code not in (404, 405):
        print(f"\n   {url} → HTTP {r.status_code}")
        if r.status_code == 200:
            mostra_json(r)

sep("1.5 robots.txt")
r = get(f"{BASE_ANAC}/robots.txt")
if r and r.status_code == 200:
    print(f"   {r.text[:500]}")

sep("1.6 Dataset CSV mensili dati.anticorruzione.it (BandiCIG)")
for url in [
    f"https://dati.anticorruzione.it/opendata/download/dataset/bandi-cig/formato/csv/anno/{anno}/mese/{mese:02d}",
    f"https://dati.anticorruzione.it/opendata/download/dataset/bandi-cig/formato/csv/anno/{anno}/mese/{mese_prec:02d}",
    f"https://dati.anticorruzione.it/opendata/download/dataset/bandi-cig/formato/json/anno/{anno}/mese/{mese_prec:02d}",
    f"https://dati.anticorruzione.it/opendata/dataset/bandi-cig",
    f"https://dati.anticorruzione.it/opendata/",
]:
    r = get(url)
    if r:
        print(f"\n   {url}")
        print(f"   HTTP: {r.status_code}, CT: {r.headers.get('Content-Type','?')[:50]}, Size: {len(r.content)/1024:.1f} KB")
        if r.status_code == 200:
            print(f"   ✅ ACCESSIBILE!")
            print(f"   Raw: {r.text[:300]}")

# ══════════════════════════════════════════════════════════════
# PARTE 2 — CAMPANIA
# ══════════════════════════════════════════════════════════════

print("\n\n" + "█"*60)
print("█ PARTE 2 — Portale Gare Campania")
print("█"*60)

def strip_html(s):
    s = re.sub(r'<[^>]+>', ' ', s or '')
    s = html_module.unescape(s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def parse_importo(s):
    s = (s or "").replace("€","").replace(" ","").strip()
    s = s.replace(".","").replace(",",".")
    try:
        return float(s)
    except:
        return None

def parse_scadenza(s):
    if not s: return None
    s = s.strip()
    return s + "+00:00" if "T" in s and "+" not in s else s

def stato_da_scadenza(scad_raw):
    if not scad_raw: return "attiva"
    try:
        scad_d = datetime.fromisoformat(scad_raw[:10]).date()
        diff = (scad_d - date.today()).days
        if diff < 0: return "scaduta"
        if diff <= 7: return "in_scadenza"
    except: pass
    return "attiva"

def parse_gare_campania(html_text):
    gare = []
    pattern_riga = re.compile(r'<tr[^>]*>(.*?)</tr>', re.DOTALL | re.IGNORECASE)
    pattern_link = re.compile(r'href=["\']([^"\']*getdettaglio=yes[^"\']*)["\']', re.IGNORECASE)
    for match in pattern_riga.finditer(html_text):
        riga = match.group(1)
        link_match = pattern_link.search(riga)
        if not link_match: continue
        link_rel = html_module.unescape(link_match.group(1))
        link_full = BASE_CAMPANIA + link_rel if link_rel.startswith("/") else link_rel
        parsed = parse_qs(link_rel.split("?",1)[-1])
        codice_gara = parsed.get("bando", [None])[0]
        scadenza_raw = parsed.get("scadenzaBando", [None])[0]
        tipo_bando = parsed.get("tipobando", ["Bando"])[0]
        tds = re.findall(r'<td[^>]*>(.*?)</td>', riga, re.DOTALL | re.IGNORECASE)
        celle = [strip_html(td) for td in tds]
        celle = [c for c in celle if c]
        if len(celle) < 2: continue
        titolo = celle[0]
        ente = celle[2] if len(celle) > 2 else celle[1]
        importo_val = None
        for cella in celle:
            if "€" in cella or re.search(r'\d{1,3}(?:\.\d{3})*,\d{2}', cella):
                importo_val = parse_importo(re.sub(r'[^\d.,€]','', cella))
                if importo_val and importo_val > 0: break
        gare.append({
            "codice_cig": None,
            "titolo": titolo[:500],
            "descrizione": None, "riassunto_ai": None, "keywords_ai": [], "settore_ai": None,
            "ente": ente,
            "regione": "CAMPANIA", "provincia": None, "comune": None,
            "categoria_cpv": None, "categoria_label": tipo_bando,
            "procedura": tipo_bando, "criterio_aggiudicazione": None,
            "importo_min": None, "importo_max": None,
            "importo_totale": round(importo_val, 2) if importo_val else None,
            "scadenza": parse_scadenza(scadenza_raw),
            "data_pubblicazione": None,
            "stato": stato_da_scadenza(scadenza_raw),
            "fonte": "CAMPANIA",
            "url_bando": link_full, "url_portale": link_full,
            "id_sintel": None, "codice_gara": codice_gara, "rup": None,
        })
    return gare

sep("2.1 Lista bandi attivi")
r = get(f"{BASE_CAMPANIA}/portalegare/index.php/bandi?scaduti=no&tipobando=")
if r and r.status_code == 200:
    gare = parse_gare_campania(r.text)
    print(f"   ✅ Gare trovate: {len(gare)}")
    for i, g in enumerate(gare, 1):
        print(f"\n   Gara #{i}")
        print(f"     Titolo:   {g['titolo'][:90]}")
        print(f"     Ente:     {g['ente']}")
        print(f"     Importo:  {g['importo_totale']} €")
        print(f"     Scadenza: {g['scadenza']}")
        print(f"     Stato:    {g['stato']}")
        print(f"     ID:       {g['codice_gara']}")
    print(f"\n   Riepilogo:")
    print(f"     Con importo:  {sum(1 for g in gare if g['importo_totale'])}/{len(gare)}")
    print(f"     Con scadenza: {sum(1 for g in gare if g['scadenza'])}/{len(gare)}")
    print(f"\n   JSON prima gara:")
    if gare: print(json.dumps(gare[0], indent=2, ensure_ascii=False))
else:
    print(f"   HTTP: {r.status_code if r else 'nessuna risposta'}")

sep("2.2 Paginazione (cerca altre pagine)")
if r and r.status_code == 200:
    html_text = r.text
    pag = re.findall(r'href=["\']([^"\']*bandi[^"\']*(?:pagina|page|p=|start)\d+[^"\']*)["\']', html_text, re.IGNORECASE)
    next_btn = re.findall(r'href=["\']([^"\']+)["\'][^>]*>[^<]*(?:success|next|avanti|›|»|Successiv)[^<]*<', html_text, re.IGNORECASE)
    print(f"   Link paginazione: {pag[:3]}")
    print(f"   Link 'successiva': {next_btn[:3]}")
    # Conta quante gare ci sono totali
    totale_match = re.findall(r'(\d+)\s*(?:risultati|gare|bandi|record)', html_text, re.IGNORECASE)
    print(f"   Totale risultati dichiarati: {totale_match[:5]}")

print(f"\n{'='*60}")
print("✅ Test completato")
print("\nCosa cercare nel log:")
print("  ✅ ACCESSIBILE → endpoint funzionante")
print("  ✅ JSON valido → API utilizzabile")
print("  Gare Campania → numero e qualità dati")
