"""
test_campania.py — Parser completo Portale Gare Campania
Simula l'importazione senza inserire nulla in Supabase.
Stampa le gare che verrebbero inserite.

Risultati round 3:
- Struttura HTML chiara: titolo, ente, importo, scadenza tutto nella lista
- scadenzaBando è già nell'URL del link dettaglio
- Importo è nella tabella (es. 29.966.429,63€)
- codice_gara = ID nel parametro bando=ID
"""

import requests, re, json, html
from datetime import datetime, date
from urllib.parse import urlencode, urlparse, parse_qs

TIMEOUT = 20
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "it-IT,it;q=0.9",
}

BASE_URL = "https://pgt.regione.campania.it"

def strip_html(s):
    s = re.sub(r'<[^>]+>', ' ', s or '')
    s = html.unescape(s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def parse_importo(s):
    """Converte '29.966.429,63€' in float"""
    s = (s or "").replace("€","").replace(" ","").strip()
    s = s.replace(".","").replace(",",".")
    try:
        return float(s)
    except:
        return None

def parse_scadenza(s):
    """Converte '2026-05-07T13:00:00' in ISO"""
    if not s:
        return None
    s = s.strip()
    if "T" in s:
        return s + "+00:00" if "+" not in s else s
    return None

def parse_gare_da_html(html_text, pagina=1):
    """
    Parsea la tabella bandi dalla pagina HTML del portale Campania.
    Struttura tabella:
    <tr>
      <td>TITOLO</td>
      <td>TIPO (Bando/Avviso)</td>
      <td>ENTE</td>
      <td>STAZIONE APPALTANTE</td>
      <td>IMPORTO€</td>
      <td>link DETTAGLIO</td>
    </tr>
    """
    gare = []

    # Estrai tutte le righe <tr> con link getdettaglio
    # Il link dettaglio contiene bando=ID e scadenzaBando=DATA
    pattern_riga = re.compile(
        r'<tr[^>]*>(.*?)</tr>',
        re.DOTALL | re.IGNORECASE
    )
    pattern_link = re.compile(
        r'href=["\']([^"\']*getdettaglio=yes[^"\']*)["\']',
        re.IGNORECASE
    )

    for match in pattern_riga.finditer(html_text):
        riga = match.group(1)

        # La riga deve contenere un link dettaglio
        link_match = pattern_link.search(riga)
        if not link_match:
            continue

        link_rel = link_match.group(1)
        link_rel = html.unescape(link_rel)
        link_full = BASE_URL + link_rel if link_rel.startswith("/") else link_rel

        # Estrai parametri dall'URL
        parsed = parse_qs(link_rel.split("?",1)[-1])
        codice_gara = parsed.get("bando", [None])[0]
        scadenza_raw = parsed.get("scadenzaBando", [None])[0]
        tipo_bando   = parsed.get("tipobando", ["Bando"])[0]

        # Estrai celle <td>
        tds = re.findall(r'<td[^>]*>(.*?)</td>', riga, re.DOTALL | re.IGNORECASE)
        celle = [strip_html(td) for td in tds]
        celle = [c for c in celle if c]  # rimuovi celle vuote

        if len(celle) < 2:
            continue

        # Struttura celle: titolo | tipo | ente | stazione | importo | [dettaglio]
        titolo   = celle[0] if len(celle) > 0 else "(n/d)"
        ente     = celle[2] if len(celle) > 2 else (celle[1] if len(celle) > 1 else None)
        # Cerca importo: cella con € o formato numerico
        importo_val = None
        for cella in celle:
            if "€" in cella or re.search(r'\d{1,3}(?:\.\d{3})*,\d{2}', cella):
                importo_val = parse_importo(re.sub(r'[^\d.,€]','', cella))
                if importo_val and importo_val > 0:
                    break

        scadenza_iso = parse_scadenza(scadenza_raw)

        # Stato da scadenza
        stato = "attiva"
        if scadenza_raw:
            try:
                scad_d = datetime.fromisoformat(scadenza_raw[:10]).date()
                diff = (scad_d - date.today()).days
                if diff < 0:
                    stato = "scaduta"
                elif diff <= 7:
                    stato = "in_scadenza"
            except:
                pass

        gara = {
            "codice_cig":   None,
            "titolo":       titolo[:500],
            "descrizione":  None,
            "riassunto_ai": None,
            "keywords_ai":  [],
            "settore_ai":   None,
            "ente":         ente,
            "regione":      "CAMPANIA",
            "provincia":    None,
            "comune":       None,
            "categoria_cpv":   None,
            "categoria_label": tipo_bando,
            "procedura":    tipo_bando,
            "criterio_aggiudicazione": None,
            "importo_min":  None,
            "importo_max":  None,
            "importo_totale": round(importo_val, 2) if importo_val else None,
            "scadenza":     scadenza_iso,
            "data_pubblicazione": None,
            "stato":        stato,
            "fonte":        "CAMPANIA",
            "url_bando":    link_full,
            "url_portale":  link_full,
            "id_sintel":    None,
            "codice_gara":  codice_gara,
            "rup":          None,
        }
        gare.append(gara)

    return gare

def scarica_pagina(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        if r.status_code == 200:
            return r.text
        print(f"   HTTP {r.status_code}")
        return None
    except Exception as e:
        print(f"   Eccezione: {e}")
        return None

# ─── Main ──────────────────────────────────────────────────────────────────────
print(f"🚀 Test Campania Parser — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
print("NON inserisce dati — solo simulazione\n")

# 1. Scarica lista bandi non scaduti
url_lista = "https://pgt.regione.campania.it/portalegare/index.php/bandi?scaduti=no&tipobando="
print(f"📥 Scarico lista bandi: {url_lista}")
html_text = scarica_pagina(url_lista)

if not html_text:
    print("❌ Impossibile scaricare la pagina")
    exit(1)

print(f"✅ HTML scaricato: {len(html_text)/1024:.1f} KB")

# 2. Parsa le gare
gare = parse_gare_da_html(html_text)
print(f"✅ Gare parsate: {len(gare)}\n")

# 3. Mostra le gare trovate
for i, g in enumerate(gare, 1):
    print(f"{'─'*50}")
    print(f"Gara #{i}")
    print(f"  Titolo:   {g['titolo'][:100]}")
    print(f"  Ente:     {g['ente']}")
    print(f"  Importo:  {g['importo_totale']} €")
    print(f"  Scadenza: {g['scadenza']}")
    print(f"  Stato:    {g['stato']}")
    print(f"  ID gara:  {g['codice_gara']}")
    print(f"  URL:      {g['url_bando'][:80]}...")

# 4. Prova paginazione — cerca link "pagina successiva"
print(f"\n{'='*50}")
print("🔍 Controllo paginazione...")
# Cerca pattern tipici di paginazione
pag_links = re.findall(
    r'href=["\']([^"\']*(?:pagina|page|start|offset|p=)\d+[^"\']*)["\']',
    html_text, re.IGNORECASE
)
next_links = re.findall(
    r'href=["\']([^"\']+)["\'][^>]*>[^<]*(?:success|next|avanti|›|»)[^<]*<',
    html_text, re.IGNORECASE
)
print(f"Link paginazione trovati: {len(pag_links)}")
print(f"Link 'successiva' trovati: {len(next_links)}")

# Cerca anche link numerici di pagina
page_nums = re.findall(r'href=["\']([^"\']*bandi[^"\']*)["\'][^>]*>\s*(\d+)\s*<', html_text)
if page_nums:
    print(f"Link pagine numerate: {page_nums[:5]}")

# Stampa sezione paginazione dall'HTML
idx_pag = html_text.lower().find("pagination")
if idx_pag < 0:
    idx_pag = html_text.lower().find("pagina")
if idx_pag > 0:
    print(f"\nHTML sezione paginazione:\n{strip_html(html_text[idx_pag:idx_pag+300])}")

# 5. Riepilogo finale
print(f"\n{'='*50}")
print(f"✅ RIEPILOGO:")
print(f"   Gare trovate:    {len(gare)}")
print(f"   Con importo:     {sum(1 for g in gare if g['importo_totale'])}")
print(f"   Con scadenza:    {sum(1 for g in gare if g['scadenza'])}")
print(f"   Stato attiva:    {sum(1 for g in gare if g['stato']=='attiva')}")
print(f"   Stato scaduta:   {sum(1 for g in gare if g['stato']=='scaduta')}")
print(f"\nJSON pronto per Supabase (prima gara):")
if gare:
    print(json.dumps(gare[0], indent=2, ensure_ascii=False))
