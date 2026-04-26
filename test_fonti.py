"""
test_fonti_v3.py — Round 3: dettaglio Campania + SATER senza filtri

Risultati round 2:
- Campania: HTML con gare reali (7 righe tabella, titoli visibili)
  → apriamo un link dettaglio per trovare importo/scadenza
- SATER: JSON API funziona ma review_state=published dà 1 solo bando 2013
  → proviamo senza filtro stato, con paginazione, e altri tipi

NON inserisce dati.
"""

import requests, json, re, xml.etree.ElementTree as ET
from datetime import datetime

TIMEOUT = 20
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "it-IT,it;q=0.9",
}

def get(url, accept=None, json_accept=False):
    h = dict(HEADERS)
    if json_accept:
        h["Accept"] = "application/json"
    elif accept:
        h["Accept"] = accept
    try:
        r = requests.get(url, headers=h, timeout=TIMEOUT)
        return r
    except Exception as e:
        print(f"   ❌ Eccezione: {e}")
        return None

def sep(titolo):
    print(f"\n{'='*60}")
    print(f"🔍 {titolo}")

def strip_html(s):
    s = re.sub(r'<[^>]+>', ' ', s or '')
    s = re.sub(r'&[a-z]+;', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

# ─────────────────────────────────────────────────────────────
# CAMPANIA — apri i link dettaglio trovati nell'HTML
# ─────────────────────────────────────────────────────────────

sep("CAMPANIA — Struttura lista bandi (parsing righe)")
r = get("https://pgt.regione.campania.it/portalegare/index.php/bandi?scaduti=no&tipobando=")
if r and r.status_code == 200:
    html = r.text

    # Cerca i link ai dettagli delle singole gare
    # I link tipici sono /portalegare/index.php/bandi/dettaglio/ID
    links_det = re.findall(
        r'href=["\']([^"\']*(?:dettaglio|view|bando/\d+|gara/\d+)[^"\']*)["\']',
        html, re.IGNORECASE
    )
    print(f"   Link dettaglio trovati: {len(links_det)}")
    print(f"   Esempi: {links_det[:5]}")

    # Cerca anche link con pattern /bandi/NNN
    links_num = re.findall(r'href=["\']([^"\']*\/bandi\/\d+[^"\']*)["\']', html)
    print(f"   Link /bandi/NNN: {len(links_num)} — {links_num[:3]}")

    # Cerca ID numerici nei link
    ids = re.findall(r'/bandi[/_](\d+)', html)
    print(f"   ID numerici trovati: {ids[:10]}")

    # Stampa HTML grezzo della sezione bandi (500 chars intorno a "Proc")
    idx = html.find("Proc")
    if idx > 0:
        print(f"\n   HTML intorno a 'Proc' (chars {idx-100}:{idx+800}):")
        print(html[max(0,idx-100):idx+800])

sep("CAMPANIA — Prova URL dettaglio gara")
# Prova diversi pattern di URL per il dettaglio
for url in [
    "https://pgt.regione.campania.it/portalegare/index.php/bandi/dettaglio/4297",
    "https://pgt.regione.campania.it/portalegare/index.php/bandi?id=4297",
    "https://pgt.regione.campania.it/portalegare/index.php/component/content/article/4297",
]:
    r = get(url)
    if r:
        print(f"\n   {url}")
        print(f"   HTTP: {r.status_code}, Size: {len(r.content)/1024:.1f} KB")
        if r.status_code == 200:
            html = r.text
            # Cerca importo
            importi = re.findall(r'(?:importo|base d.asta|valore|€)\s*[:\s]*([€\d.,\s]{4,20})', html, re.IGNORECASE)
            # Cerca scadenza
            scadenze = re.findall(r'(?:scadenza|termine|entro il|data)\s*[:\s]*(\d{1,2}[/\-]\d{1,2}[/\-]\d{4}|\d{4}[/\-]\d{2}[/\-]\d{2})', html, re.IGNORECASE)
            print(f"   Importi trovati: {importi[:3]}")
            print(f"   Scadenze trovate: {scadenze[:3]}")
            # Stampa testo grezzo intorno a parole chiave
            for kw in ["importo", "scadenz", "oggetto", "Proc"]:
                idx = html.lower().find(kw.lower())
                if idx > 0:
                    print(f"   Contesto '{kw}': ...{strip_html(html[idx:idx+200])}...")
                    break

# ─────────────────────────────────────────────────────────────
# SATER — prova senza filtro review_state, altri tipi
# ─────────────────────────────────────────────────────────────

sep("SATER — API senza filtro review_state (tutti i bandi)")
r = get(
    "https://intercenter.regione.emilia-romagna.it/@search?portal_type=Bando&b_size=10&sort_on=Date&sort_order=descending",
    json_accept=True
)
if r:
    print(f"   HTTP: {r.status_code}")
    if r.status_code == 200:
        try:
            d = r.json()
            print(f"   items_total: {d.get('items_total', '?')}")
            items = d.get("items", [])
            print(f"   Items ricevuti: {len(items)}")
            for it in items[:5]:
                print(f"     - [{it.get('Date','?')[:10]}] {it.get('title', it.get('@id','?'))[:80]}")
                print(f"       URL: {it.get('@id','')}")
        except Exception as e:
            print(f"   JSON error: {e} — Raw: {r.text[:200]}")

sep("SATER — API con tipo 'Procedura' o 'Gara'")
for tipo in ["Procedura", "Gara", "Notice", "Avviso"]:
    r = get(
        f"https://intercenter.regione.emilia-romagna.it/@search?portal_type={tipo}&b_size=3",
        json_accept=True
    )
    if r and r.status_code == 200:
        try:
            d = r.json()
            tot = d.get("items_total", 0)
            print(f"   portal_type={tipo}: {tot} risultati")
            if tot > 0:
                for it in d.get("items", [])[:2]:
                    print(f"     - {it.get('title', it.get('@id',''))[:80]}")
        except: pass

sep("SATER — Apri un bando da API e leggi il dettaglio")
# Prima prendo la lista, poi apro il primo item
r = get(
    "https://intercenter.regione.emilia-romagna.it/@search?portal_type=Bando&b_size=5&sort_on=Date&sort_order=descending",
    json_accept=True
)
if r and r.status_code == 200:
    try:
        d = r.json()
        items = d.get("items", [])
        print(f"   Totale bandi: {d.get('items_total','?')}, ricevuti: {len(items)}")
        for it in items[:3]:
            url_det = it.get("@id","")
            print(f"\n   Apro dettaglio: {url_det}")
            rd = get(url_det, json_accept=True)
            if rd and rd.status_code == 200:
                try:
                    det = rd.json()
                    print(f"   Keys: {list(det.keys())[:15]}")
                    # Cerca campi importo e scadenza
                    for campo in ["title","description","scadenza","importo","text","effective","expires","subjects","start","end"]:
                        v = det.get(campo)
                        if v:
                            print(f"   {campo}: {str(v)[:150]}")
                except:
                    # Prova HTML
                    html = rd.text
                    importi = re.findall(r'(?:importo|base d.asta|valore)[^€\d]*([€\d.,]{5,20})', html, re.IGNORECASE)
                    scadenze = re.findall(r'\d{1,2}[/\-]\d{1,2}[/\-]\d{4}', html)
                    print(f"   HTML — importi: {importi[:2]}, scadenze: {scadenze[:3]}")
            break  # solo il primo per ora
    except Exception as e:
        print(f"   Errore: {e}")

sep("SATER — Prova paginazione API (batch 10, pagina 2)")
r = get(
    "https://intercenter.regione.emilia-romagna.it/@search?portal_type=Bando&b_size=10&b_start=0",
    json_accept=True
)
if r and r.status_code == 200:
    try:
        d = r.json()
        print(f"   Totale: {d.get('items_total','?')}")
        print(f"   Batches disponibili: {d.get('batching', {})}")
        # Prova b_start=10
        r2 = get(
            "https://intercenter.regione.emilia-romagna.it/@search?portal_type=Bando&b_size=10&b_start=10",
            json_accept=True
        )
        if r2 and r2.status_code == 200:
            d2 = r2.json()
            print(f"   Pagina 2 items: {len(d2.get('items',[]))}")
    except Exception as e:
        print(f"   Errore: {e}")

print(f"\n{'='*60}")
print("✅ Test v3 completato")
