"""
test_fonti_v2.py — Approfondimento SATER (Emilia-Romagna) e Campania
Risultati round 1:
- Dati Lombardia: 403 login richiesto
- EmPULIA: redirect a autenticazione
- START Toscana / STELLA Piemonte: 404
- SATER: 200 ✅ — cercare endpoint JSON/RSS
- Campania: 200 ✅ — trovato link RSS nell'HTML

NON inserisce dati.
"""

import requests, json, re, xml.etree.ElementTree as ET
from datetime import datetime

TIMEOUT = 20
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "it-IT,it;q=0.9",
}

def get(url, accept=None):
    h = dict(HEADERS)
    if accept:
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

# ─────────────────────────────────────────────────────────────
# CAMPANIA — prova il feed RSS trovato nell'HTML
# ─────────────────────────────────────────────────────────────
sep("CAMPANIA — Feed RSS diretto")
rss_url = "https://pgt.regione.campania.it/portalegare/modules/mod_rss_aflink/mod_rss_aflink.php?modalericerca=yes&chiamante=https%3A%2F%2Fpgt.regione.campania.it%2Fportalegare%2Findex.php%2Fbandi%3F&desc_prot=Protocollo&COL_DATA=DtScadenzaBandoTecnical&COL_DESC=Oggetto&COL_PROTOCOLLO=RegistroSistema&FILE_CSS=https://pgt.regione.campania.it/portalegare/templates/aflinktemplate3/css/aflink_style.css&PATH_CSS=https://pgt.regione.campania.it/portalegare/templates/aflinktemplate3/css&hidden_field=&add_field=&TIPO_FILTRO_ENTE="
print(f"   URL: {rss_url[:80]}...")
r = get(rss_url, accept="application/rss+xml,application/xml,text/xml,*/*")
if r:
    print(f"   HTTP: {r.status_code}")
    print(f"   Content-Type: {r.headers.get('Content-Type','?')}")
    print(f"   Size: {len(r.content)/1024:.1f} KB")
    if r.status_code == 200:
        ct = r.headers.get("Content-Type","")
        print(f"   Raw (primi 500 chars):\n{r.text[:500]}")
        # Prova a parsare come XML/RSS
        try:
            root = ET.fromstring(r.content)
            print(f"\n   ✅ XML valido! Tag root: {root.tag}")
            # Cerca items RSS
            items = root.findall(".//item")
            print(f"   Items RSS trovati: {len(items)}")
            for item in items[:3]:
                title = item.findtext("title") or ""
                link  = item.findtext("link") or ""
                desc  = item.findtext("description") or ""
                print(f"\n   Item:")
                print(f"     Titolo: {title[:100]}")
                print(f"     Link: {link[:100]}")
                print(f"     Desc: {desc[:100]}")
        except Exception as e:
            print(f"   XML parse error: {e}")

sep("CAMPANIA — Lista bandi HTML (bandi non scaduti)")
r = get("https://pgt.regione.campania.it/portalegare/index.php/bandi?scaduti=no&tipobando=")
if r:
    print(f"   HTTP: {r.status_code}, Size: {len(r.content)/1024:.1f} KB")
    if r.status_code == 200:
        html = r.text
        # Cerca pattern di gare nella pagina
        # Cerca righe tabella con dati
        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', html, re.DOTALL)
        print(f"   Righe tabella trovate: {len(rows)}")
        for row in rows[:5]:
            tds = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
            if tds and len(tds) > 1:
                testo = [re.sub(r'<[^>]+>', '', td).strip() for td in tds]
                testo = [t for t in testo if t and len(t) > 2]
                if testo:
                    print(f"     Riga: {' | '.join(testo[:5])[:150]}")
        # Cerca link a singole gare
        links_gare = re.findall(r'href=["\']([^"\']*(?:dettaglio|view|bando)[^"\']*)["\']', html, re.IGNORECASE)
        print(f"   Link a dettaglio gara: {len(links_gare)}")
        if links_gare:
            print(f"   Primi: {links_gare[:3]}")

# ─────────────────────────────────────────────────────────────
# SATER / INTERCENTER — prova vari endpoint JSON/RSS/API
# ─────────────────────────────────────────────────────────────
sep("SATER — Prova endpoint JSON API")
# Intercenter usa Plone CMS — spesso ha endpoint /@search JSON
r = get("https://intercenter.regione.emilia-romagna.it/@search?portal_type=Bando&review_state=published&sort_on=Date&sort_order=descending&b_size=5",
        accept="application/json")
if r:
    print(f"   HTTP: {r.status_code}, CT: {r.headers.get('Content-Type','?')[:50]}")
    if r.status_code == 200 and "json" in r.headers.get("Content-Type",""):
        try:
            d = r.json()
            print(f"   ✅ JSON! Keys: {list(d.keys())[:10]}")
            items = d.get("items", d.get("@id", []))
            print(f"   Items: {len(items) if isinstance(items, list) else 'n/a'}")
            print(f"   Contenuto: {json.dumps(d, ensure_ascii=False)[:400]}")
        except: print(f"   Raw: {r.text[:200]}")
    else:
        print(f"   Raw: {r.text[:200]}")

sep("SATER — Feed RSS bandi")
for rss in [
    "https://intercenter.regione.emilia-romagna.it/bandi-e-strumenti-di-acquisto/bandi-intercenter/RSS",
    "https://intercenter.regione.emilia-romagna.it/RSS",
    "https://intercenter.regione.emilia-romagna.it/bandi-e-strumenti-di-acquisto/RSS",
]:
    print(f"\n   Provo: {rss}")
    r = get(rss, accept="application/rss+xml,application/xml,*/*")
    if r:
        print(f"   HTTP: {r.status_code}, Size: {len(r.content)/1024:.1f} KB")
        if r.status_code == 200:
            print(f"   Raw: {r.text[:300]}")
            try:
                root = ET.fromstring(r.content)
                items = root.findall(".//item")
                print(f"   ✅ RSS valido! Items: {len(items)}")
                for item in items[:2]:
                    print(f"     - {item.findtext('title','')[:80]}")
            except Exception as e:
                print(f"   XML error: {e}")

sep("SATER — Plone REST API search bandi")
r = get("https://intercenter.regione.emilia-romagna.it/++api++/@search?portal_type=Bando&b_size=5",
        accept="application/json")
if r:
    print(f"   HTTP: {r.status_code}")
    if r.status_code == 200:
        try:
            d = r.json()
            print(f"   ✅ JSON! {json.dumps(d, ensure_ascii=False)[:500]}")
        except: print(f"   Raw: {r.text[:200]}")
    else:
        print(f"   Raw: {r.text[:150]}")

sep("SATER — Scraping pagina principale (elenco bandi visibile)")
r = get("https://intercenter.regione.emilia-romagna.it/bandi-e-strumenti-di-acquisto/bandi-intercenter/bandi-e-procedure-di-gara")
if r and r.status_code == 200:
    html = r.text
    # Cerca la tabella dei bandi
    rows = re.findall(r'<tr[^>]*>(.*?)</tr>', html, re.DOTALL)
    print(f"   Righe tabella: {len(rows)}")
    gare_trovate = 0
    for row in rows:
        tds = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
        if len(tds) >= 2:
            testo = [re.sub(r'<[^>]+>', '', td).strip() for td in tds]
            testo = [t for t in testo if t and len(t) > 3]
            if testo:
                gare_trovate += 1
                if gare_trovate <= 5:
                    print(f"   Riga {gare_trovate}: {' | '.join(testo[:4])[:150]}")
    print(f"   Totale righe con dati: {gare_trovate}")
    # Cerca link a bandi singoli
    links = re.findall(r'href=["\']([^"\']+/bandi[^"\']+)["\']', html)
    links = list(set(links))[:5]
    print(f"   Link a bandi singoli: {links}")

print(f"\n{'='*60}")
print("✅ Test v2 completato")
