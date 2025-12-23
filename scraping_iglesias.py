import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
from duckduckgo_search import DDGS

# CONFIGURACIÓN ACTUALIZADA: NYC
BASE_URL = "https://www.churchstaffing.com/findjobs/search"
PARAMS = {
    "Keywords": "Worship",
    "Location": "New York City",  # <--- CAMBIO AQUÍ
    "Radius": "50",
    "Sort": "Date"
}
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

def find_website_url(church_name, location):
    """Busca la URL oficial usando DuckDuckGo"""
    query = f"{church_name} {location} church official website"
    try:
        results = DDGS().text(query, max_results=1)
        if results:
            return results[0]['href']
    except Exception as e:
        print(f"   ⚠️ No se pudo buscar web para {church_name}: {e}")
    return None

def extract_emails_from_url(url):
    """Entra a la web y busca patrones @email.com"""
    emails = set()
    try:
        # Timeout corto para no trabar el script
        response = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 1. Buscar en el texto visible y enlaces mailto:
        text_content = soup.get_text()
        mailto_links = [a['href'] for a in soup.select('a[href^=mailto:]')]
        
        # Regex para encontrar emails
        email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        
        found_in_text = re.findall(email_pattern, text_content)
        
        for email in found_in_text + mailto_links:
            if "mailto:" in email:
                email = email.replace("mailto:", "")
            # Filtros basura (evitar emails falsos de librerías js)
            if not any(x in email for x in ['sentry', 'example', 'domain', '.png', '.jpg']):
                emails.add(email)
                
    except Exception:
        pass
    
    return list(emails)

def get_jobs():
    print("🔍 Buscando vacantes en ChurchStaffing para NYC...")
    try:
        response = requests.get(BASE_URL, params=PARAMS, headers=HEADERS)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        job_cards = soup.select('.job-result-card')
        if not job_cards:
            job_cards = soup.find_all('div', class_='job-listing-item') 
            
        leads = []
        print(f"   -> Encontrados {len(job_cards)} posibles leads. Iniciando enriquecimiento...")
        
        for card in job_cards:
            try:
                title_tag = card.find('h3') or card.find('h2')
                company_tag = card.find('div', class_='employer-name') or card.find('div', class_='company-name')
                location_tag = card.find('div', class_='location')
                
                if title_tag:
                    title = title_tag.text.strip()
                    church = company_tag.text.strip() if company_tag else "Iglesia Desconocida"
                    location = location_tag.text.strip() if location_tag else "New York"
                    
                    print(f"   ⚡️ Procesando: {church}...")
                    
                    # 1. Buscar Web
                    website = find_website_url(church, location)
                    
                    # 2. Buscar Email (si encontramos web)
                    emails = []
                    if website:
                        emails = extract_emails_from_url(website)
                    
                    leads.append({
                        "Rol": title,
                        "Iglesia": church,
                        "Ubicación": location,
                        "Website": website if website else "No encontrada",
                        "Emails_Encontrados": ", ".join(emails) if emails else "No encontrados"
                    })
                    
                    # Pausa pequeña
                    time.sleep(1)
            except Exception as e:
                continue
                
        return leads
    except Exception as e:
        print(f"❌ Error general: {e}")
        return []

if __name__ == "__main__":
    data = get_jobs()
    if data:
        df = pd.DataFrame(data)
        # Reordenar columnas
        cols = ['Iglesia', 'Emails_Encontrados', 'Website', 'Rol', 'Ubicación']
        df = df[cols]
        
        csv_name = "leads_nyc_emails.csv"
        df.to_csv(csv_name, index=False)
        print(f"\n✅ ÉXITO: CSV generado '{csv_name}' con {len(data)} filas.")
    else:
        print("⚠️ No se encontraron resultados.")
