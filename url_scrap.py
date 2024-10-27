from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd

# Initialize the Firefox driver
driver = webdriver.Chrome()

# Function to get the total number of pages
def get_total_pages():
    driver.get("https://remax-central.com.sv/en?page=1")
    
    # Wait for the pagination to load
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CLASS_NAME, 'pagination'))
    )
    
    pagination = driver.find_element(By.CLASS_NAME, 'pagination')  
    pages = pagination.find_elements(By.TAG_NAME, 'a')
    
    total_pages = 0
    for page in pages:
        if page.text.isdigit(): 
            total_pages = max(total_pages, int(page.text))
    
    return total_pages

total_pages = get_total_pages()

def main():
    link =[]
    for i in range(1, total_pages + 1): 
        driver.get(f"https://remax-central.com.sv/en?page={i}")
        
        # Wait for the elements to be loaded
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'div.mt4:nth-child(2)'))  
        )
        
        elements = driver.find_elements(By.CSS_SELECTOR, 'div.mt4:nth-child(2)')  
        for elem in elements:
            html_content = elem.get_attribute('innerHTML')
            soup = BeautifulSoup(html_content, 'html.parser')
            content = soup.find('div', class_ = 'clearfix')
            links = content.find_all('a')
            for l in links:
                url = l.get('href')
                
                if url and not url.startswith('javascript:void(0)'):
                        link.append(url) 

                
                
    df = pd.DataFrame(link, columns =['LINKS'])      
    df.to_csv("elSal1.csv", index = False)

main()
driver.quit()
