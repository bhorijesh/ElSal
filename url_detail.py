from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
from bs4 import BeautifulSoup
import uuid
from datetime import datetime
from collections import OrderedDict  

# Scraping function that handles data extraction from a single URL
def scrape_data(driver, url):
    driver.get(url)

    # Wait for the main details container to be visible
    try:
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, '.sm-col-10')))
    except Exception as e:
        print(f"Timeout waiting for page to load: {e}")
        return None  

    # Initialize empty fields
    title, price, code_number, location, property_type, sale_type, descriptions = [None] * 7
    rooms, bathrooms_info, image_src_list = {}, {"Full Bathrooms": None, "Half Bathrooms": None}, []

    try:
        # Find the details container
        details = driver.find_element(By.CSS_SELECTOR, '.sm-col-10')
        html_content = details.get_attribute('innerHTML')
        soup = BeautifulSoup(html_content, 'html.parser')

        # Extract title
        for_title = soup.find('div', class_="clearfix mxn2 mb2")
        if for_title:
            title_element = for_title.find('h1', class_="mb1")
            title = title_element.text if title_element else "Title not found"

        # Extract price
        price_div = soup.find('div', class_='md-col md-col-4 px2')
        if price_div:
            prices = price_div.text.strip()
            if prices.startswith('$'):
                price = prices.replace('$', '').replace(',', '')

        # Extract code number, location, property type, sale type
        list_section = soup.find('div', class_="clearfix mxn2 mb2")
        if list_section:
            code_number = list_section.find('p', class_="md-right").text.strip() if list_section else None
            list_items = list_section.find_all('li', class_='inline-block')
            location = list_items[0].get_text(strip=True) if len(list_items) > 0 else None
            property_type = list_items[1].get_text(strip=True) if len(list_items) > 1 else None
            sale_type = list_items[2].get_text(strip=True) if len(list_items) > 2 else None

        # Extract description
        desc = soup.find('div', class_="md-col md-col-8 px2 mb4")
        descriptions = desc.get_text(strip=True) if desc else None

        # Extract room and bathroom details
        contain = soup.find('div', class_="clearfix mb2")
        if contain:
            try:
                for container in contain.find_all('div', class_='col col-6 md-col-4 lg-col-2 py1 px2 center'):
                    key = container.find('p').text.strip() if container.find('p') else "Unknown"
                    value = container.find('div', class_='inline-block text-80').text.strip().replace('x','') if container.find('div') else "Unknown"

                    # Check if this is bathroom information
                    if key == "Bathrooms":
                        parts = value.split()
                        if len(parts) == 2 and "full" not in parts and "half" not in parts:
                            bathrooms_info["Full Bathrooms"] = parts[0]
                            bathrooms_info["Half Bathrooms"] = parts[1].replace('½','1')
                        elif "full" in value or "half" in value:
                            if "full" in value:
                                bathrooms_info["Full Bathrooms"] = parts[0] if parts[0].isdigit() else None
                            if "half" in value:
                                bathrooms_info["Half Bathrooms"] = parts[-1] if parts[-1].isdigit() else None
                    else:
                        rooms[key] = value.split()[0]  # Just take the first number for other room types
            except Exception as e:
                print(f"Error while processing room and bathroom information: {e}")

        # Extract image URLs and ensure uniqueness
        try:
            carousel = driver.find_element(By.CSS_SELECTOR, '.carousel.mb1')
            carousel_html = carousel.get_attribute('innerHTML')
            carousel_soup = BeautifulSoup(carousel_html, 'html.parser')

            if carousel_soup:
                items = carousel_soup.find_all('li')
                for item in items:
                    img = item.find('a')
                    if img and 'href' in img.attrs:
                        img_url = img['href']
                        if img_url not in image_src_list:
                            image_src_list.append(img_url)
        except Exception as e:
            print(f"Error while extracting images: {e}")

    except Exception as e:
        print(f"Error while scraping {url}: {e}")

    unique_id = str(uuid.uuid4())
    created_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    uploaded_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Use OrderedDict to maintain consistent column order
    return OrderedDict({
        'uuid': unique_id,
        'URL': url,
        'Title': title,
        'PRICE': price,
        'CODE NUMBER': code_number,
        'LOCATION': location,
        'PROPERTY TYPE': property_type,
        'TYPE': sale_type,
        'Description': descriptions,
        **bathrooms_info,
        **rooms,
        'Image URLs': image_src_list,
        'Created Date': created_date,
        'Uploaded Date': uploaded_date
    })

# Main function to manage the process
def main():
    # Read the CSV file containing URLs
    csv_data = pd.read_csv('elSal1.csv')

    # Initialize the Chrome driver
    driver = webdriver.Chrome()

    # List to store scraped data
    scraped_data = []

    # Loop through each URL in the CSV
    for index, row in csv_data.iterrows():
        # if index >= 20:  
        #     break

        url = row['LINKS']
        data = scrape_data(driver, url)
        if data:
            scraped_data.append(data)

    # Create a DataFrame from the scraped data
    scraped_df = pd.DataFrame(scraped_data)

    # Add a sequential serial number to the 'id' column
    scraped_df.insert(0, 'id', scraped_df.index + 1)

    # Define the desired column order
    column_order = [
        'id', 'uuid', 'URL', 'Title', 'PRICE', 'CODE NUMBER', 'LOCATION', 
        'PROPERTY TYPE', 'TYPE', 'Description', 'Area of Land', 'Construction Area',
        'Rooms', 'Full Bathrooms', 'Half Bathrooms', 'Parking Lot', 'Image URLs', 
        'Created Date', 'Uploaded Date'
    ]

    # Reorder the DataFrame columns
    scraped_df = scraped_df.reindex(columns=column_order)

    # Save the DataFrame to a CSV file
    scraped_df.to_csv('trial1.csv', index=False)

    print("Scraping completed!")
    driver.quit()

if __name__ == "__main__":
    main()
