from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
from bs4 import BeautifulSoup
import uuid
from datetime import datetime
from collections import OrderedDict
import re  # Ensure re is imported for regex handling

# Scraping function that handles data extraction from a single URL
def scrape_data(driver, link):
    driver.get(link)

    # Wait for the main details container to be visible
    try:
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, '.sm-col-10')))
    except Exception as e:
        print(f"Timeout waiting for page to load: {e}")
        return None  

    # Initialize empty fields
    title, price, code_number, location, property_type, sale_type, descriptions,privacy = [None] * 8
    rooms,rom,land_area,construction_area, Levels,park, bathrooms_info, image_src_list = {},{"Bedroom": None},{"area_square_vara" : None},{"Construction_area" : None},{"levels" : None},{"parking" : None},{"full_bathrooms": None, "half_bathrooms": None}, []

    try:
        # Find the details container
        details = driver.find_element(By.CSS_SELECTOR, '.sm-col-10')
        html_content = details.get_attribute('innerHTML')
        soup = BeautifulSoup(html_content, 'html.parser')

        # Extract title
        for_title = soup.find('div', class_="clearfix mxn2 mb2")
        if for_title:
            title_element = for_title.find('h1', class_="mb1")
            title = title_element.text if title_element else None

        # Extract price
        price_div = soup.find('div', class_='md-col md-col-4 px2')
        if price_div:
            prices = price_div.text.strip().split()[0]
            if prices.startswith('$'):
                price = prices.replace('$', '').replace(',', '')

        # Extract code number, location, property type, sale type
        list_section = soup.find('div', class_="clearfix mxn2 mb2")
        if list_section:
            code_number = list_section.find('p', class_="md-right").text.strip() if list_section else None
            list_items = list_section.find_all('li', class_='inline-block')
            location = list_items[0].get_text(strip=True) 
            property_type = list_items[1].get_text(strip=True) 
            sale_type = list_items[2].get_text(strip=True) 

        # Extract description
        desc_section = soup.find('div', class_="md-col md-col-8 px2 mb4")
        if desc_section:
            descriptions = ''.join(desc_section.find_all(string=True, recursive=False)).strip()
        else:
            descriptions = None


        # Extract room and bathroom details
        contain = soup.find('div', class_="clearfix mb2")
        if contain:
            try:
                for container in contain.find_all('div', class_='col col-6 md-col-4 lg-col-2 py1 px2 center'):
                    key_element = container.find('p')
                    key = key_element.text.strip() if key_element else None  # Safely extract the key text

                    if key and "Area of Land" in key:
                        value_element = container.find('div', class_='inline-block text-80')
                        if value_element:
                            value = value_element.get_text(strip=True).replace('v2','').split()
                            land_area["area_square_vara"] = value[0] if value else None

                    elif key and "Construction Area" in key:
                        value_element = container.find('div', class_='inline-block text-80')
                        if value_element:
                            value = value_element.get_text(strip=True).replace('m2','').split()
                            construction_area["Construction_area"] = value[0] if value else None                        

                    # Extract bathroom information safely
                    elif key and "Bathrooms" in key:
                        value_element = container.find('div', class_='inline-block text-80')
                        if value_element:
                            value = value_element.get_text(strip=True)
                            datas = value.split()
                            bathrooms_info["full_bathrooms"] = datas[0].replace('x', '') if len(datas) > 0 else None
                            bathrooms_info["half_bathrooms"] = datas[1].replace('½', '1') if len(datas) > 1 else None

                    # For individual 'Full baths' or 'half baths'
                    elif key and ('Full baths' in key or 'half baths' in key):
                        value_element = container.find('div', class_='inline-block text-80')
                        if value_element:
                            value = value_element.get_text(strip=True).replace('x', '')
                            bathrooms_info["full_bathrooms"] = value
                        half_bath_match = re.findall(r'\d+', key)
                        bathrooms_info["half_bathrooms"] = int(half_bath_match[0]) if half_bath_match else None
                    elif key and "Parking Lot" in key:
                        value_element = container.find('div', class_='inline-block text-80')
                        if value_element:
                            value = value_element.text.strip().replace('x','').split()
                            park["parking"] = value[0] if value else None
                    elif key and "Rooms" in key:
                        value_element = container.find('div', class_='inline-block text-80')
                        if value_element:
                            value = value_element.get_text(strip=True).replace('x', '')
                            rom["Bedroom"] = value if value else None


                    # For other room-related information
                    else:
                        room_value_element = container.find('div', class_='inline-block text-80')
                        if room_value_element:
                            room_text = room_value_element.text.strip().replace('x', '').replace('v2', '').split()
                            rooms[key] = room_text[0] if room_text else None
                        else:
                            rooms[key] = None
            except Exception as e:
                print(f"Error while processing room and bathroom information: {e}")
            
            try:
                for con in contain.find_all('div', class_='col col-6 md-col-4 lg-col-1 py1 px1 center'):
                    key_element = con.find('p', class_ ='text-80')
                    key = key_element.text.strip() if key_element else None  # Safely extract the key text
                    if key and "Levels" in key:
                         value_element = con.find('div', class_='inline-block text-80')
                         if value_element:
                            value = value_element.get_text(strip=True).replace('x','')
                            Levels["levels"] = value if value else None 
            except Exception as e:
                print(f"Error while processing levels: {e}")

        


        # Extract privacy information
        try:
            type_element = contain.find('div', class_='col col-12 sm-col-12 lg-col-1 py1 px2 center')
            privacy_element = type_element.find('p', class_='text-80') if type_element else None
            
            if privacy_element:
                privacy = privacy_element.text.strip()
        except Exception as e:
            print(f'Error occurred while getting privacy information: {e}')


        # Extract image URLs and ensure uniqueness
        try:
            carousel = driver.find_element(By.CSS_SELECTOR, '.carousel.mb1')
            carousel_html = carousel.get_attribute('innerHTML')
            carousel_soup = BeautifulSoup(carousel_html, 'html.parser')

            if carousel_soup:
                item1 = carousel_soup.find('div', class_ ="col-10 py1 overflow-hidden")
                items = item1.find_all('li')
                for item in items:
                    img = item.find('a')
                    if img and 'href' in img.attrs:
                        img_url = img['href']
                        if img_url not in image_src_list:
                            image_src_list.append(img_url)
        except Exception as e:
            print(f"Error while extracting images: {e}")

    except Exception as e:
        print(f"Error while scraping {link}: {e}")

    unique_id = str(uuid.uuid4())
    created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    deleted_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    country = "El Salvador"
    previous = None
    phone = None
    price_unit = None

    # Use OrderedDict to maintain consistent column order
    return OrderedDict({
        'uuid': unique_id,
        'link': link,
        'title': title,
        'price': price,
        'code_number': code_number,
        'location': location,
        'property_type': property_type,
        'type': sale_type,
        'description': descriptions,
        **land_area,
        **construction_area,
        **bathrooms_info,
        **rooms,
        **park,
        **Levels,
        **rom,
        'privacy': privacy,
        'img_src': image_src_list,
        'country': country,
        'created_at': created_at,
        'updated_at': updated_at,
        'deleted_at' : deleted_at,
        'previous_price' : previous,
        'phone': phone,
        'price_unit': price_unit
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
        if index >= 5:  
            break

        link= row['LINKS']
        data = scrape_data(driver, link)
        if data:
            scraped_data.append(data)

    # Create a DataFrame from the scraped data
    scraped_df = pd.DataFrame(scraped_data)

    # Define the desired column order
    column_order = [
        'uuid', 'link', 'title', 'price','price_unit','phone', 'code_number', 'location', 
        'property_type', 'type', 'description','country', 'area_square_vara', 'Construction_area',
        'Bedroom', 'full_bathrooms', 'half_bathrooms', 'levels','parking','privacy', 'img_src', 
        'created_at', 'updated_at','deleted_at','previous_price'
    ]

    # Reorder the DataFrame columns
    scraped_df = scraped_df.reindex(columns=column_order)

    # Save the DataFrame to a CSV fileclear
    scraped_df.to_csv('details2.csv', index=False)

    print("Scraping completed!")
    driver.quit()

if __name__ == "__main__":
    main()
