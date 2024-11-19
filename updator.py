import concurrent.futures
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from sqlalchemy import create_engine
import pymysql
import requests
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
from bs4 import BeautifulSoup
from selenium.common.exceptions import TimeoutException
from datetime import datetime
import json
import re

# Define constants
QUERYSTRING = {
    "ajaxcall": "true",
    "ajaxtarget": "privatenotes,listingmeta,customlistinginfo_attributiontext,listingdetailsmap,routeplanner,listingcommunityinfo,metainformation,listingmeta_bottom,listingmedia,listingpropertydescription,listingtabtitles,listingtools_save_bottom,customlistinginfo_commentsshort,listingtools,listingtools_mobile,listinginfo,listingmarkettrendsmodule,localguidelistingdetailspage,listingdrivetime,listingphotos,listingdetails",
    "cms_current_mri": "119274"
}

HEADERS = {
    "accept": "application/xml, text/xml, */*; q=0.01",
    "accept-language": "en-US,en;q=0.9",
    "cache-control": "no-cache",
    "pragma": "no-cache",
    "priority": "u=1, i",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "Windows",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "x-requested-with": "XMLHttpRequest"
}

DB_TABLE_NAME = "ElSal"  # Define your table name here

class PropertyUpdater:
    def __init__(self):
        self.db_connection, self.cursor = self.connect_database_with_pymysql()
        self.engine = self.connect_database_with_sqlalchemy()
        self.driver = self.driver_initialization()

    def connect_database_with_sqlalchemy(self):
        try:
            db_url = 'mysql+pymysql://root:new_password@localhost:3306/practice'
            engine = create_engine(db_url)
            engine.connect()
            print('Database connected successfully')
            return engine
        except Exception as e:
            print(f'Error while connecting to the database: {e}')
            return None

    def connect_database_with_pymysql(self):
        try:
            connection = pymysql.connect(
                host='localhost',
                user='root',
                password='new_password',
                db='practice'
            )
            cursor = connection.cursor()
            print("MySQL connection established")
            return connection, cursor
        except Exception as e:
            print(f'Error while connecting to MySQL: {e}')
            return None, None
        
# ---------------links are fetched from the database------------!
    def fetch_link(self, i, link: str) -> dict:
        url = link
        extracted_data = {
            'title': None, 'price': None, 'code_number': None, 'location': None,
            'property_type': None, 'sale_type': None, 'description': None, 'privacy': None,
            'rooms': {}, 'rom': {"Bedroom": None}, 'land_area': {"area_square_vara": None},
            'construction_area': {"Construction_area": None}, 'Levels': {"levels": None},
            'park': {"parking": None}, 'bathrooms_info': {"full_bathrooms": None, "half_bathrooms": None},
            'image_src_list': []
        }

        try:
            response = requests.get(url, headers=HEADERS, params=QUERYSTRING)

            if response.status_code == 200 and response.content.strip():
                print(f"Data fetched successfully from {url}")
                try:
                    detailed_data = self.scrape_data(self.driver,link)
                    if detailed_data:
                        extracted_data.update(detailed_data)
                        # print(extracted_data)
                    if not extracted_data.get('description'):
                        print(f"Essential data missing for link: {link}")
                        return {'status_404': 1, 'link': link}
                    return extracted_data  # Return valid data
                except Exception as e:
                    print(f'TimeOutException : Conetnt not found for link : {link}')
                    status_404 = 1

            elif response.status_code == 204:  # Assuming 204 indicates "No Content"
                print(f"Content not available from {url} (Status Code 204).")
                status_404 = 1
            else:
                print(f"Failed to fetch data from {url} - Status code: {response.status_code}")
                status_404 = 1

        except Exception as e:
            print(f"Error fetching data from {url}: {e}")
            status_404 = 1
            extracted_data = {'status_404': status_404, 'link': link}

        return extracted_data

# --------------validating the data from database and newly fetched data-------!
    def validating_with_db_data(self,db_data : pd.DataFrame, extracted_data : dict) -> tuple[dict | None, bool]:
        VALUE_CHANGED = False
        IMAGES_CHANGED = False
        changed_extracted_data = {}
        print(f"Validating link: {extracted_data['link']}")
        
        db_row = db_data[db_data['link'] == extracted_data['link']]
        # print(db_row)
        if not db_row.empty:
            changed_row = {}
            
            for key in extracted_data.keys():
                if key in db_row.columns:
                        db_value = db_row[key].iloc[0]
                        extracted_value = extracted_data[key]
                        
                        if db_value == '':
                            db_value = None
                            
                        if key == 'img_src':
                            db_value_copy = db_value.split(',') if db_value else []
                            
                            if len(db_value_copy) != len(extracted_value):
                                VALUE_CHANGED = True
                                changed_row[key] = extracted_value
                        
                        elif key in ['price', 'bedrooms', 'full_baths','half_baths','construction_area','area_square_vara','levels','parking']:
                            if extracted_value is not None and db_value is not None:
                                
                                if str(float(extracted_value)) != str(float(db_value)):
                                    VALUE_CHANGED = True
                                    changed_row[key] = extracted_value
                                    
                            else:
                                if str(extracted_value) != str(db_value):
                                    changed_row[key] = extracted_value
                                    
                        else:
                            if str(extracted_value) != str(db_value):
                                VALUE_CHANGED = True
                                changed_row[key] = extracted_value
                                
            if VALUE_CHANGED or IMAGES_CHANGED:
                changed_row['id'] = db_row['id'].iloc[0]
                changed_row['link'] = extracted_data['link']
                changed_row['updated_at'] = datetime.now().strftime("%y-%m-%d %H:%M:%S")
                changed_extracted_data.update(changed_row)
        else:
            print(f'Link is not Found in database : {extracted_data['link']}')
            VALUE_CHANGED = True
            changed_extracted_data.update(extracted_data)
            
            
        if VALUE_CHANGED or IMAGES_CHANGED:
            return changed_extracted_data, IMAGES_CHANGED
        else:
            None, IMAGES_CHANGED

# --------------deleting if content not fouond--------!
    def delete_not_found_items(self, filtered_df: dict) -> None:
        query_tuple = (datetime.today().strftime('%Y/%m/%d'), datetime.today().strftime('%Y/%m/%d'), filtered_df['link'])
        try:
            self.db_connection.ping(reconnect=True)
            self.cursor.execute(f"UPDATE {DB_TABLE_NAME} SET deleted_at = %s, updated_at = %s WHERE link = %s", query_tuple)
            self.db_connection.commit()
            print(f"Successfully deleted data for link: {filtered_df['link']}")
        except Exception as e:
            self.db_connection.rollback()
            print(f"Error deleting not found items: {str(e)}")
            raise
        
# --------------- updating data in the databae----------------!
    def update_data(self, changed_data: dict) -> None:
        if 'img_src' in changed_data and isinstance(changed_data['img_src'], list):
            changed_data['img_src'] = json.dumps(changed_data['img_src'])
        update_fields = [f"{key} = %s" for key in changed_data.keys() if key not in ['id', 'link']]

        update_query = f"UPDATE {DB_TABLE_NAME} SET {', '.join(update_fields)} WHERE link = %s"

        update_values = [changed_data[key] for key in changed_data.keys() if key not in ['id', 'link']]
        update_values.append(changed_data['link'])

        # Debug prints to inspect the query and values
        # print("Update query:", update_query)
        # print("Update values:", update_values)

        try:
            self.db_connection.ping(reconnect=True)
            self.cursor.execute(update_query, tuple(update_values))
            self.db_connection.commit()
            print(f"Successfully updated data for link: {changed_data['link']}")
        except Exception as e:
            self.db_connection.rollback()
            print(f"Error updating data: {str(e)}")


# ---------------geting data from database------------!
    def get_database_links(self) -> pd.DataFrame:
        query = "SELECT id, title, uuid, link, price, price_unit, img_src, description, Bedroom, full_bathrooms, half_bathrooms, property_type,code_number FROM ElSal WHERE deleted_at IS NULL"
        try:
            df = pd.read_sql(query, con=self.engine)
            print(f"Fetched {len(df)} links from the database.")
            return df
        except Exception as e:
            print(f"Error fetching links from the database: {e}")
            return pd.DataFrame()
        
# ------------scrapping the data from the database link---------!
    def scrape_data(self,driver, link):
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


        return {
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
        }


# -------------driver initialization -------!
    def driver_initialization(self):
        """Initializes and returns a Selenium WebDriver."""
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        driver = webdriver.Chrome(options=options)
        return driver
    
    
# main function
    def main(self):
        df = self.get_database_links()
        # links = [
        #     "https://remax-central.com.sv/en/properties/oasis-residence-model-b---4-rooms-barra-de-santiago-el-salvador",
        #     "https://remax-central.com.sv/en/properties/millennium-tower-office-space-5/a"
        #     ]
        
        for i, link in enumerate(df['link']):
            data = self.fetch_link(i, link)
            if data.get('status_404') == 1:
                self.delete_not_found_items(data)
            else:
                changed_data, _ = self.validating_with_db_data(df, data)
                if changed_data:
                    self.update_data(changed_data)

        self.driver.quit()

if __name__ == "__main__":
    updater = PropertyUpdater()
    updater.main()

