from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from bs4 import BeautifulSoup

'''
from seleniumbase import Driver

driver = Driver()
driver.get("https://selenium.dev")

driver.quit()
'''


def collect_gemi_data(soup):
    # Extract data based on your HTML structure (replace with your specific logic)
    gemi_data = []
    for element in soup.find_all('div', class_='col-lg-7'):
        details = element.find('div', class_='table-responsive').find_all('tr')
        for detail in details:
            columns = detail.find_all('td')
            if len(columns) == 2:
                if columns[0].text.strip() in ("Gemi Number", "Αρ. Γ.Ε.ΜΗ."):
                    gemi_data.append(columns[1].text.strip().replace(" ↗", ""))
    
    return gemi_data



def parse_acci_search_url(first_page_url, outputfile):

    # Define the starting URL
    starting_url = first_page_url

    # Create a new instance of the Firefox driver
    # Create a new instance of the Firefox driver with headless option

    # Create a new instance of the Chrome driver with headless option
    options = Options()
    options.add_argument('--headless')
    driver = webdriver.Chrome(options=options)


    # Visit the starting URL
    driver.get(starting_url)

    # Loop through pages
    while True:
        # Get the page source
        html_content = driver.page_source
        soup = BeautifulSoup(html_content, 'html.parser')
        
        gemi_data = []

        # Extract and append gemi numbers       
        gemi_data.extend(collect_gemi_data(soup))

        #append new data to the outputfile
        with open(outputfile, "a") as f:
            gemi_data_str = '\n'.join(gemi_data)
            gemi_data_str = gemi_data_str + '\n'
            f.write(gemi_data_str)

        # Find and click the next page button
        active_page_element = driver.find_element(By.XPATH, '//ul[@class="pagination text-center"]/li/a[@class="active"]')
        parent_li = active_page_element.find_element(By.XPATH, '..')  # Get the parent li of the disabled element
        parent_ul = active_page_element.find_element(By.XPATH, '../..')  # Get the parent li of the disabled element
        current_li_index = parent_ul.find_elements(By.TAG_NAME, 'li').index(parent_li)  # Get the index of the disabled element
        next_li = parent_ul.find_elements(By.TAG_NAME, 'li')[current_li_index + 1]
        next_button = next_li.find_element(By.XPATH, './a')  # Find the a tag within the next li element
        if next_button:
            if next_button.text not in ("Last","Τελευταία"):
                WebDriverWait(driver, 10).until(EC.element_to_be_clickable(next_button))
                next_button.click()
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, '//ul[@class="pagination text-center"]/li/a[@class="active"]')))
            else:
                break
        else:
            break  # Exit the loop if there's no next page button

    # Close the browser
    driver.quit()
