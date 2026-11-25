from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup


def collect_page_data(soup):
    # Extract data based on your HTML structure (replace with your specific logic)
    out_data = []
    for element in soup.find_all('div', id='tk_id'):
        # find all a elements where the href is like href="/tk/*"
        tk_links = element.find_all('a', href=lambda href: href and '/tk/' in href)
        for link in tk_links:
            out_data.append(link.text.strip())
    
    return out_data


def parse_search_url(first_page_url, outputfile):

    # Define the starting URL
    starting_url = first_page_url

    # Create a new instance of the Firefox driver
    # Create a new instance of the Firefox driver with headless option

    # Create a new instance of the Chrome driver with headless option
    options = Options()
    # options.add_argument('--headless')
    driver = webdriver.Chrome()

    # Visit the starting URL
    driver.get(starting_url)

    # Get the page source
    html_content = driver.page_source
    soup = BeautifulSoup(html_content, 'html.parser')
    
    parsed_data = []

    # Extract and append gemi numbers       
    parsed_data.extend(collect_page_data(soup))

    # append new data to the outputfile
    with open(outputfile, "a") as f:
        parsed_data_str = '\n'.join(parsed_data)
        parsed_data_str = parsed_data_str + '\n'
        f.write(parsed_data_str)

    # Close the browser
    driver.quit()
