import acci
import post_codes
import sys
import concurrent.futures
from multiprocessing import freeze_support


if __name__ == '__main__':
    freeze_support()

        # Check if "feed=acci" argument is present in the command-line arguments
    if "feed=acci" in sys.argv:
        # Perform actions specific to the "feed=acci" argument
        acci_urls_to_crawl = {
            "AE": [
                "https://directory.acci.gr/companies/list?who=&afm=&gemi=&legalType=18&businessCategory=1&objective=&dtFromTo=",
                7031,475
            ],
            "AE_rest": [
                "https://directory.acci.gr/companies/list?who=&afm=&gemi=&legalType=23&legalType=19&legalType=35&legalType=29&businessCategory=1&objective=&dtFromTo=",
                216,"208 done..."],
            "Atomiki": [
                "https://directory.acci.gr/companies/list?who=&afm=&gemi=&legalType=2&businessCategory=1&objective=&dtFromTo=",
                5161
            ],
            "All_rest_small": [
                "https://directory.acci.gr/companies/list?who=&afm=&gemi=&legalType=32&legalType=30&legalType=31&legalType=38&legalType=36&legalType=37&legalType=33&legalType=34&legalType=55&legalType=48&legalType=40&legalType=41&legalType=39&legalType=17&legalType=7&legalType=81703380&legalType=54&legalType=11&legalType=50&legalType=51&legalType=14&legalType=28&legalType=53&legalType=56&legalType=9&legalType=5&legalType=52&legalType=25&legalType=26&legalType=21&legalType=22&businessCategory=1&objective=&dtFromTo=",
                442
            ],
            "EE": [
                "https://directory.acci.gr/companies/list?who=&afm=&gemi=&legalType=4&businessCategory=1&objective=&dtFromTo=",
                1770
            ],
            "EPE": [
                "https://directory.acci.gr/companies/list?who=&afm=&gemi=&legalType=10&legalType=13&businessCategory=1&objective=&dtFromTo=",
                1932
            ],
            "IKE": [
                "https://directory.acci.gr/companies/list?who=&afm=&gemi=&legalType=15&legalType=16&businessCategory=1&objective=&dtFromTo=",
                3825
            ],
            "OE": [
                "https://directory.acci.gr/companies/list?who=&afm=&gemi=&legalType=3&businessCategory=1&objective=&dtFromTo=",
                1673
            ]  
        }

        #RUN ACCI
        with concurrent.futures.ProcessPoolExecutor(max_workers=3) as executor:
            for legal_acci_url, url_data in acci_urls_to_crawl.items():
                starting_url = url_data[0]
                output_file = "./data/"+legal_acci_url+".txt"
                executor.submit(acci.parse_search_url, starting_url, output_file)
    elif "feed=post_codes" in sys.argv:
        url_to_crawl = "https://www.taxidromikoskodikas.gr/tk/"
        
        output_file = "./data/post_codes.txt"
        post_codes.parse_search_url(url_to_crawl, output_file)
    elif "feed=post_codes_gemi" in sys.argv:
        url_to_crawl = "https://www.taxidromikoskodikas.gr/tk/"
        
        output_file = "./data/post_codes.txt"
        post_codes.parse_search_url(url_to_crawl, output_file)        
    else:
        # Perform actions for all other arguments
        pass
