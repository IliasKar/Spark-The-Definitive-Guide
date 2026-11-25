import json
import requests
import datetime
import sys


class GemiAPI:
    def __init__(self):
        self.base_url = "https://publicity.businessportal.gr/api/search"
        self.headers = {'Content-Type': 'application/json',
                        'User-Agent': 'PostmanRuntime/7.37.0'}

    def get_last_day_of_month(self, input_day):
        # The day 28 exists in every month. 4 days later, it's always next month
        next_month = input_day.replace(day=28) + datetime.timedelta(days=4)
        # subtracting the number of the current day brings us back one month
        return next_month - datetime.timedelta(days=next_month.day)

    def make_api_call(self, payload):
        response = requests.post(self.base_url, headers=self.headers, data=json.dumps(payload))
        # print(json.dumps(payload))
        return response.json()
            
    def process_zip_code(self, zip_code, target_page, recommendationDateFrom, 
                         recommendationDateTo, level):

        payload = {
            "dataToBeSent": {
                "city": None,
                "postcode": zip_code,
                "legalType": [],
                "status": [],
                "suspension": [],
                "category": [],
                "specialCharacteristics": [],
                "employeeNumber": [],
                "armodiaGEMI": [],
                "kad": [],
                "recommendationDateFrom": recommendationDateFrom,
                "recommendationDateTo": recommendationDateTo,
                "closingDateFrom": None,
                "closingDateTo": None,
                "alterationDateFrom": None,
                "alterationDateTo": None,
                "person": [],
                "personrecommendationDateFrom": None,
                "personrecommendationDateTo": None,
                "radioValue": "all",
                "places": [],
                "page": target_page
            }
        }

        # print(f"payload is: {json.dumps(payload)}")

        api_response = self.make_api_call(payload)
        self.process_api_response(api_response, zip_code.strip(), target_page,
                                  recommendationDateFrom, recommendationDateTo, level)        

    def lvl1_exception(self, zip_code):
        current_year = datetime.datetime.now().year
        for year in range(1970, current_year):

            lvl2_rec_from = f"01/01/{year}"
            lvl2_rec_to = f"31/12/{year}"

            lvl2_payload = {
                "dataToBeSent": {
                    "city": None,
                    "postcode": zip_code,
                    "legalType": [],
                    "status": [],
                    "suspension": [],
                    "category": [],
                    "specialCharacteristics": [],
                    "employeeNumber": [],
                    "armodiaGEMI": [],
                    "kad": [],
                    "recommendationDateFrom": lvl2_rec_from,
                    "recommendationDateTo": lvl2_rec_to,
                    "closingDateFrom": None,
                    "closingDateTo": None,
                    "alterationDateFrom": None,
                    "alterationDateTo": None,
                    "person": [],
                    "personrecommendationDateFrom": None,
                    "personrecommendationDateTo": None,
                    "radioValue": "all",
                    "places": [],
                    "page": 1
                }
            }
        
            lv2_api_response = self.make_api_call(lvl2_payload)

            # print(f"payload is: {json.dumps(lvl2_payload)}")

            self.process_api_response(lv2_api_response, zip_code,
                                        1, lvl2_rec_from, lvl2_rec_to, 1)        

    def lvl2_exception(self, zip_code, recommendationDateFrom,
                       recommendationDateTo):
        # crawl the GEMI by month for the specific time range []
        start_date = datetime.datetime.strptime(recommendationDateFrom.strip(), '%d/%m/%Y')
        end_date = datetime.datetime.strptime(recommendationDateTo.strip(), '%d/%m/%Y')

        months_between = (end_date.year - start_date.year) * 12 + end_date.month - start_date.month

        for i in range(months_between+1):

            month = start_date.month + i
            
            if month < 10:
                month = f"0{month}"
            
            lvl3_rec_from = f"{start_date.day}/{month}/{start_date.year}"

            end_of_month = self.get_last_day_of_month(datetime.date(start_date.year, start_date.month + i, 1))

            lvl3_rec_to = f"{end_of_month.day}/{month}/{start_date.year}"

            lvl3_payload = {
                "dataToBeSent": {
                    "city": None,
                    "postcode": zip_code,
                    "legalType": [],
                    "status": [],
                    "suspension": [],
                    "category": [],
                    "specialCharacteristics": [],
                    "employeeNumber": [],
                    "armodiaGEMI": [],
                    "kad": [],
                    "recommendationDateFrom": lvl3_rec_from,
                    "recommendationDateTo": lvl3_rec_to,
                    "closingDateFrom": None,
                    "closingDateTo": None,
                    "alterationDateFrom": None,
                    "alterationDateTo": None,
                    "person": [],
                    "personrecommendationDateFrom": None,
                    "personrecommendationDateTo": None,
                    "radioValue": "all",
                    "places": [],
                    "page": 1
                }
            }

            lv3_api_response = self.make_api_call(lvl3_payload)

            print(f"Found {lv3_api_response['company']['total']['value']} companies")
            # print(f"payload is: {json.dumps(lvl3_payload)}")

            self.process_api_response(lv3_api_response, zip_code,
                                        1, lvl3_rec_from, lvl3_rec_to, 2)        

    def lvl3_exception(self, zip_code, recommendationDateFrom,
                       recommendationDateTo):
        # crawl the GEMI by month for the specific time range []
        start_date = datetime.datetime.strptime(recommendationDateFrom.strip(), '%d/%m/%Y')
        end_date = datetime.datetime.strptime(recommendationDateTo.strip(), '%d/%m/%Y')

        months_between = (end_date.year - start_date.year) * 12 + end_date.month - start_date.month

        for i in range(months_between+1):
            print("i=", i)

            month = start_date.month + i
            
            if month < 10:
                month = f"0{month}"

            day = start_date.day

            if day < 10:
                day = f"0{day}"
            
            lvl4_rec_from = f"{day}/{month}/{start_date.year}"

            end_of_month = self.get_last_day_of_month(datetime.date(start_date.year, start_date.month + i, 1))

            lvl4_rec_to = ""

            for j in range(end_of_month.day):
                print("j=", j)
                if j+1 < 10:
                    lvl4_rec_to = f"0{j+1}/{month}/{start_date.year}"
                else:
                    lvl4_rec_to = f"{j+1}/{month}/{start_date.year}"

                lvl4_payload = {
                    "dataToBeSent": {
                        "city": None,
                        "postcode": zip_code,
                        "legalType": [],
                        "status": [],
                        "suspension": [],
                        "category": [],
                        "specialCharacteristics": [],
                        "employeeNumber": [],
                        "armodiaGEMI": [],
                        "kad": [],
                        "recommendationDateFrom": lvl4_rec_to,
                        "recommendationDateTo": lvl4_rec_to,
                        "closingDateFrom": None,
                        "closingDateTo": None,
                        "alterationDateFrom": None,
                        "alterationDateTo": None,
                        "person": [],
                        "personrecommendationDateFrom": None,
                        "personrecommendationDateTo": None,
                        "radioValue": "all",
                        "places": [],
                        "page": 1
                    }
                }

                lv4_api_response = self.make_api_call(lvl4_payload)

                # print(f"Found {lv4_api_response['company']['total']['value']} companies")
                # print(f"payload is: {json.dumps(lvl3_payload)}")

                self.process_api_response(lv4_api_response, zip_code,
                                            1, lvl4_rec_to, lvl4_rec_to, 3) 


    def process_api_response(self, api_response, zip_code, current_page,
                             recommendationDateFrom, recommendationDateTo,
                             level):
        
        # Check company/total/value and take appropriate action
        if api_response['company']['total']['value'] > 100:
            if recommendationDateFrom is None and recommendationDateTo is None:
                #lvl1_exception
                self.lvl1_exception(zip_code)
            else:
                if level == 1:
                    self.lvl2_exception(zip_code, recommendationDateFrom,
                                        recommendationDateTo)
                if level == 2:
                    self.lvl3_exception(zip_code, recommendationDateFrom,
                                        recommendationDateTo)
                elif level == 3:
                    self.store_in_exception_file(zip_code,
                                                 recommendationDateFrom,
                                                 recommendationDateTo,
                                                 api_response['company']
                                                 ['total']['value'], 4)

        elif api_response['company']['total']['value'] <= 100 and api_response['company']['total']['value'] > 0:
            self.collect_gemi_numbers(zip_code, recommendationDateFrom,
                                      recommendationDateTo, api_response
                                      ['company']['hits'])
            print(f'{zip_code} - {api_response["company"]["total"]["value"]}, current_page:{current_page} has rergistered {len(api_response["company"]["hits"])} companies for period {recommendationDateFrom} - {recommendationDateTo}')
            
            next_page = self.calculate_next_page(api_response, current_page)
            if next_page is not None:
                self.process_zip_code(zip_code, next_page,
                                      recommendationDateFrom,
                                      recommendationDateTo, level)
        elif api_response['company']['total']['value'] == 0:
            print(f'{zip_code} - {api_response["company"]["total"]["value"]}, current_page:{current_page} has rergistered {len(api_response["company"]["hits"])} companies for period {recommendationDateFrom} - {recommendationDateTo}')

    def store_in_exception_file(self, zip_code,
                                recommendationDateFrom, recommendationDateTo,
                                num_companies, level):
        with open(f'./data/zip_code_gemi_numbers_lvl{level}_exceptions.txt', 'a') as file:
            file.write(f"{zip_code}, {recommendationDateFrom}, {recommendationDateTo}, {num_companies}\n")            

    def collect_gemi_numbers(self, zip_code, recommendationDateFrom,
                             recommendationDateTo, hits):
        gemi_numbers = []
        # Logic to collect gemiNumber values from hits
        for hit in hits:
            gemi_numbers.append(f"{zip_code},{recommendationDateFrom},{recommendationDateTo},{hit['gemiNumber']},{hit['afm']}")

        # Logic to write gemiNumbers to a file
        with open('./data/zip_code_gemi_numbers.txt', 'a') as file:
            gemi_numbers_str = '\n'.join(gemi_numbers)
            gemi_numbers_str = gemi_numbers_str + '\n'
            file.write(gemi_numbers_str)

        return gemi_numbers

    def calculate_next_page(self, api_response, current_page):
        # the total pages is the api_response['company/total/value'] / 10 absolute value
        total_records = api_response['company']['total']['value']
        gemi_page_size = 10
        total_pages = (total_records+gemi_page_size-1)//gemi_page_size

        if current_page < total_pages:
            return current_page + 1
        else:
            return None


# Example usage:
api = GemiAPI()

if __name__ == '__main__':
    if "parse=all" in sys.argv:
        with open('data/post_codes.txt', 'r') as file:
            zip_codes = file.readlines()
            for zip_code in zip_codes:
                zip_code = zip_code.strip()
                api.process_zip_code(zip_code, 1, None, None, 1)
    elif "parse=lvl2" in sys.argv:
        with open('data/zip_code_gemi_numbers_lvl2_exceptions.txt', 'r') as file:
            lines = file.readlines()
            for line in lines:
                line = line.strip()
                zip_code, recommendationDateFrom, recommendationDateTo, num_companies = line.split(',')
                api.lvl2_exception(zip_code, recommendationDateFrom, recommendationDateTo)
    elif "parse=lvl3" in sys.argv:
        with open('data/zip_code_gemi_numbers_lvl3_exceptions.txt', 'r') as file:
            lines = file.readlines()
            for line in lines:
                line = line.strip()
                zip_code, recommendationDateFrom, recommendationDateTo, num_companies = line.split(',')
                api.lvl3_exception(zip_code, recommendationDateFrom, recommendationDateTo)



     