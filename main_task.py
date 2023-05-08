
import time
import os
import requests
import re
import pandas as pd
from bs4 import BeautifulSoup
import sys
import json


"""creating error url 404"""
def status_log(r):
    """Pass response as a parameter to this function"""
    url_log_file = 'url_log.txt'
    if not os.path.exists(os.getcwd() + '\\' + url_log_file):
        with open(url_log_file, 'w') as f:
            f.write('url, status_code\n')
    with open(url_log_file, 'a') as file:
        file.write(f'{r.url}, {r.status_code}\n')

"""this fn will do if url gives 500error it will retry """
def retry(func, retries=10):
    """Decorator function"""
    retry.count = 0
    def retry_wrapper(*args, **kwargs):
        attempt = 0
        while attempt < retries:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                attempt += 1
                total_time = attempt * 10
                print(f'Retrying {attempt}: Sleeping for {total_time} seconds, error: ', e)
                time.sleep(total_time)
            if attempt == retries:
                retry.count += 1
                url_log_file = 'url_log.txt'
                if not os.path.exists(os.getcwd() + '\\' + url_log_file):
                    with open(url_log_file, 'w') as f:
                        f.write('url, status_code\n')
                with open(url_log_file, 'a') as file:
                    file.write(f'{args[0]}, requests.exceptions.ConnectionError\n')
            # if retry.count == 3:
            #     print("Stopped after retries, check network connection")
            #     raise SystemExit

    return retry_wrapper

"""url to html content"""
@retry
def get_soup(url, headers):
    """returns the soup of the page when given with the of an url"""
    r = requests.get(url, headers=headers)
    r.encoding = r
    print(r.status_code)
    if r.status_code == 200:
        soup = BeautifulSoup(r.text, 'html.parser')
        return soup
    elif 499 >= r.status_code >= 400:
        print(f'client error response, status code {r.status_code} \nrefer: {r.url}')
        status_log(r)
    elif 599 >= r.status_code >= 500:
        print(f'server error response, status code {r.status_code} \nrefer: {r.url}')
        count = 1
        while count != 5:
            print('while', count)
            r = requests.get(url, headers=headers)
            r.encoding = r
            print('status_code: ', r.status_code)
            if r.status_code == 200:
                soup = BeautifulSoup(r.text, 'html.parser')
                return soup
                # print('done', count)
            else:
                print('retry ', count)
                count += 1
                # print(count * 2)
                time.sleep(count * 3)
    else:
        status_log(r)
        return None

"""string to bs4"""
def text_soup(text):
    return BeautifulSoup(text, 'html.parser')

"""unwanted spaces and nextlines remove"""
def strip_it(text):
    return re.sub('\s+', r' ', text)

"""completed url will write"""
def write_visited_log(url):
    with open(f'Visited_Urls.txt', 'a') as file:
        file.write(f'{url}\n')

"""reads completed urls"""
def read_visited_log(url):
    with open(f'Visited_Urls.txt', 'r') as file:
        text = file.read()
    return text

def scrapy_datas(base_url):
    file_name = 'Product_details'
    to_check_url_count = 100
    datas_dict_list = []
    start_time = time.time()
    """reading csv file (Asin, country)"""
    for loop_count, single_row in enumerate(pd.read_csv('Amazon Scraping.csv').iterrows(), start=1):
        """here it will check json is there or not and if file is there it will read old and store in variable ---- 
        if file is not there it will create new file"""
        if os.path.isfile(f'{file_name}.json'):
            with open(f'{file_name}.json', encoding="utf8") as json_file:
                datas_dict_list = json.load(json_file)
        row_datas = single_row[1].to_dict()
        Asin_code = row_datas['Asin']
        country_code = row_datas['country']
        # print(Asin_code, country_code)
        """urls"""
        url = base_url.replace("country",f"{country_code}").replace("asin",f"{Asin_code}")
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36'
        }
        # url = r'https://www.amazon.fr/dp/000108660X'
        print(url, loop_count)
        """checks url already completed or not"""
        text = ''
        if os.path.isfile(f'Visited_Urls.txt'):
            text = read_visited_log(url)
        if url not in text:
            """url request to html text"""
            try:
                url_datas = get_soup(url, headers=headers)
            except:
                url_datas = ""
            # print(url_datas)
            if "productTitle" in str(url_datas):
                """Product_title"""
                Product_title = url_datas.find('span', id='productTitle').text.strip()
                print(Product_title)
                """Image_URL"""
                if "imageBlockOuter" in str(url_datas):
                    Product_Image_URL = url_datas.find('div', id='imageBlockOuter').img['src']
                else:
                    Product_Image_URL = url_datas.find('div', id='imgTagWrapperId').img['src']
                """imgTagWrapperId"""
                print(Product_Image_URL)
                """Price"""
                if "a-unordered-list a-nostyle a-button-list a-horizontal" in str(url_datas):
                    try:
                        Price_of_the_Product = url_datas.find('ul', class_='a-unordered-list a-nostyle a-button-list a-horizontal')\
                            .find('span', class_='a-color-base').text.replace("à partir de","").strip()
                    except:
                        Price_of_the_Product = url_datas.find('ul', class_='a-unordered-list a-nostyle a-button-list a-horizontal')\
                            .find('span', class_='a-color-secondary').text.replace("à partir de", "").strip()
                else:
                    Price_of_the_Product = url_datas.find('span',
                                                          class_='a-price aok-align-center reinventPricePriceToPayMargin priceToPay')\
                        .find('span', class_='a-offscreen').text.replace("à partir de", "").strip()
                Price_of_the_Product = Price_of_the_Product.replace(" ‏","").replace(" ","").replace("  "," ")
                print(Price_of_the_Product)
                """Product_Details"""
                if "detailBullets_feature_div" in str(url_datas):
                    Product_Details = text_soup(str(url_datas.find('div', id='detailBullets_feature_div'))
                                                    .replace("<li"," • <li")).text.strip().strip('•').strip()
                else:
                    Product_Details = text_soup(str(url_datas.find('div', id='productDescription'))
                                                       .replace("<li", " • <li")).text.strip().strip('•').strip()
                Product_Details = Product_Details.replace(" ‏","").replace(" ","").replace("  "," ")
                print(Product_Details)
                """product details dict"""
                datas_dict = {
                    'Product_title': Product_title,
                    'Product_Image_URL': Product_Image_URL,
                    'Price_of_the_Product': Price_of_the_Product,
                    'Product_Details': Product_Details,
                    'Product_url': url
                }
                """will append in old dict variable and it appends in json file"""
                datas_dict_list.append(datas_dict)
                with open(f'{file_name}.json', 'w', encoding='utf-8') as file:
                    json.dump(datas_dict_list, file, ensure_ascii=False)
                write_visited_log(url)
        """start_time --- end time checks"""
        if to_check_url_count == int(loop_count):
            to_complete = time.time() - start_time
            print("to_complete", to_complete)
            with open(f'script_time.txt', 'a') as file:
                file.write(f'{to_check_url_count} urls to complete time {to_complete}\n')
            to_check_url_count += 100

if __name__ == '__main__':
    base_url = r'https://www.amazon.country/dp/asin'
    scrapy_datas(base_url)



