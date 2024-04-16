



import json

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
}
@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2), tags=['scraper'])
def simple_scrape_taskflow_api():
    
    @task()
    def scrap():
        
        from bs4 import BeautifulSoup
        import requests
        url = 'https://www.scrapethissite.com/pages/forms/'
        page = requests.get(url)
        soup = BeautifulSoup(page.text, 'html')
        print(soup)
        soup.find('div')
        
        p_with_class_lead = soup.find('p', class_ = 'lead').text.strip()
        
        return {"p_with_class_lead":p_with_class_lead}
    
    scrape = scrap()
    
    
tutorial_etl_dag = simple_scrape_taskflow_api()
