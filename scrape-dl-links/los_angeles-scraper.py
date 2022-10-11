from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

import csv, json, time
 
####### EDIT ME
driver_path = '/usr/bin/chromedriver'
platform = 'linux' # 'windows' | 'linux'
baseURL = "https://data.lacity.org/"
listURL = "browse?limitTo=datasets&q=Crime+Data+from&sortBy=alpha&tags=crime"
wait_sta = 12 # seconds
wait_dyn = 5 # seconds
city = 'los angeles' # output filename
maxcount = 2 # how many items to scrape
####### END EDIT

fname = city.replace(' ', '_')
option = webdriver.ChromeOptions()
## for windows
if platform == 'windows':
    option.add_argument('--disable-dev-sh-usage')
## for headless linux
elif platform == 'linux':
    option.add_argument('--headless')
    option.add_argument('--no-sandbox')
    option.add_argument('--disable-dev-shm-usage')
    option.add_argument('--disable-gpu')
    option.add_argument('--disable-extensions')
else:
    raise NameError('invalid platform specified') 

driver = webdriver.Chrome(service=ChromeService(driver_path), options=option)

# get static list
driver.get(baseURL + listURL)
print('\nGetting and parsing list page HTML\n')
soup = BeautifulSoup(driver.page_source, 'html.parser')

totalScrapedInfo = []
results = soup.find_all('div', 'browse2-result')

for num in range(maxcount):
    anchor = results[num].find('a', 'browse2-result-name-link')
# enter each item link
    driver.get(anchor['href'])
    print(f'Getting and parsing {anchor.text} page HTML')
    export_button = driver.find_element(By.CSS_SELECTOR, '.btn.btn-simple.btn-sm.download')
    export_button.click()
    export_dialog = driver.find_element(By.ID, 'export-flannel')
    dl_anchor = export_dialog.find_element(By.CSS_SELECTOR, '[role="menuitem"]')
    scrapedInfo = {
        "city": city.title(),
        "dataset": anchor.text,
        "result_id": results[num]['data-view-id'],
        "homepage_url": anchor['href'],
        "download_url": dl_anchor.get_attribute('href'),
    }
    totalScrapedInfo.append(scrapedInfo)
    driver.save_screenshot(f'{fname}-{num}.png')
    print(scrapedInfo)
    print()
    time.sleep(wait_sta)

driver.quit()
# save to csv, json files

print('\nSaving to files\n')
with open(f'{fname}.csv', 'w') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=list(totalScrapedInfo[0].keys()))
    writer.writeheader()
    for item in totalScrapedInfo:
        writer.writerow(item)

with open(f'{fname}.json', 'w') as jsonfile:
    jsonfile.write(json.dumps(totalScrapedInfo))
