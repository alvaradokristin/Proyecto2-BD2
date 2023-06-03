from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import ElementNotInteractableException
import csv
import requests
from bs4 import BeautifulSoup
import time
import hdfs

# Configuración del navegador
driver = webdriver.Chrome()

pagesToUse = {
    "http://data.un.org/DocumentData.aspx?id=444": "Intentional homicide victims by sex, counts and rates per 100,000 population",
    "http://data.un.org/DocumentData.aspx?id=443": "Intentional Homicide Victims by counts and rates per 100,000 population",
    "http://data.un.org/DocumentData.aspx?id=477": "Domestic Tourism",
    "http://data.un.org/DocumentData.aspx?id=459": "Employment tourism",
    "http://data.un.org/DocumentData.aspx?id=481": "Inbound Tourism",
    "http://data.un.org/DocumentData.aspx?id=458": "Outbound Tourism",
    "http://data.un.org/DocumentData.aspx?id=482": "Tourism Industries"
}

whoPages = {
    "http://data.un.org/Data.aspx?d=WHO&f=MEASURE_CODE%3aWHS2_3070_all": "Age-standardized mortality rate by cause (ages 30-70, per 100 000 population)-All causes",
    "http://data.un.org/Data.aspx?d=WHO&f=MEASURE_CODE%3aWHS2_3070_cancer": "Age-standardized mortality rate by cause (ages 30-70, per 100 000 population)-Cancer",
    "http://data.un.org/Data.aspx?d=WHO&f=MEASURE_CODE%3aWHS2_3070_cdd": "Age-standardized mortality rate by cause (ages 30-70, per 100 000 population)-Cardiovascular disease and diabetes",
    "http://data.un.org/Data.aspx?d=WHO&f=MEASURE_CODE%3aWHS2_3070_chronic": "Age-standardized mortality rate by cause (ages 30-70, per 100 000 population)-Chronic respiratory conditions",
    "http://data.un.org/Data.aspx?d=WHO&f=MEASURE_CODE%3aWHS2_513": "Age-standardized mortality rate by cause (per 100 000 population)-Communicable",
    "http://data.un.org/Data.aspx?d=WHO&f=MEASURE_CODE%3aWHS2_163": "Age-standardized mortality rate by cause (per 100 000 population)-Noncommunicable",
    "http://data.un.org/Data.aspx?d=WHO&f=MEASURE_CODE%3aWHS2_162": "Age-standardized mortality rate by cause (per 100 000 population)-Injuries",
    "http://data.un.org/Data.aspx?d=WHO&f=MEASURE_CODE%3aWHS9_88": "Population median age (years)",
    "http://data.un.org/Data.aspx?d=WHO&f=MEASURE_CODE%3aasfr1": "Adolescent fertility rate (per 1000 women)",
    "http://data.un.org/Data.aspx?d=WHO&f=MEASURE_CODE%3aimr": "Infant mortality rate (probability of dying between birth and age 1 per 1000 live births)",
    "http://data.un.org/Data.aspx?d=WHO&f=MEASURE_CODE%3atfr": "Total fertility rate (per woman)",
    "http://data.un.org/Data.aspx?d=WHO&f=MEASURE_CODE%3aWHS7_149": "General government expenditure on health as a percentage of total expenditure on health",
    "http://data.un.org/Data.aspx?d=WHO&f=MEASURE_CODE%3aWHOSIS_000001": "Life expectancy at birth (years)",
    "http://data.un.org/Data.aspx?d=WHO&f=MEASURE_CODE%3aWHS9_86": "Population in thousands total"
}


def saveCSVtoHDFS(filename):
    # Change the username
    client = hdfs.InsecureClient('http://localhost:9870', user='kag07')
    hdfs_path = '/input/csv/' + filename
    with open(filename, 'rb') as local_file:
        client.write(hdfs_path, data=local_file, overwrite=True)


def createCSV(type, data, url):
    if type == "who":
        filename = f"{whoPages[url].replace(' ', '_').replace(',_', '_').replace(',', '')}.csv"
    else:
        filename = f"{pagesToUse[url].replace(' ', '_').replace(',_', '_').replace(',', '').replace('(', '').replace(')', '')}.csv"

    # Save CSV to local file
    with open(filename, mode='w', newline="") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerows(data)

    # Upload CSV to HDFS
    # saveCSVtoHDFS(filename)


for url in pagesToUse.keys():
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find("table", attrs={"cellpadding": "0"})
    data = []
    rows = table.find_all("tr")
    partHomBySex = []
    stop = False

    # Find header rows and add them to data
    header_rows = table.find_all("tr", class_="rgHeader")
    for header_row in header_rows:
        cols = header_row.find_all("th")
        cols = [col.text.strip() for col in cols]
        data.append(cols)

    # Check if "homicide" is present in the title
    ignore_first_row = "homicide" in pagesToUse[url].lower()

    # Check if "sex" is present in the URL
    append_5_values = "sex" in pagesToUse[url].lower()

    for index, row in enumerate(rows):
        if index == 0 and ignore_first_row and not append_5_values:
            continue  # Skip the first row if "homicide" is present in the title

        if index == 0 and append_5_values:
            cols = row.find_all("td")  # Get the th elements of the first row
            cols = [col.text.strip() for col in cols]
            # Append the values to the second row in the first 5 columns
            partHomBySex += cols[:5]
            continue  # Skip adding the first row

        cols = row.find_all(["th", "td"])  # Include both th and td elements
        cols = [col.text.strip() for col in cols]

        if all(elements == '' for elements in cols):
            break

        if ignore_first_row and not append_5_values:
            # Delete the last 22 columnas of the Homicides csvs
            cols = cols[:-22]

        if append_5_values:
            # Delete the last 21 columnas of the Homicides csvs
            cols = cols[:-21]

        if len(partHomBySex) > 0:
            cols = partHomBySex + cols
            partHomBySex = []

        if cols:
            data.append(cols)

    createCSV("general", data, url)


for url, page_title in whoPages.items():
    # Load page
    driver.get(url)

    # Wait for the table to load completely.
    wait = WebDriverWait(driver, 70)
    table = wait.until(EC.presence_of_element_located(
        (By.CSS_SELECTOR, '#divData > div > table > tbody')))

    # Collect all the data in the table
    data = []
    headers_added = False  # Variable to check if headers have already been added

    while True:
        rows = table.find_elements(By.TAG_NAME, 'tr')

        for index, row in enumerate(rows):
            if not headers_added:
                # Add headers only in the first iteration
                headers = row.find_elements(By.TAG_NAME, 'th')
                header_data = [header.text for header in headers]
                data.append(header_data)
                headers_added = True
                continue

            cells = row.find_elements(By.TAG_NAME, 'td')
            if cells:
                row_data = [cell.text for cell in cells]
                data.append(row_data)

        try:
            # Click on the next arrow button if available.
            next_button = driver.find_element(By.CSS_SELECTOR, '#linkNextB')
            next_button.click()
            # Wait for the next page of the table to load.
            time.sleep(1)  # Add an additional one second of waiting
            wait.until(EC.staleness_of(table))
            table = wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, '#divData > div > table > tbody')))
            # Reset the variable to add the headers on the following page
            headers_added = True
        except ElementNotInteractableException:
            # Exit loop when there are no more pages to load
            break

    # Save data in a CSV file
    createCSV("who", data, url)

# Close the browser
driver.quit()
