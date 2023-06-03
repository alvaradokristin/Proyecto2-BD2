import csv
import requests
from bs4 import BeautifulSoup

# Tables with counts and rates
homicide1 = ["http://data.un.org/DocumentData.aspx?id=444"]
homicide2 = ["http://data.un.org/DocumentData.aspx?id=443"]

# 5 tables with tourism = domestic, employment, inbound, outbound, industries
turismo = ["http://data.un.org/DocumentData.aspx?id=477", "http://data.un.org/DocumentData.aspx?id=459", "http://data.un.org/DocumentData.aspx?id=481",
           "http://data.un.org/DocumentData.aspx?id=458", "http://data.un.org/DocumentData.aspx?id=482"]

whoUrls = ["http://data.un.org/Data.aspx?d=WHO&f=MEASURE_CODE%3aWHS2_3070_all", "http://data.un.org/Data.aspx?d=WHO&f=MEASURE_CODE%3aWHS2_3070_cancer",
           "http://data.un.org/Data.aspx?d=WHO&f=MEASURE_CODE%3aWHS2_3070_cdd", "http://data.un.org/Data.aspx?d=WHO&f=MEASURE_CODE%3aWHS2_3070_chronic",
           "http://data.un.org/Data.aspx?d=WHO&f=MEASURE_CODE%3aWHS2_513", "http://data.un.org/Data.aspx?d=WHO&f=MEASURE_CODE%3aWHS2_163",
           "http://data.un.org/Data.aspx?d=WHO&f=MEASURE_CODE%3aWHS2_162", "http://data.un.org/Data.aspx?d=WHO&f=MEASURE_CODE%3aWHS9_88",
           "http://data.un.org/Data.aspx?d=WHO&f=MEASURE_CODE%3aasfr1", "http://data.un.org/Data.aspx?d=WHO&f=MEASURE_CODE%3aimr",
           "http://data.un.org/Data.aspx?d=WHO&f=MEASURE_CODE%3atfr", "http://data.un.org/Data.aspx?d=WHO&f=MEASURE_CODE%3aWHS7_149",
           "http://data.un.org/Data.aspx?d=WHO&f=MEASURE_CODE%3aWHOSIS_000001"]

filename = "data.csv"
filename2 = "prueba.csv"

# Information from WHO (first part of urls with the same format)
for url in whoUrls:
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find("table", attrs={"cellpadding": "0"})

    data = []
    rows = table.find_all("tr")
    for row in rows:
        cols = row.find_all("td")
        cols = [col.text.strip() for col in cols]
        if cols:
            data.append(cols)

    # different URLs: 9, 10, 11 and 13. The rest follow the same format for the table (Country, Year and Value)
    with open(filename, mode='a', newline="") as f:
        writer = csv.writer(f)
        if (whoUrls.index(url) == 8):
            writer.writerow(["Country", "Year", "Education Level",
                            "Residence Area", "Wealth Quintile", "Value"])
        elif (whoUrls.index(url) == 9):
            writer.writerow(["Country", "Year", "Gender", "Education Level",
                            "Residence Area", "Wealth Quintile", "Value"])
        elif (whoUrls.index(url) == 10):
            writer.writerow(
                ["Country", "Year", "Education Level", "Wealth Quintile", "Value"])
        elif (whoUrls.index(url) == 12):
            writer.writerow(["Country", "Year", "Gender", "Value"])
        else:
            writer.writerow(["Country", "Year", "Value"])
        writer.writerows(data)
        writer.writerow(["TABLE_END"])

for url in homicide1:
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find("table", attrs={"cellpadding": "0"})
    data = []
    rows = table.find_all("tr")
    i = 1
    for row in rows:
        cols = row.find_all("td")
        cols = [col.text.strip() for col in cols]
        i += 1

        if i == 3 and cols:
            cols = [""] * 5 + cols
            data.append(cols)
        else:
            data.append(cols)

with open(filename, mode='a', newline="") as f:
    writer = csv.writer(f)
    writer.writerows(data)
    writer.writerow(["TABLE_END"])

for url in homicide2:
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find("table", attrs={"cellpadding": "0"})
    data = []
    rows = table.find_all("tr")
    for row in rows:
        cols = row.find_all("td")
        cols = [col.text.strip() for col in cols]
        if cols:
            data.append(cols)

with open(filename, mode='a', newline="") as f:
    writer = csv.writer(f)
    writer.writerows(data)
    writer.writerow(["TABLE_END"])

for url in turismo:
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find("table", attrs={"cellpadding": "0"})

    data = []
    rows = table.find_all("tr")
    for row in rows:
        cols = row.find_all("td")
        cols = [col.text.strip() for col in cols]
        if cols:
            data.append(cols)

    # different URLs: 9, 10, 11 and 13. The rest follow the same format for the table (Country, Year and Value)
    with open(filename, mode='a', newline="") as f:
        writer = csv.writer(f)
        if turismo.index(url) == 0:
            writer.writerow(["Domestic Tourism"])
        elif turismo.index(url) == 1:
            writer.writerow(["Employment Tourism"])
        elif turismo.index(url) == 2:
            writer.writerow(["Inbound Tourism"])
        elif turismo.index(url) == 3:
            writer.writerow(["Outbound Tourism"])
        else:
            writer.writerow(["Tourism Industries"])
        writer.writerows(data)
        writer.writerow(["TABLE_END"])
