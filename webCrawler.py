import csv
import requests
from bs4 import BeautifulSoup

# Tables with counts and rates
homicides = ["http://data.un.org/DocumentData.aspx?id=443", "http://data.un.org/DocumentData.aspx?id=444"]

# 5 tablas turismo = domestic, employment, inbound, outbound, industries
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
            
    #different URLs: 9, 10, 11 and 13. The rest follow the same format for the table (Country, Year and Value)
    with open(filename, mode='a', newline="") as f:
        writer = csv.writer(f)
        print(whoUrls.index(url))
        if (whoUrls.index(url) == 8): 
            writer.writerow(["Country", "Year", "Education Level", "Residence Area", "Wealth Quintile", "Value"])
        elif (whoUrls.index(url) == 9): 
            writer.writerow(["Country", "Year", "Gender", "Education Level", "Residence Area", "Wealth Quintile", "Value"])
        elif (whoUrls.index(url) == 10): 
            writer.writerow(["Country", "Year", "Education Level", "Wealth Quintile", "Value"])
        elif (whoUrls.index(url) == 12): 
            writer.writerow(["Country", "Year", "Gender", "Value"])
        else:
            writer.writerow(["Country", "Year", "Value"])
        writer.writerows(data)
        writer.writerow(["TABLE_END"])    
    
for url in homicides:
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find("table", attrs={"cellpadding": "0"})
    
    data = []
    rows = table.find_all("tr")
    for row in rows:
        cols = row.find_all("td")
        cols = [col.text.strip() for col in cols]
        if cols: data.append(cols)
    
with open(filename, mode='a' , newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["", "", "", "", "Number of intentional homicides", "Rates of intentional homicides"])
    writer.writerows(data)
    writer.writerow(["TABLE_END"])

