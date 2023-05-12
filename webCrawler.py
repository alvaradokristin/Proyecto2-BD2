import csv
import requests
from bs4 import BeautifulSoup

urls = ["http://data.un.org/Data.aspx?d=WHO&f=MEASURE_CODE%3aWHS2_3070_all", "http://data.un.org/Data.aspx?d=WHO&f=MEASURE_CODE%3aWHS2_3070_cancer"]
filename = "WHOdata.csv"
    
for url in urls:
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
        writer.writerow(["Country", "Year", "Value"])
        writer.writerows(data)
        writer.writerow(["TABLE_END"])
    
