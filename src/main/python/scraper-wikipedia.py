from bs4 import BeautifulSoup
import requests
import pandas as pd

if __name__ == "__main__":
    # Get content of the article
    url = "https://en.wikipedia.org/wiki/List_of_doping_cases_in_athletics"
    webpage = requests.get(url)
    soup = BeautifulSoup(webpage.text, "html.parser")

    # Scrape the data we want
    tables = soup.find_all("table", class_="wikitable sortable")

    # Convert the tables of the page into a dataframe
    df = pd.read_html(str(tables))
    df = pd.DataFrame(pd.concat([df[d] for d in range(26)]))

    # Process the data into the format we want
    df.drop("Reference(s)", axis=1, inplace=True)
    df.reset_index(inplace=True, drop=True)

    # Convert the dataframe into a .CSV file
    df.to_csv("../data/List_of_doping_cases_in_athletics.csv")
