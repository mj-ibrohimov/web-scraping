import requests
import pandas as pd
from bs4 import BeautifulSoup
import db_conn  

url = 'https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films'
db_name = './ETL project/Movies.db'
table_name = 'Top_50'
csv_path = './ETL project/top_50_films.csv'

# Scraping and loading data into DataFrame
df = pd.DataFrame(columns=["Average Rank", "Film", "Year"])
count = 0
html_page = requests.get(url).text
data = BeautifulSoup(html_page, 'html.parser')

tables = data.find_all('tbody')
rows = tables[0].find_all('tr')

for row in rows:
    if count < 50:
        col = row.find_all('td')
        if len(col) != 0:
            data_dict = {"Average Rank": col[0].contents[0],
                         "Film": col[1].contents[0],
                         "Year": col[2].contents[0]}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
            count += 1
    else:
        break

print(df)

# Saving data to CSV
df.to_csv(csv_path)

# Using db_conn.py to handle the database operations
conn = db_conn.create_connection(db_name)  
if conn:
    db_conn.save_to_database(df, table_name, conn)  
    query_result = db_conn.read_from_database(table_name, conn)  
    print(query_result)  # Display the data from the database
    conn.close()  
