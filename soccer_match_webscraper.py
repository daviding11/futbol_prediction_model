import time
import requests
import pandas as pd
import time
from bs4 import BeautifulSoup
from random import randint
from time import sleep

# standings_url = "https://fbref.com/en/comps/9/11160/2021-2022-Premier-League-Stats"
## website URL
#standings_url = "https://fbref.com/en/comps/9/Premier-League-Stats"


def premier_league_web_scraper(y1, y2, url):
    years = list(range(y2, y1, -1))
    all_matches = []
    for year in years:
        data = requests.get(standings_url)
        soup = BeautifulSoup(data.text, 'html.parser')
        standings_table = soup.select('table.stats_table')[0]

        links = [l.get("href") for l in standings_table.find_all('a')]
        links = [l for l in links if '/squads/' in l]
        team_urls = [f"https://fbref.com{l}" for l in links]

        previous_season = soup.select("a.prev")[0].get("href")
        standings_url = f"https://fbref.com{previous_season}"
        #print(year)
        for i in range(len(team_urls)):
            team_name = team_urls[i].split("/")[-1].replace("-Stats", "").replace("-", " ")
            #print(team_name)
            data = requests.get(team_urls[i])
            time.sleep(randint(3, 9))
            # basic match data
            matches = pd.read_html(data.text, match="Scores & Fixtures")[0]
            matches.head()
            # use bs4 to grab links to all detailed match stats
            soup = BeautifulSoup(data.text, 'html.parser')
            links = soup.find_all('a')
            links = [l.get("href") for l in links]
            # shooting data pull
            sht_links = [l for l in links if l and 'all_comps/shooting/' in l]
            sht_data = requests.get(f"https://fbref.com{sht_links[0]}")
            shooting = pd.read_html(sht_data.text, match="Shooting ")[0]
            shooting.columns = shooting.columns.droplevel()
            try:
                team_data = matches.merge(shooting[["Date", "Sh", "SoT", "Dist", "FK", "PK", "PKatt"]], on="Date")
            except ValueError:
                continue
            time.sleep(randint(3, 9))
            # defense data pull
            def_links = [l for l in links if l and 'all_comps/defense/' in l]
            def_data = requests.get(f"https://fbref.com{def_links[0]}")
            defense = pd.read_html(def_data.text, match="Defensive Actions")[0]
            defense.columns = defense.columns.map('_'.join)  # map function to use first level as prefix
            defense.rename(columns={defense.columns[0]: 'Date'}, inplace=True)  # rename date column
            try:
                team_data = team_data.merge(
                    defense[["Date", "Pressures_Press", "Pressures_Succ", "Pressures_Def 3rd", "Pressures_Mid 3rd",
                             "Pressures_Att 3rd", "Blocks_Blocks", "Blocks_Sh", "Blocks_ShSv",
                             "Blocks_Pass"]], on="Date")
            except ValueError:
                continue
            time.sleep(randint(3, 9))
            # possession data pull
            pos_links = [l for l in links if l and 'all_comps/possession/' in l]
            pos_data = requests.get(f"https://fbref.com{pos_links[0]}")
            possession = pd.read_html(pos_data.text, match="Possession")[0]
            possession.columns = possession.columns.map('_'.join)  # map function to use first level as prefix
            possession.rename(columns={possession.columns[0]: 'Date'}, inplace=True)  # rename date column
            try:
                team_data = team_data.merge(possession[["Date", "Touches_Touches", "Touches_Def Pen", "Touches_Mid 3rd",
                                                        "Touches_Att 3rd", "Touches_Att Pen", "Dribbles_Succ",
                                                        "Dribbles_Att",
                                                        "Dribbles_#Pl", "Carries_Carries", "Carries_TotDist",
                                                        "Carries_PrgDist",
                                                        "Carries_Prog", "Carries_1/3", "Carries_CPA", "Receiving_Targ",
                                                        "Receiving_Rec", "Receiving_Prog"]], on="Date")
            except ValueError:
                continue
            # team_data = team_data[team_data["Comp"] == "Premier League"]
            team_data["Season"] = year
            team_data["Team"] = team_name
            all_matches.append(team_data)
            print(team_name + year + "success")
            time.sleep(randint(10, 15))
    print('extraction done!')
    return all_matches


def convert_to_csv(df):
    match_df = pd.concat(df)
    match_df.columns = [c.lower() for c in match_df.columns]
    match_df.to_csv('prem_team_matches.csv', index=False)
    print('csv created')

print('this is new')

