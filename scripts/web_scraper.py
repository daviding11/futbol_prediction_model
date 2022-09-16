import time
import requests
import pandas as pd
import time
from bs4 import BeautifulSoup
from random import randint
from time import sleep

class match_scraper:

    def __init__(self,year1,year2,league,url):
            self.year1 = year1
            self.year2 = year2
            self.league = league
            self.url = url

    def web_scraper(self):
        years = list(range(self.year1,self.year2,-1))
        standings_url = self.url
        all_matches = []

        for year in years:  
            data = requests.get(standings_url)
            soup = BeautifulSoup(data.text,features = "lxml")
            standings_table = soup.select('table.stats_table')[0]

            links = [l.get("href") for l in standings_table.find_all('a')]
            links = [l for l in links if '/squads/' in l]
            team_urls = [f"https://fbref.com{l}" for l in links]
        
            previous_season = soup.select("a.prev")[0].get("href")
            standings_url = f"https://fbref.com{previous_season}"
        
            for team_url in team_urls:
                team_name = team_url.split("/")[-1].replace("-Stats", "").replace("-", " ")
                data = requests.get(team_url)
                matches = pd.read_html(data.text, match="Scores & Fixtures")[0]
                soup = BeautifulSoup(data.text,features = "lxml")
                links = [l.get("href") for l in soup.find_all('a')]
                links = [l for l in links if l and 'all_comps/shooting/' in l]
                data = requests.get(f"https://fbref.com{links[0]}")
                shooting = pd.read_html(data.text, match="Shooting")[0]
                shooting.columns = shooting.columns.droplevel()
                #Shooting Data
                try:
                    team_data = matches.merge(shooting[["Date", "Sh", "SoT", "Dist", "FK", "PK", "PKatt"]], on="Date")
                except ValueError:
                    continue
                time.sleep(randint(1, 4))
                team_data["Season"] = year
                team_data["Team"] = team_name
                all_matches.append(team_data)
                print(team_name + " success " + f"{year}")
                time.sleep(randint(5, 10))
                #break

        match_df = pd.concat(all_matches)
        match_df.columns = [c.lower() for c in match_df.columns]
        match_df.to_csv(f"matches\{self.league}_matches.csv")

year1,year2 = 2022,2021
prem_matches = match_scraper(year1,year2,'premier',"https://fbref.com/en/comps/9/Premier-League-Stats")
ligue_1_matches = match_scraper(year1,year2,'ligue_1',"https://fbref.com/en/comps/13/Ligue-1-Stats")
serie_a_matches = match_scraper(year1,year2,'serie_a',"https://fbref.com/en/comps/11/Serie-A-Stats")
laliga_matches = match_scraper(year1,year2,'laliga',"https://fbref.com/en/comps/12/La-Liga-Stats")
bundesliga_matches = match_scraper(year1,year2,'bundesliga',"https://fbref.com/en/comps/20/Bundesliga-Stats")

prem_matches.web_scraper()
ligue_1_matches.web_scraper()
serie_a_matches.web_scraper()
laliga_matches.web_scraper()
bundesliga_matches.web_scraper()
