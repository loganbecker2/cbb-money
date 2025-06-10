# -*- coding: utf-8 -*-
"""
Created on Mon Dec 30 16:24:07 2024

@author: Logmo
"""

import pandas as pd
import time
import random
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine


#%% RUN FIRST
# BASE URL
base_url = "https://www.sports-reference.com/cbb"

# For now use seasons 2022, 2023, 2024, 2025
seasons = [2022,2023,2024,2025]
#%% Scraping web function
def scrape_cbb(url):
    print(f"Scraping: {url}")
    try:
        # Returns list of tables (In our case there is one table)
        df = pd.read_html(url, header=[1])
        
        # Get first table
        df = df[0]
        
        return df
   
    except Exception as e:
        print(f"Error scraping {url}: {e}")
        return None

#%% Create function to go through each season
def scrape_seasons(base_url, seasons):
    all_data = pd.DataFrame()
    
    for season in seasons:
        url = f"{base_url}/seasons/{season}-school-stats.html"
        df = scrape_cbb(url)
        df = cleanSeasonData(df)

        if df is not None:
            df['Season'] = season
            all_data = pd.concat([all_data, df], ignore_index=True)
        
        # Random delay between 3 and 6 seconds to avoid detection and keep within limits
        time.sleep(random.randint(3, 6))
        
    return all_data

#%% Create function to get each teams wins and losses data
def scrape_team_gamelog(base_url, seasons):
    all_data = pd.DataFrame()

    for season in seasons:
        schools = getSchoolList(season)
        for school in schools:
            url = f"{base_url}/schools/{school}/men/{season}-gamelogs.html"
            df = scrape_cbb(url)    
            
            if df is not None:
                df['Season'] = season
                df['School'] = school
                all_data = pd.concat([all_data, df], ignore_index=True)
                
            df = clean_gamelogs(df)
                
            # Random delay between 3 and 6 seconds to avoid detection and keep within limits
            time.sleep(random.randint(3, 6))
    
    return all_data
#%% Cleaning and preparing gamelog dataframe function    
def clean_gamelogs(df):    
    # delete index columns
    if 'Unnamed: 0' in df:
        df = df.drop('Unnamed: 0', axis = 1)
    if 'Rk' in df:
        df = df.drop('Rk', axis = 1)
    if 'Gtm' in df:
        df = df.drop('Gtm', axis = 1)
    # Delete date column
    if 'Date' in df:
        df = df.drop('Date', axis = 1)
    
    # change name of home or away column     Rename Reslt to Result     Tm to TeamPts     Opp to AwayPts
    df = df.rename(columns= {'Unnamed: 3': 'Location', 'Rslt': 'Result', 'Tm': 'TeamPts', 'Opp.1': 'AwayPts'})
    # change name of team and opponent stats
    df = df.rename(columns={
    # Team stats
    'FG': 'Team_FG', 'FGA': 'Team_FGA', 'FG%': 'Team_FG_Percentage',
    '3P': 'Team_ThreeP', '3PA': 'Team_ThreePA', '3P%': 'Team_ThreeP_Percentage',
    '2P': 'Team_TwoP', '2PA': 'Team_TwoPA', '2P%': 'Team_TwoP_Percentage',
    'eFG%': 'Team_eFG_Percentage', 'FT': 'Team_FT', 'FTA': 'Team_FTA',
    'FT%': 'Team_FT_Percentage', 'ORB': 'Team_ORB', 'DRB': 'Team_DRB',
    'TRB': 'Team_TRB', 'AST': 'Team_AST', 'STL': 'Team_STL', 'BLK': 'Team_BLK',
    'TOV': 'Team_TOV', 'PF': 'Team_PF',
    # Away stats
    'FG.1': 'Away_FG', 'FGA.1': 'Away_FGA', 'FG%.1': 'Away_FG_Percentage',
    '3P.1': 'Away_ThreeP', '3PA.1': 'Away_ThreePA', '3P%.1': 'Away_ThreeP_Percentage',
    '2P.1': 'Away_TwoP', '2PA.1': 'Away_TwoPA', '2P%.1': 'Away_TwoP_Percentage',
    'eFG%.1': 'Away_eFG_Percentage', 'FT.1': 'Away_FT', 'FTA.1': 'Away_FTA',
    'FT%.1': 'Away_FT_Percentage', 'ORB.1': 'Away_ORB', 'DRB.1': 'Away_DRB',
    'TRB.1': 'Away_TRB', 'AST.1': 'Away_AST', 'STL.1': 'Away_STL',
    'BLK.1': 'Away_BLK', 'TOV.1': 'Away_TOV', 'PF.1': 'Away_PF'
    })

    
    # change the values in the column
    df['Location'] = df['Location'].fillna('Home')
    df.loc[df['Location'].str.contains('@', na=False), 'Location'] = 'Away'
    df.loc[df['Location'].str.contains('N', na=False), 'Location'] = 'Neutral'
    
    # Change the value names of 'type' column
    df.loc[df['Type'].str.contains('REG (Conf)', na = False, regex = False), 'Type'] = 'Conference'
    df.loc[df['Type'].str.contains('REG (Non-Conf)', na = False, regex = False), 'Type'] = 'Non-Conference'
    df.loc[df['Type'].str.contains('CTOURN',na = False), 'Type'] = 'CTOURN' #PLACEHOLDER if we want to change it later
    
    # Change values in OT column so we don't have null values
    if df['OT'].isnull().all(): # If column is all nulls, just fill with 0
        df['OT'] = 0
    else:
        df['OT'] = df['OT'].astype(str)
        df.loc[df['OT'].str.contains('OT', na=False), 'OT'] = 1
        df['OT'] = pd.to_numeric(df['OT'], errors='coerce').fillna(0).astype(int)


    
    # Delete extra rows with column names
    df = df[df['Opp'] != 'Opp']
    
    # Delete last row, just totals for the season but we have season stats
    df = df[df['Opp'].notna()]
    
    return df

#%% Cleaning and preparing season dataframe function
def cleanSeasonData(df):
    # Drop 'unnamed' columns
    df = df.loc[:, ~df.columns.str.startswith('Unnamed')] # IDK if this is working tbh
    
    df = df.rename(columns={
        'W': 'W_Tot',
        'L': 'L_Tot',
        'W.1': 'W_Conf',
        'L.1': 'L_Conf',
        'W.2': 'W_Home',
        'L.2': 'L_Home',
        'W.3': 'W_Away',
        'L.3': 'L_Away',
        'Tm.': 'Tm_Pts',
        'Opp.': 'Opp_Pts',
        'SRS': 'Simple_Rating_System',
        'SOS': 'Strength_Of_Schedule',
        'W-L%': 'W_L_Percentage',
        'FG%': 'FG_Percentage',
        '3P': 'ThreeP',
        '3PA': 'ThreePA',
        '3P%': 'ThreeP_Percentage',
        'FT%': 'FT_Percentage'
    })
    
    # Delete first unnamed column (just an index column)
    if 'Unnamed: 0' in df.columns:
        df = df.drop('Unnamed: 0', axis = 1)   
    # Delete Rk column as well (another index column)
    if 'Rk' in df.columns:    
        df = df.drop('Rk', axis = 1)
    
    # Delete extra rows with column names
    df = df.dropna(subset='School') # Drop category row that is iterated
    df = df[df['School'] != 'School'] # Drop second reiterated column row with Rk value in Rk column
    
    return df
#%% Function to get list of all schools
def getSchoolList(season): # urlNeeeded is for when you need the school names to be changed for the url
    url = f'https://www.sports-reference.com/cbb/seasons/men/{season}-school-stats.html'
    print(f"Scraping for school list for season {season}")
    tables = pd.read_html(url, header=[1])
    
    # Extract the 'School' column and drop all Nan values
    schools_df = (tables[0][tables[0].columns[1]]).dropna()

    # Convert to list
    school_list = schools_df.tolist()
    
    school_list = [format_school_name(school) for school in school_list]
    
    # Remove 'school' from list (scrape grabs the 'school' headers)
    school_list = [school for school in school_list if school.lower() != 'school']
    
    # Random delay between 3 and 6 seconds to avoid detection and keep within limits
    time.sleep(random.randint(3, 6))
    
    print(f"Done scraping for school list, season {season}.")
    
    return school_list

#%% Function to format school names for the url
def format_school_name(school):
    school = school.lower()
    # Past seasons have ncaa in them for some teams
    school = school.replace('\xa0ncaa','')
    school = school.replace('(','')
    school = school.replace(')','')
    school = school.replace('&','')
    school = school.replace("'","")
    school = school.replace('.','')
    
    # Random school changes in their url
    school = school.replace('east texas am', 'texas-am-commerce')
    school = school.replace('fdu','fairleigh-dickinson')
    school = school.replace('houston christian','houston-baptist')
    school = school.replace('iu indy','iupui')
    school = school.replace('kansas city', 'missouri-kansas-city')
    school = school.replace('little rock','arkansas little-rock')
    if school == 'louisiana':
        school = school.replace('louisiana','louisiana-lafayette')
    school = school.replace('nc state','north-carolina-state')
    school = school.replace('omaha','nebraska-omaha')
    school = school.replace('purdue fort wayne','ipfw')
    school = school.replace('sam houston','sam-houston-state')
    school = school.replace('siu edwardsville','southern-illinois-edwardsville')
    school = school.replace('st thomas','st-thomas-mn')
    school = school.replace('tcu','texas-christian')
    school = school.replace('texas rio grande valley','texas-pan-american')
    school = school.replace('the citadel','citadel')
    school = school.replace('uab','alabama-birmingham')
    school = school.replace('uc davis','california-davis')
    school = school.replace('uc irvine','california-irvine')
    school = school.replace('uc riverside','california-riverside')
    school = school.replace('uc san diego','california-san-diego')
    school = school.replace('uc santa barbara','california-santa-barbara')
    school = school.replace('ucf','central-florida')
    school = school.replace('unc asheville','north-carolina-asheville')
    school = school.replace('unc greensboro','north-carolina-greensboro')
    school = school.replace('unc wilmington','north-carolina-wilmington')
    school = school.replace('ut arlington','texas-arlington')
    school = school.replace('utah tech','dixie-state')
    school = school.replace('utep','texas-el-paso')
    school = school.replace('utsa','texas-san-antonio')
    school = school.replace('vmi','virginia-military-institute')
    school = school.replace('william-mary','william-mary')
    school = school.replace(' ', '-')
    
    return school

#%% save into mysql database
def toSQL(df, databaseName):
    # Load .env file
    load_dotenv()
    
    engine = create_engine(f'mysql+mysqlconnector://{os.getenv("DB_USER")}:{os.getenv("DB_PASSWORD")}@{os.getenv("DB_HOST")}/{databaseName}')
    return df.to_sql(name=databaseName, con=engine, if_exists='append', index=False)

#%% Scrape overall cbb data
cbb_data = scrape_seasons(base_url, seasons)

# Save all seasons combined in one CSV
###cbb_data.to_csv('C:/Users/Logmo/cbb-money/DataFrames/Overall-Data/season_stats.csv', index=False)

# Save into mysql
print(f"Rows affected: {toSQL(cbb_data, 'season_stats')}")

print("Finished scraping and creating season stats dataframes.")

#%% Scrape team game logs (WILL TAKE 3+ HOURS) UNCOMMENT WHEN NEEDING TO RUN
all_teams_logs = scrape_team_gamelog(base_url, seasons)

# Save all team gamelogs for all seasons combined into one CSV
###all_teams_logs.to_csv('C:/Users/Logmo/cbb-money/DataFrames/Team-Gamelogs/all_team_gamelogs.csv', index=False)

# Save into mysql
print(f"Rows affected: {toSQL(all_teams_logs, 'gamelogs')}")

print("Finished scraping and creating dataframes for all team gamelogs.")
    
