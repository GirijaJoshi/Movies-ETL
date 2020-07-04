#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np
import json
import os
import re
from sqlalchemy import create_engine
import psycopg2
import time
from config import username, password


def clean_movie(movie):
    """
        This function is removing relernative title also renaming certain columns
    """
    
    movie = dict(movie) #create a non-destructive copy
    alt_titles = {}
    
    alternative_title = ['Also known as','Arabic','Cantonese','Chinese','French',
                'Hangul','Hebrew','Hepburn','Japanese','Literally',
                'Mandarin','McCune–Reischauer','Original title','Polish',
                'Revised Romanization','Romanized','Russian',
                'Simplified','Traditional','Yiddish']
    for key in alternative_title:
        if key in movie:
            alt_titles[key] = movie[key]
            movie.pop(key)
    if len(alt_titles) > 0:
        movie['alt_titles'] = alt_titles
    
    def change_column_name(old_name, new_name):
        if old_name in movie:
            movie[new_name] = movie.pop(old_name)
    
    change_column_name('Adaptation by', 'Writer(s)')
    change_column_name('Country of origin', 'Country')
    change_column_name('Directed by', 'Director')
    change_column_name('Distributed by', 'Distributor')
    change_column_name('Edited by', 'Editor(s)')
    change_column_name('Length', 'Running time')
    change_column_name('Original release', 'Release date')
    change_column_name('Music by', 'Composer(s)')
    change_column_name('Produced by', 'Producer(s)')
    change_column_name('Producer', 'Producer(s)')
    change_column_name('Productioncompanies ', 'Production company(s)')
    change_column_name('Productioncompany ', 'Production company(s)')
    change_column_name('Released', 'Release Date')
    change_column_name('Release Date', 'Release date')
    change_column_name('Screen story by', 'Writer(s)')
    change_column_name('Screenplay by', 'Writer(s)')
    change_column_name('Story by', 'Writer(s)')
    change_column_name('Theme music composer', 'Composer(s)')
    change_column_name('Written by', 'Writer(s)')
    
    return movie


# need a function to turn the extracted values into a numeric value.
def parse_dollars(s):
    # if s is not a string, return NaN
    if type(s) != str:
        return np.nan

    # if input is of the form $###.# million
    if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):

        # remove dollar sign and " million"
        s = re.sub('\$|\s|[a-zA-Z]','', s)

        # convert to float and multiply by a million
        value = float(s) * 10**6

        # return value
        return value

    # if input is of the form $###.# billion
    elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):

        # remove dollar sign and " billion"
        s = re.sub('\$|\s|[a-zA-Z]','', s)

        # convert to float and multiply by a billion
        value = float(s) * 10**9

        # return value
        return value

    # if input is of the form $###,###,###
    elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):

        # remove dollar sign and commas
        s = re.sub('\$|,','', s)

        # convert to float
        value = float(s)

        # return value
        return value

    # otherwise, return NaN
    else:
        return np.nan


# Function that fills in missing data for a column pair and then drops the redundant column.
def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
    df[kaggle_column] = df.apply(
        lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column]
        , axis=1)
    df.drop(columns=wiki_column, inplace=True)


class SQL_Database:
    def __init__(self, protocol, username, password, location, port, db):
        '''
        Store database credentials:
        Parameters:
            protocol - the protocol for the database
            username - database username
            password - database password
            location - the ip address for server location
            port     - port for connection
            db       - name of the database
        '''
        self.protocol = protocol
        self.username = username
        self.password = password
        self.location = location
        self.port = port
        self.db = db
        
    def connect(self):
        '''
        Lets setup a test for DB connection:
        Sets in Class object:
            engine - engine used to extract data
            conn   - connection value to verify connection
            cursor - the cursor item from the connection
        '''
        try:
            connection_string = f'{self.protocol}://{self.username}:{self.password}@{self.location}:{self.port}/{self.db}'  
            self.conn = psycopg2.connect(connection_string)
            self.cursor = self.conn.cursor()
            self.engine = create_engine(connection_string)
            print("Connected!")
            # print(pd.DataFrame(self.engine.table_names(), columns=["Tables in database:"]))
            return "Connected"
        except:
            print(f"Failed to connect to Database:{self.db}")
            return "Failed to connect to Database:{self.db}"
        
            
    def insert_or_create(self,data_df, table_name):
        '''
        This will insert a new table if the table does not exist.
        If the table does exist we will append to the table
        Parameters:
            data_df    - a dataframe of the data we are inserting
            table_name - name of the table we are inserting the data into
        '''
        print(f'Please wait adding data to table: {table_name}')
        check=self.engine.has_table(table_name)
        try:
            if check == False:
                # print("New table created")
                data_df.to_sql(name=table_name, con=self.engine, index=False)
            else:
                # print("Table appended too")
                data_df.to_sql(name=table_name, con=self.engine, index=False, if_exists="append")
            return True
        except sqlalchemy.exc.OperationalError as e:
            print('Error occured while executing a query {}'.format(e.args))
            return False
            
            
    def count_number_of_table_rows(self, table_name):
        '''
        This will count the number of rows inserted and will return the count
        '''
        postgreSQL_select_Query = f'select count(*) from {table_name}'
        self.cursor.execute(postgreSQL_select_Query)
        return self.cursor.fetchall()
        
        
    def close_database_connection(self):
        '''
        This will close the connection with database
        '''
        if(self.conn):
            self.cursor.close()
            self.conn.close()


def perform_etl(wiki_movies_raw_file_name, kaggle_metadata_file_name, ratings_file_name):

    """
    This function takes Wikidata from json, kaggle meta data from csv and ratings data file names with full path
    """
    protocol = 'postgres'
    location = 'localhost'
    port = '5432'
    db = 'movie_data'
    
    print('Please wait running the script')
    # wiki_movies_raw_file_name kaggle_metadata_file_name, ratings_file_name
    try:
        with open(wiki_movies_raw_file_name, "r") as file:
            wiki_movies_raw = json.load(file)
    except (FileNotFoundError, IOError):
        print("Error in finding a file or opening a file, exiting....")
        return
    
    # keeping only those movie where Director or Directed By is present and Also imdb_link and no episodes.
    wiki_movies = [movie for movie in wiki_movies_raw
                       if ('Director' in movie or 'Directed by' in movie)
                               and 'imdb_link' in movie
                               and 'No. of episodes' not in movie]
    
    # We can make a list of cleaned movies with a list comprehension:
    clean_movies = [clean_movie(movie) for movie in wiki_movies]
    
    # convert json file raw data to Data Frame
    try:
        wiki_movies_df = pd.DataFrame(clean_movies)
    except TypeError:
        print(f'Error in convertion json to DataFrame')
    
    # print(len(wiki_movies_df.columns.tolist()))
    
    # from imdb link removing all characters before actual number and creating new col
    wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
    
    # deleting duplicate imdb_ids
    wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)
    
    # Remove Mostly Null Columns
    wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < 
                            len(wiki_movies_df) * 0.9]
    
    # new colums
    wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]
    
    # create a list of Box Office column with no nas
    box_office = wiki_movies_df['Box office'].dropna() 
    
    # bos office is string so get those rows
    box_office[box_office.map(lambda x: type(x) != str)]
    
    # If box office row has a list then join the list 
    box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)
    
    # Some values are given as a range replace those with $
    box_office = box_office.str.replace(r'\$.*[-](?![a-z])', '$', regex=True)
    
    # regular expression for “$123.4 million” (or billion), and “$123,456,789.”
    # Some values have spaces in between the dollar sign and the number.
    form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'
    # Some values use a period as a thousands separator, not a comma.
    form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'
    
    # First, we need to extract the values from box_office using str.extract. 
    # Then we'll apply parse_dollars to the first column in the DataFrame returned by str.extract,
    wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', 
                                                          flags=re.IGNORECASE)[0].apply(parse_dollars)
    
    # all valyes are Nan, drop col
    wiki_movies_df.drop('Box office', axis=1, inplace=True)
    
    # drop na in Bugget
    budget = wiki_movies_df['Budget'].dropna()
    
    # if budget row is a list then join it by space
    budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x)
    
    # remove any values between a dollar sign and a hyphen 
    budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)

    wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
    
    # drop col Budget
    wiki_movies_df.drop('Budget', axis=1, inplace=True)
    
    # make a variable that holds the non-null values of Release date in the DataFrame, converting lists to strings:
    release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    
    # Reg Expression January 1, 2020 
    date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
    # Reg Expression for 2020-01-1
    date_form_two = r'\d{4}.[01]\d.[123]\d'
    # Reg Expression January 2020
    date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
    # Reg expression 2020
    date_form_four = r'\d{4}'
    
    # pass extracted dates to date time function
    wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(
        f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)
    # print(wiki_movies_df['release_date'])
    
    # make a variable that holds the non-null values of Running time also join if list
    running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    
    # time could be any format h-hour
    running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')
    # fill nana with 0
    running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)
    wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)
    
    # we can drop Running time from the dataset
    wiki_movies_df.drop('Running time', axis=1, inplace=True)
    
    # checking kaggle movie data
    try:
        kaggle_metadata = pd.read_csv(kaggle_metadata_file_name, low_memory=False)
    except (FileNotFoundError, IOError):
        print("Error in finding a file or opening a file, exiting....")
        return
    
    # remove bad data from adult
    kaggle_metadata[~kaggle_metadata['adult'].isin(['True','False'])]
    
    # keep rows where adult is False and drop adult column
    kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult',axis='columns')
   
    kaggle_metadata['video'] = kaggle_metadata['video'] == 'True'
    
    #convert each column with appropriate data type
    try:
        kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)
        kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')
        kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')
        kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])
    except ValueError:
        print('Failed to convert to data type')
    
    # print(kaggle_metadata.head())
    
    # merge wiki_movies_df and kaggle_metadata
    try:
        movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id', suffixes=['_wiki','_kaggle'])
    except pd.errors.MergeError as e:
        print(f'Merge Error:{e}')
    
    # drop row where release date wiki > 1996-01-01 and release_date_kaggle < 1965-01-01
    movies_df = movies_df.drop(movies_df[(movies_df['release_date_wiki'] > '1996-01-01') & 
                                     (movies_df['release_date_kaggle'] < '1965-01-01')].index)
    
    # if language is list tehn join and add value count
    movies_df['Language'].apply(lambda x: tuple(x) if type(x) == list else x).value_counts(dropna=False)
    movies_df['original_language'].value_counts(dropna=False)
    
    # First, we’ll drop the title_wiki, release_date_wiki, Language, and Production company(s) columns.
    movies_df.drop(columns=['title_wiki','release_date_wiki','Language','Production company(s)'], inplace=True)
    
    fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
    fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
    fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')
    
    # get cols
    movies_df = movies_df.loc[:, ['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                       'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                       'genres','original_language','overview','spoken_languages','Country',
                       'production_companies','production_countries','Distributor',
                       'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                      ]]
    
    # rename
    movies_df.rename({'id':'kaggle_id',
                  'title_kaggle':'title',
                  'url':'wikipedia_url',
                  'budget_kaggle':'budget',
                  'release_date_kaggle':'release_date',
                  'Country':'country',
                  'Distributor':'distributor',
                  'Producer(s)':'producers',
                  'Director':'director',
                  'Starring':'starring',
                  'Cinematography':'cinematography',
                  'Editor(s)':'editors',
                  'Writer(s)':'writers',
                  'Composer(s)':'composers',
                  'Based on':'based_on'
                 }, axis='columns', inplace=True)
    
    
    # Check the ratings file:
    try:
        ratings = pd.read_csv(ratings_file_name)
    except (FileNotFoundError, IOError):
        print(f"Error in finding a file or opening a file {ratings_file_name}, exiting....")
        return
    
    # group movie id and rating and count, rname userID to count and convert to pivot table
    try:
        rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count()                     .rename({'userId':'count'}, axis=1)                     .pivot(index='movieId',columns='rating', values='count')
    except TypeError:
        print(f'Error in groupby ratings movieId and rating')
    
    # append rating suffix to each col
    rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]
    # print(rating_counts)
    
    # merge movies_df and rating_counts. we need to use a left merge, since we want to keep everything in movies_df:
    movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')
    
    # there will be missing values instead of zeros. We have to fill those in ourselves
    movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)
    # print(movies_with_ratings_df.head())
    
    # connect to DB
    db_handle = SQL_Database(protocol, username, password, location, port, db)
    if 'Connected' not in db_handle.connect():
        print('Failed to connect to DB')
        return
    
    # append data tp table
    table_name = 'movies'
    if not db_handle.insert_or_create(movies_df, table_name):
        print(f'Failed to insert to table: {table_name}, exiting...')
        return
    
    print(f'Number of record appended:{db_handle.count_number_of_table_rows(table_name)}')
    
    # ratings is too big, so enting data in chucks
    rows_imported = 0
    start_time = time.time()
    table_name = 'ratings'
    try:
        
        for data in pd.read_csv(ratings_file_name, chunksize=1000000):

            # print out the range of rows that are being imported
            print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
            if db_handle.insert_or_create(data, table_name) == False:
                print(f'Failed to insert data into table: {table_name}, exiting....')
                return

            # increment the number of rows imported by the size of 'data'
            rows_imported += len(data)
            print(f"\nNumber of rows imported {db_handle.count_number_of_table_rows(table_name)}")

            # print that the rows have finished importing
            tot_secs = "{:.2f}".format(time.time() - start_time)
            print(f'Done. {tot_sec} total seconds elapsed')
    except (FileNotFoundError, IOError):
        print("Error in finding a file or opening a file, exiting....")
        return
    tot_secs = "{:.2f}".format(time.time() - start_time)
    print(f'Total time to import ratings data. {tot_secs} total seconds elapsed')
    
    db_handle.close_database_connection()


wiki_movies_raw_file_name = os.path.join(".", "wikipedia.movies.json")
kaggle_metadata_file_name = os.path.join(".", "3405_6663_bundle_archive", "movies_metadata.csv")
ratings_file_name = os.path.join(".", "3405_6663_bundle_archive", "ratings.csv")
perform_etl(wiki_movies_raw_file_name, kaggle_metadata_file_name, ratings_file_name)
