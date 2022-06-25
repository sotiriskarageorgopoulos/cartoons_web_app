import sqlite3 as sq
import pandas as pd  
from uuid import uuid4
import logging
import os

class DBTransactions:
    def connect():
        '''
        Establishes connection with SQLite database \n
        Returns:
        - Connection.
        '''
        script_dir = os.path.dirname(__file__)
        db_path = os.path.join(script_dir, 'db/cartoons.db')
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        connection = sq.connect(db_path)
        cursor = connection.cursor()
        videos = """ CREATE TABLE IF NOT EXISTS videos
                           ([video_id] TEXT PRIMARY KEY, 
                            [title] TEXT NOT NULL,
                            [link] TEXT NOT NULL, 
                            [views] INTEGER NOT NULL,  
                            [likes] INTEGER NOT NULL, 
                            [rating] REAL NOT NULL)"""
        subtitles = """ CREATE TABLE IF NOT EXISTS subtitles
                       ([id] TEXT NOT NULL PRIMARY KEY,
                        [video_id] TEXT NOT NULL, 
                        [text] TEXT,
                        [duration] REAL NOT NULL, 
                        [start] REAL NOT NULL,
                        [sentiment] REAL NOT NULL,
                        FOREIGN KEY (video_id) REFERENCES video(video_id))"""
        user_queries = """ CREATE TABLE IF NOT EXISTS user_queries
                          ([query_id] TEXT NOT NULL PRIMARY KEY,
                           [query] TEXT NOT NULL
                          )
                       """
        queries_videos = """CREATE TABLE IF NOT EXISTS queries_videos
                         ([query_id] TEXT NOT NULL,
                          [video_id] TEXT NOT NULL,
                          [norm_rating] REAL NOT NULL,
                          FOREIGN KEY (video_id) REFERENCES videos(video_id),
                          FOREIGN KEY (query_id) REFERENCES user_queries(query_id)
                         )
                         """
        cursor.execute(videos)
        cursor.execute(subtitles)
        cursor.execute(user_queries)
        cursor.execute(queries_videos)
        connection.commit()
        return connection
    
    
    def insert(videos_df:pd.DataFrame,subtitles_df:pd.DataFrame,norm_ratings:list,query:str):
        '''
        Insert rows to videos, subtitles, queries_videos and user_queries tables.
        Returns:
        - True -> The rows were inserted successfully in the database.
        - False -> The rows were not inserted successfully in the database.
        '''
        logging.basicConfig(level = logging.ERROR)
        con = DBTransactions.connect()
        try:    
            videos_df.to_sql('videos', con, if_exists = 'append', index = False)
            subtitles_df.to_sql('subtitles', con, if_exists = 'append', index = False)
            cursor = con.cursor()
            query_id = str(uuid4())
            cursor.execute("INSERT INTO user_queries(query_id,query) VALUES (?,?)",(query_id,query))
            con.commit()
            
            videos_ids = videos_df["video_id"].unique()
            qv_rows = []
            for idx,vid in enumerate(videos_ids):
                qv_rows.append((query_id,vid,norm_ratings[idx]))
            
            cursor.executemany("INSERT INTO queries_videos(query_id,video_id,norm_rating) VALUES(?,?,?)",qv_rows)
            con.commit()
            logging.info("The rows were inserted in database...")
        except:
            logging.error(f"Something went wrong on {DBTransactions.insert.__name__}...")
            logging.info("The rows were not inserted in database...")
        finally:
            con.close()
     
    def insert_to_vid_queries(videos_ids:list, norm_ratings:list,query:str):
        '''
        Insert videos to table queries_videos, which exist in another query.
        Arguments:
        - videos_ids: A list with existed videos in database.
        - norm_ratings: Normalized ratings for this query.
        - query: The user's query.
        '''
        logging.basicConfig(level = logging.ERROR)
        con = DBTransactions.connect()
        try:
            cursor = con.cursor()
            cursor.execute(f"SELECT uq.query_id FROM user_queries AS uq WHERE uq.query = '{query}';")
            con.commit()
            query_id = cursor.fetchone()[0]
            print(query_id)
            
            query_ids = [query_id] * len(norm_ratings) 
            print(query_ids)
            video_ratings = list(zip(query_ids,videos_ids,norm_ratings))
        
            cursor.executemany("INSERT INTO queries_videos(query_id,video_id,norm_rating) VALUES(?,?,?)",video_ratings)
            con.commit()
            logging.info("The rows were inserted in database...")
        except:
            logging.error(f"Something went wrong on {DBTransactions.insert_to_vid_queries.__name__}...")
            logging.info("The rows were not inserted in database...")
        finally:
            con.close()
        
    def is_exist_query(query:str):
        '''
        Checks if the user's query exists in the database.
        The check is performed in order to reduce API Requests to Youtube API.
        Returns:
        - True -> If the query has been done by a user.
        - False -> If the query has not yet been done by a user.
        '''
        logging.basicConfig(level = logging.ERROR)
        con = DBTransactions.connect()
        try:
            cursor = con.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM user_queries WHERE query = '{query}' ;")
            is_exist = cursor.fetchone()[0] == 1
        except: 
            logging.error(f"Something went wrong on {DBTransactions.is_exist_query.__name__}...")
        finally:
            con.close()
        return is_exist
        
    def is_exist_videos_list(videos_ids:list):
        '''
        Checks if a video exists in the database.
        Returns a tuple of (is_exist,results):
        - is_exist -> If at least one video exists in database, then is_exist is True, and vice versa
        - results -> A list with videos ids 
        '''
        logging.basicConfig(level = logging.ERROR)
        con = DBTransactions.connect()
        try:
            cursor = con.cursor()
            vids_str = "('"+"','".join(videos_ids)+"')"
            cursor.execute(f"""SELECT v.video_id 
                            FROM videos AS v
                            WHERE v.video_id IN {vids_str};
                            """)
            results = [r[0] for r in cursor.fetchall()]
            is_exist = len(results) > 0
        except:
            logging.error(f"Something went wrong on {DBTransactions.is_exist_videos_list.__name__}...")
        finally:
            con.close()    
        return is_exist, results
            
    def fetch_videos_by_query(query:str):
        '''
        Fetch videos from database by the user's query.
        Returns:
        - results -> A list with tuples.
        '''
        con = DBTransactions.connect()
        try:
            cursor = con.cursor()
            cursor.execute(f"""
                        SELECT v.video_id,v.title,v.link,v.views,v.likes,v.rating,qv.norm_rating
                        FROM videos AS v
                        INNER JOIN queries_videos AS qv
                        ON v.video_id = qv.video_id
                        INNER JOIN user_queries AS uq
                        ON uq.query_id = qv.query_id
                        WHERE uq.query = '{query}'
                        ORDER BY qv.norm_rating, v.likes, v.views DESC;
                        """)
            con.commit()
        except:
            logging.error(f"Something went wrong on {DBTransactions.fetch_videos_by_query.__name__}...")   
            con.close()
        finally:
            results = cursor.fetchall()
            con.close()
            return results 
