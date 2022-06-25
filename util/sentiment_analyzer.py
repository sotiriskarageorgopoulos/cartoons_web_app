from util.data_preprocessing import Preprocess
from textblob import TextBlob
from util.db_transactions import DBTransactions
import pandas as pd
import logging

class SentimentAnalyzer:
    def __init__(self,query):
        self.__query = query
    
    def analyze_videos_sentiments(self):
        '''
        Do sentiment analysis of videos and store them to SQLite database. \n
        Returns:
          - A list of videos.  
        '''
        logging.basicConfig(level = logging.INFO)
        query_exists = DBTransactions.is_exist_query(self.__query)
        
        if query_exists:
            return DBTransactions.fetch_videos_by_query(self.__query)
        else:
            videos_df,subtitles_df = Preprocess.search_videos(self.__query)
            print(videos_df,subtitles_df)
            if videos_df is not None and subtitles_df is not None:
                videos_ids = videos_df["video_id"].unique()
                is_exist_list, db_videos_ids = DBTransactions.is_exist_videos_list(videos_ids)
                if is_exist_list:
                    videos_df = SentimentAnalyzer.__calculate_sentiment_rating(videos_df,subtitles_df)
                    non_existed_videos = videos_df[~(videos_df["video_id"].isin(db_videos_ids))]
                    existed_videos = videos_df[videos_df["video_id"].isin(db_videos_ids)]
                    non_existed_videos_subs = subtitles_df[~(subtitles_df["video_id"].isin(db_videos_ids))]
                    existed_videos_norm_ratings = existed_videos["norm_rating"].values.tolist()
                    no_ex_videos_norm_ratings = non_existed_videos["norm_rating"].values.tolist()
                    existed_videos = existed_videos.loc[:,existed_videos.columns != "norm_rating"]
                    non_existed_videos = non_existed_videos.loc[:,non_existed_videos.columns != "norm_rating"]
                    
                    DBTransactions.insert(non_existed_videos,non_existed_videos_subs,no_ex_videos_norm_ratings,self.__query)
                    DBTransactions.insert_to_vid_queries(videos_ids,existed_videos_norm_ratings,self.__query)
                    return DBTransactions.fetch_videos_by_query(self.__query)
                else:
                    videos_df = SentimentAnalyzer.__calculate_sentiment_rating(videos_df,subtitles_df)
                    norm_ratings = videos_df["norm_rating"].values.tolist()
                    videos_df.drop(columns=['norm_rating'],inplace=True)
                    DBTransactions.insert(videos_df,subtitles_df,norm_ratings,self.__query)
                    return DBTransactions.fetch_videos_by_query(self.__query)
            return []
    
    def __calculate_sentiment_rating(videos_df,subtitles_df):
        '''
        Calculates sentiment rating of videos.
        '''
        subtitles_df["sentiment"] = subtitles_df["text"].apply(lambda t: TextBlob(t).sentiment.polarity)
        subtitles_df["rating"] = subtitles_df["sentiment"].mul(subtitles_df["duration"])
        videos_df = pd.merge(videos_df,subtitles_df.groupby('video_id')["rating"].sum(),on='video_id',how='inner')
        subtitles_df.drop(columns=['rating'],inplace=True)
        videos_df["norm_rating"] = (videos_df["rating"] - videos_df["rating"].min())/(videos_df["rating"].max() - videos_df["rating"].min())
        return videos_df  