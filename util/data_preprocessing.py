from googleapiclient.discovery import build
from dotenv import load_dotenv
from youtube_transcript_api import YouTubeTranscriptApi
from uuid import uuid4
import pandas as pd
import os
import re

class Preprocess:
    load_dotenv()
    API_KEY = os.getenv('API_KEY_1')

    def get_pos_and_video(id,videos):
        '''
        Returns the position of video in list and the video object.
        '''
        return list(filter(lambda x: x[1]["video_id"] == id,enumerate(videos)))[0]

    def delete_video_by_pos(id,videos):
        '''
        Deletes video by position in list
        '''
        video = Preprocess.get_pos_and_video(id,videos)
        position = video[0]
        del videos[position]

    def construct_subtitles(videos_subtitles):
        '''
        Stores subtitles in a list. Clears subtitles from words in parenthesis.
        '''
        subtitles = []
        for video_id,sbts in videos_subtitles[0].items():
            for s in sbts:
                text = re.sub("[\(\[].*?[\)\]]", "", s["text"])
                subtitle = dict(id=str(uuid4()),video_id=video_id,text=text.strip(),duration=s["duration"],start=s["start"])
                subtitles.append(subtitle)
        return subtitles

    def get_at_least_100_videos(query:str):
        '''
        Search at least 100 videos from Youtube API v3. \n
        Returns: \n
        - videos: A list with videos
        - video_ids: A list with video identifiers
        - ytb_service: The Youtube Service configuration
        '''
        ytb_service = build('youtube','v3',developerKey=Preprocess.API_KEY)
        forbidden_words = ['compilation','episodes','best moments','scenes','best scenes','moments','draw','doodle art','remixs','gta']
        videos = []
        video_ids = []
        
        search_videos_by_query = ytb_service.search()
        req_searched_videos = search_videos_by_query.list(part="snippet",maxResults=50,q=query,relevanceLanguage="en",regionCode="US")
        while len(videos) < 100:
            res_searched_videos = req_searched_videos.execute() 
            
            for i in range(len(res_searched_videos["items"])):
                title_words = res_searched_videos["items"][i]["snippet"]["title"].lower().split()
                num_of_forbidden_words = len(list(filter(lambda x: x in forbidden_words,title_words)))
                num_of_query_words = len(list(filter(lambda x: x in query.split(),title_words)))
                
                if "videoId" in res_searched_videos["items"][i]["id"].keys() and num_of_forbidden_words == 0 and num_of_query_words != 0:
                    video_id = res_searched_videos["items"][i]["id"]["videoId"]
                    title = res_searched_videos["items"][i]["snippet"]["title"]
                    video = dict()
                    video["video_id"] = video_id
                    video["title"] = title.strip()
                    video["link"] = f"https://www.youtube.com/watch?v={video_id}"
                    video_ids.append(video_id)
                    videos.append(video)
            
            req_searched_videos = search_videos_by_query.list_next(req_searched_videos,res_searched_videos)
            
        return videos,video_ids,ytb_service

    def search_videos(query:str):
        '''
        Search videos by query. \n
        Chooses cartoon videos with short duration and returns a tuple with two Pandas data frames: 
        - videos_df: Pandas data frame with cartoon videos
        - subtitles_df: Pandas data frame with subtitles of each cartoon video 
        '''
        videos,video_ids,ytb_service = Preprocess.get_at_least_100_videos(query)
    
        iter_videos = 0
        dividedBy50 = len(video_ids) % 50 == 0
        if dividedBy50:
            iter_videos = len(video_ids) / 50
        else:
            iter_videos = int(len(video_ids) / 50) + 1
            
        videos_service = ytb_service.videos()
        
        for idx in range(iter_videos):
            ids_str = str()
            if idx == iter_videos and not dividedBy50:
                ids_str = ",".join(video_ids[50*idx:len(video_ids)])
            else:
                ids_str = ",".join(video_ids[50*idx:50*(idx+1)])
            
            req_videos = videos_service.list(part="snippet,contentDetails,statistics",id=ids_str)
            videos_res = req_videos.execute()
                
            accepted_video_ids = []
            for item in videos_res["items"]:
                duration_str = item["contentDetails"]["duration"][2:]
                split_by_minutes = duration_str.split('M')
                
                if len(split_by_minutes) <= 1 or split_by_minutes[0].find('H') != -1 or split_by_minutes[0].find('D') != -1:
                    Preprocess.delete_video_by_pos(item["id"],videos)
                    continue
            
                durationMin = int(split_by_minutes[0])
                
                if durationMin > 8:
                    Preprocess.delete_video_by_pos(item["id"],videos)
                else: 
                    video = Preprocess.get_pos_and_video(item["id"],videos)
                    accepted_video_ids.append(video[1]["video_id"])
                    
                    if "viewCount" in item["statistics"].keys():
                        video[1]["views"] = item["statistics"]["viewCount"]
                    else:
                        video[1]["views"] = 0 
                    if "likeCount" in item["statistics"].keys():
                        video[1]["likes"] = item["statistics"]["likeCount"]
                    else:
                        video[1]["likes"] = 0 
        
        videos_subtitles = YouTubeTranscriptApi.get_transcripts(video_ids=accepted_video_ids, languages=['en'],continue_after_error=True)
        videos_without_subtitles = videos_subtitles[1]
            
        for id in videos_without_subtitles:
            Preprocess.delete_video_by_pos(id,videos)
        
        
        subtitles_df = pd.DataFrame(Preprocess.construct_subtitles(videos_subtitles))
        videos_df = pd.DataFrame(videos)
        
        #We want only videos with subtitles
        video_with_subs = subtitles_df["video_id"].unique()
        videos_df = videos_df[videos_df["video_id"].isin(video_with_subs)]
    
        return videos_df,subtitles_df
    
