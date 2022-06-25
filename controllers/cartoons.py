import sys
sys.path.append('../')
from util.sentiment_analyzer import SentimentAnalyzer
from flask import Flask,request,render_template
from flask_paginate import Pagination, get_page_args

if __name__ == '__main__':
    app = Flask(__name__,template_folder="../templates")
    videos = []
    
    def paginate_videos(videos,offset,per_page=5):
        return videos[offset:offset+per_page]
    
    @app.route("/",methods=["GET", "POST"])
    def main_page():
        global videos
        query = request.form.get('query')
        if query is not None: 
            s = SentimentAnalyzer(query)
            videos = s.analyze_videos_sentiments()
       
        page, per_page, offset = get_page_args(page_parameter='page',per_page_parameter='per_page')
        paginated_videos = paginate_videos(videos,offset)
        total_videos = len(videos)
        pagination = Pagination(page=page,per_page=per_page,total=total_videos,css_framework='bootstrap4')
            
        return render_template("index.html",videos=paginated_videos,pagination=pagination,page=page,per_page=per_page)
        
    app.run(debug=True, host='0.0.0.0', port=5000)