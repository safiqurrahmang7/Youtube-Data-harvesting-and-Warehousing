from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

def Api_connect():
    Api_Id="AIzaSyCBnexYflBZ2Jf6UGJsI1kHOiBPQKlvH8w"

    api_service_name="youtube"
    api_version="v3"

    youtube=build(api_service_name,api_version,developerKey=Api_Id)

    return youtube

youtube=Api_connect()

def get_channel_info(channel_id):
    request=youtube.channels().list(
                    part="snippet,ContentDetails,statistics",
                    id=channel_id
    )
    response=request.execute()

    for i in response['items']:
        data=dict(Channel_Name=i["snippet"]["title"],
                Channel_Id=i["id"],
                Subscribers=i['statistics']['subscriberCount'],
                Views=i["statistics"]["viewCount"],
                Total_Videos=i["statistics"]["videoCount"],
                Channel_Description=i["snippet"]["description"],
                Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data


def get_videos_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:
        response1=youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=Playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids


def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id
        )
        response=request.execute()

        for item in response["items"]:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                    Channel_Id=item['snippet']['channelId'],
                    Video_Id=item['id'],
                    Title=item['snippet']['title'],
                    Tags=item['snippet'].get('tags'),
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Description=item['snippet'].get('description'),
                    Published_Date=item['snippet']['publishedAt'],
                    Duration=item['contentDetails']['duration'],
                    Views=item['statistics'].get('viewCount'),
                    Likes=item['statistics'].get('likeCount'),
                    Comments=item['statistics'].get('commentCount'),
                    Favorite_Count=item['statistics']['favoriteCount'],
                    Definition=item['contentDetails']['definition'],
                    Caption_Status=item['contentDetails']['caption']
                    )
            video_data.append(data)    
    return video_data



def get_comment_info(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()

            for item in response['items']:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                        Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                
                Comment_data.append(data)
                
    except:
        pass
    return Comment_data



def get_playlist_details(channel_id):
        next_page_token=None
        All_data=[]
        while True:
                request=youtube.playlists().list(
                        part='snippet,contentDetails',
                        channelId=channel_id,
                        maxResults=50,
                        pageToken=next_page_token
                )
                response=request.execute()

                for item in response['items']:
                        data=dict(Playlist_Id=item['id'],
                                Title=item['snippet']['title'],
                                Channel_Id=item['snippet']['channelId'],
                                Channel_Name=item['snippet']['channelTitle'],
                                PublishedAt=item['snippet']['publishedAt'],
                                Video_Count=item['contentDetails']['itemCount'])
                        All_data.append(data)

                next_page_token=response.get('nextPageToken')
                if next_page_token is None:
                        break
        return All_data




client=pymongo.MongoClient("mongodb://localhost:27017")
db=client["Youtube_data"]

def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    pl_details=get_playlist_details(channel_id)
    vi_ids=get_videos_ids(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)

    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,
                      "video_information":vi_details,"comment_information":com_details})
    
    return "upload completed successfully"



def channels_table(channel_name_s):
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="safiq543",
                        database="youtube_data",
                        port="5432")
    cursor=mydb.cursor()

    try:
        create_query='''create table if not exists channels(Channel_Name varchar(100),
                                                            Channel_Id varchar(80) primary key,
                                                            Subscribers bigint,
                                                            Views bigint,
                                                            Total_Videos int,
                                                            Channel_Description text,
                                                            Playlist_Id varchar(80))'''
        cursor.execute(create_query)
        mydb.commit()

    except:
        print("Channels table already created")

    
    query_1= "SELECT * FROM channels"
    cursor.execute(query_1)
    table= cursor.fetchall()
    mydb.commit()

    chann_list= []
    chann_list2= []
    df_all_channels= pd.DataFrame(table)

    chann_list.append(df_all_channels)

    for i in chann_list[0]:
         chann_list2.append(i)
    

    if channel_name_s in chann_list2:
         news= f"Your Provided Channel {channel_name_s} is Already exists"        
         return news
    
    else:

        single_channel_details= []
        coll1=db["channel_details"]
        for ch_data in coll1.find({"channel_information.Channel_Name":channel_name_s},{"_id":0}):
            single_channel_details.append(ch_data["channel_information"])

        df_single_channel= pd.DataFrame(single_channel_details)



        for index,row in df_single_channel.iterrows():
            insert_query='''insert into channels(Channel_Name ,
                                                Channel_Id,
                                                Subscribers,
                                                Views,
                                                Total_Videos,
                                                Channel_Description,
                                                Playlist_Id)
                                                
                                                values(%s,%s,%s,%s,%s,%s,%s)'''
            values=(row['Channel_Name'],
                    row['Channel_Id'],
                    row['Subscribers'],
                    row['Views'],
                    row['Total_Videos'],
                    row['Channel_Description'],
                    row['Playlist_Id'])

            try:
                cursor.execute(insert_query,values)
                mydb.commit()

            except:
                print("Channel values are already inserted")


def playlist_table(channel_name_s):
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="safiq543",
                        database="youtube_data",
                        port="5432")
    cursor=mydb.cursor()

    create_query='''create table if not exists playlists(Playlist_Id varchar(100) primary key,
                                                        Title varchar(100),
                                                        Channel_Id varchar(100),
                                                        Channel_Name varchar(100),
                                                        PublishedAt timestamp,
                                                        Video_Count int
                                                        )'''

    cursor.execute(create_query)
    mydb.commit()

    single_channel_details= []
    coll1=db["channel_details"]
    for ch_data in coll1.find({"channel_information.Channel_Name":channel_name_s},{"_id":0}):
        single_channel_details.append(ch_data["playlist_information"])

    df_single_channel= pd.DataFrame(single_channel_details[0])

    for index,row in df_single_channel.iterrows():
        insert_query='''insert into playlists(Playlist_Id,
                                            Title,
                                            Channel_Id,
                                            Channel_Name,
                                            PublishedAt,
                                            Video_Count
                                            )
                                            
                                            values(%s,%s,%s,%s,%s,%s)'''
        
        values=(row['Playlist_Id'],
                row['Title'],
                row['Channel_Id'],
                row['Channel_Name'],
                row['PublishedAt'],
                row['Video_Count']
                )

        
        cursor.execute(insert_query,values)
        mydb.commit()

def videos_table(channel_name_s):
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="safiq543",
                        database="youtube_data",
                        port="5432")
    cursor=mydb.cursor()

    create_query='''create table if not exists videos(Channel_Name varchar(100),
                                                    Channel_Id varchar(100),
                                                    Video_Id varchar(30) primary key,
                                                    Title varchar(150),
                                                    Tags text,
                                                    Thumbnail varchar(200),
                                                    Description text,
                                                    Published_Date timestamp,
                                                    Duration interval,
                                                    Views bigint,
                                                    Likes bigint,
                                                    Comments int,
                                                    Favorite_Count int,
                                                    Definition varchar(10),
                                                    Caption_Status varchar(50)
                                                        )'''

    cursor.execute(create_query)
    mydb.commit()


    single_channel_details= []
    coll1=db["channel_details"]
    for ch_data in coll1.find({"channel_information.Channel_Name":channel_name_s},{"_id":0}):
        single_channel_details.append(ch_data["video_information"])

    df_single_channel= pd.DataFrame(single_channel_details[0])



    for index,row in df_single_channel.iterrows():
            insert_query='''insert into videos(Channel_Name,
                                                    Channel_Id,
                                                    Video_Id,
                                                    Title,
                                                    Tags,
                                                    Thumbnail,
                                                    Description,
                                                    Published_Date,
                                                    Duration,
                                                    Views,
                                                    Likes,
                                                    Comments,
                                                    Favorite_Count,
                                                    Definition,
                                                    Caption_Status
                                                )
                                                
                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        

            values=(row['Channel_Name'],
                    row['Channel_Id'],
                    row['Video_Id'],
                    row['Title'],
                    row['Tags'],
                    row['Thumbnail'],
                    row['Description'],
                    row['Published_Date'],
                    row['Duration'],
                    row['Views'],
                    row['Likes'],
                    row['Comments'],
                    row['Favorite_Count'],
                    row['Definition'],
                    row['Caption_Status']
                    )

            
            cursor.execute(insert_query,values)
            mydb.commit()


def comments_table(channel_name_s):
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="safiq543",
                        database="youtube_data",
                        port="5432")
    cursor=mydb.cursor()

    create_query='''create table if not exists comments(Comment_Id varchar(100) primary key,
                                                        Video_Id varchar(50),
                                                        Comment_Text text,
                                                        Comment_Author varchar(150),
                                                        Comment_Published timestamp
                                                        )'''

    cursor.execute(create_query)
    mydb.commit()

    single_channel_details= []
    coll1=db["channel_details"]
    for ch_data in coll1.find({"channel_information.Channel_Name":channel_name_s},{"_id":0}):
        single_channel_details.append(ch_data["comment_information"])

    df_single_channel= pd.DataFrame(single_channel_details[0])

    for index,row in df_single_channel.iterrows():
            insert_query='''insert into comments(Comment_Id,
                                                    Video_Id,
                                                    Comment_Text,
                                                    Comment_Author,
                                                    Comment_Published
                                                )
                                                
                                                values(%s,%s,%s,%s,%s)'''
            
            
            values=(row['Comment_Id'],
                    row['Video_Id'],
                    row['Comment_Text'],
                    row['Comment_Author'],
                    row['Comment_Published']
                    )

            
            cursor.execute(insert_query,values)
            mydb.commit()


def tables(channel_name):

    news= channels_table(channel_name)
    if news:
        st.write(news)
    else:
        playlist_table(channel_name)
        videos_table(channel_name)
        comments_table(channel_name)

    return "Tables Created Successfully"

def show_channels_table():
    ch_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_list)

    return df

def show_playlists_table():
    pl_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1=st.dataframe(pl_list)

    return df1

def show_videos_table():
    vi_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2=st.dataframe(vi_list)

    return df2

def show_comments_table():
    com_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3=st.dataframe(com_list)

    return df3







st.title("YOUTUBE DATA HARVESTING AND WAREHOUSING")


channel_id=st.text_input("Enter the channel ID")

if st.button("collect and store data"):
    ch_ids=[]
    
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_Id"])
        
   
    if channel_id in ch_ids:
        st.success("Channel Details of the given channel id already exists")

    else:
        insert=channel_details(channel_id)
        st.success(insert)


        
all_channels= []
coll1=db["channel_details"]
for ch_data in coll1.find({},{"_id":0,"channel_information":2}):
    all_channels.append(ch_data["channel_information"]["Channel_Name"])
        
unique_channel= st.selectbox("Select the Channel",all_channels)

if st.button("Migrate to Sql"):
    Table=tables(unique_channel)
    st.success(Table)

show_table=st.radio("SELECT TO VIEW JSON AND TABLE",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    ch_details = []
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_details.append(ch_data["channel_information"])

    st.json(ch_details)
    show_channels_table()

elif show_table=="PLAYLISTS":
    ch_details = []
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"playlist_information":1}).limit(2):
        ch_details.append(ch_data["playlist_information"])

    st.json(ch_details)
    show_playlists_table()

elif show_table=="VIDEOS":
    ch_details = []
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"video_information":1}).limit(2):
        ch_details.append(ch_data["video_information"])
    st.json(ch_details)
    show_videos_table()

elif show_table=="COMMENTS":
    ch_details = []
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"comment_information":1}).limit(2):
        ch_details.append(ch_data["comment_information"])
    st.json(ch_details)
    show_comments_table()