import streamlit as st
import psycopg2
import pandas as pd

mydb=psycopg2.connect(host="localhost",
                    user="postgres",
                    password="safiq543",
                    database="youtube_data",
                    port="5432")
cursor=mydb.cursor()

question=st.selectbox("Select your question",("All the videos and the channel name",
                                              "channels with most number of videos",
                                              "10 most viewed videos",
                                              "comments in each videos",
                                              "Videos with higest likes",
                                              "likes of all videos",
                                              "views of each channel",
                                              "videos published in the year of 2022",
                                              "average duration of all videos in each channel",
                                              "videos with highest number of comments"))

if question=="All the videos and the channel name":
    query='''select title as videos,channel_name as channelname from videos'''
    cursor.execute(query)
    mydb.commit()
    t1=cursor.fetchall()
    df=pd.DataFrame(t1,columns=["video title","channel name"])
    st.write(df)

elif question=="channels with most number of videos":
    query='''select channel_name as channelname,total_videos as no_videos from channels 
                order by total_videos desc'''
    cursor.execute(query)
    mydb.commit()
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2,columns=["channel name","No of videos"])
    st.write(df2)

elif question=="10 most viewed videos":
    query='''select views as views,channel_name as channelname,title as videotitle from videos 
                where views is not null order by views desc limit 10'''
    cursor.execute(query)
    mydb.commit()
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns=["views","channel name","videotitle"])
    st.write(df3)

elif question=="comments in each videos":
    query='''select comments as no_comments,title as videotitle from videos where comments is not null'''
    cursor.execute(query)
    mydb.commit()
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=["no of comments","videotitle"])
    st.write(df4)

elif question=="Videos with higest likes":
    query='''select title as videotitle,channel_name as channelname,likes as likecount
                from videos where likes is not null order by likes desc'''
    cursor.execute(query)
    mydb.commit()
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["videotitle","channelname","likecount"])
    st.write(df5)

elif question=="likes of all videos":
    query='''select likes as likecount,title as videotitle from videos'''
    cursor.execute(query)
    mydb.commit()
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=["likecount","videotitle"])
    st.write(df6)

elif question=="views of each channel":
    query='''select channel_name as channelname ,views as totalviews from channels'''
    cursor.execute(query)
    mydb.commit()
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=["channel name","totalviews"])
    st.write(df7)

elif question=="videos published in the year of 2022":
    query='''select title as video_title,published_date as videorelease,channel_name as channelname from videos
                where extract(year from published_date)=2022'''
    cursor.execute(query)
    mydb.commit()
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=["videotitle","published_date","channelname"])
    st.write(df8)

elif question=="average duration of all videos in each channel":
    query='''select channel_name as channelname,AVG(duration) as averageduration from videos group by channel_name'''
    cursor.execute(query)
    mydb.commit()
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9,columns=["channelname","averageduration"])

    T9=[]
    for index,row in df9.iterrows():
        channel_title=row["channelname"]
        average_duration=row["averageduration"]
        average_duration_str=str(average_duration)
        T9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))
    df1=pd.DataFrame(T9)
    st.write(df1)

elif question=="videos with highest number of comments":
    query='''select title as videotitle, channel_name as channelname,comments as comments from videos where comments is
                not null order by comments desc'''
    cursor.execute(query)
    mydb.commit()
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10,columns=["video title","channel name","comments"])
    st.write(df10)