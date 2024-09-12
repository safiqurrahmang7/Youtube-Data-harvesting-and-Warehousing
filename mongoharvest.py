import pymongo
import Youtube_APIs as apis

client=pymongo.MongoClient("mongodb://localhost:27017")
db=client["Youtube_data"]

def channel_details(channel_id):
    ch_details = apis.get_channel_info(channel_id)
    pl_details = apis.get_playlist_details(channel_id)
    vi_ids = apis.get_videos_ids(channel_id)
    vi_details = apis.get_video_info(vi_ids)
    com_details = apis.get_comment_info(vi_ids)

    coll1 = db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,
                      "video_information":vi_details,"comment_information":com_details})
    
    return "upload completed successfully"
