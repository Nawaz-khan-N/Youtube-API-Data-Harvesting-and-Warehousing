import streamlit as st
import pandas as pd
from googleapiclient.discovery import build
import pymongo as py
import sqlalchemy as sq
from pandas.io import sql
from sqlalchemy import create_engine, text
import pymysql
import isodate
from datetime import datetime, timezone
import datetime

# Connecting to YouTube API
api_key = 'AIzaSyBOmQQcbMkMCi0wqKjtt_LZBcQgrSgIMGA'
youtube = build('youtube', 'v3', developerKey=api_key)

# Connection to MongoDB

connect = py.MongoClient("mongodb+srv://Logan:Logan1116@mongo1.7lgrplr.mongodb.net/?retryWrites=true&w=majority")

# Creates or Connecting to a Database
db = connect['Capstudy']

# Creating or Connecting to a collection
col = db['Youtube']

dbb = connect.Capstudy
collection = dbb.Youtube



# Connection to MYSQL

user = 'root'
pwd = '3008'
host = 'localhost'
port = '3306'
db = 'YT'
engine = create_engine(f"mysql+pymysql://{user}:{pwd}@{host}:{port}/{db}")


def getComments(vId):
    request = youtube.commentThreads().list(
        part="snippet",
        videoId=vId
    )
    response = request.execute()

    # Getting count of total comments on video
    count = response['pageInfo']['totalResults']

    # Empty list to have all comments
    totalcomments = {}

    if count == 0:
        return totalcomments
    else:
        # Getting comments adding to dict
        for i in range(count):
            comment_info = {
                'Comment_Id': response['items'][i]['id'],
                'Comment_Text': response['items'][i]['snippet']['topLevelComment']['snippet']['textOriginal'],
                'Comment_Author': response['items'][i]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                'Comment_PublishedAt': response['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt']
            }
            # Variable to give each comment key name
            name = 'Comment_Id_' + str(i)
            totalcomments[name] = comment_info

    return totalcomments


def getVideos(vID):
    # Video Details based on Video ID
    request_video = youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id=vID
    )

    response_video = request_video.execute()

    response_Vd = response_video['items'][0]

    vid = response_Vd['id']
    vtitle = response_Vd['snippet']['title']

    if 'description' in response_Vd['snippet'].keys():
        vdesc = response_Vd['snippet']['description']
    else:
        vdesc = ''
    if 'tags' in response_Vd['snippet'].keys():
        vtag = response_Vd['snippet']['tags']
    else:
        vtag = ''
    vpub = response_Vd['snippet']['publishedAt']
    if 'viewCount' in response_Vd['statistics'].keys():
        vwc = response_Vd['statistics']['viewCount']
    else:
        vwc = 0
    if 'likeCount' in response_Vd['statistics'].keys():
        vlc = response_Vd['statistics']['likeCount']
    else:
        vlc = 0
    if 'dislikeCount' in response_Vd['statistics'].keys():
        vdc = response_Vd['statistics']['dislikeCount']
    else:
        vdc = 0
    if 'favoriteCount' in response_Vd['statistics'].keys():
        vfc = response_Vd['statistics']['favoriteCount']
    else:
        vfc = 0
    if 'commentCount' in response_Vd['statistics'].keys():
        vcc = response_Vd['statistics']['commentCount']
    else:
        vcc = 0
    vdur = response_Vd['contentDetails']['duration']
    if 'standard' in response_Vd['snippet']['thumbnails'].keys():
        vth = response_Vd['snippet']['thumbnails']['standard']['url']
    elif 'default' in response_Vd['snippet']['thumbnails'].keys():
        vth = response_Vd['snippet']['thumbnails']['default']['url']
    elif 'medium' in response_Vd['snippet']['thumbnails'].keys():
        vth = response_Vd['snippet']['thumbnails']['medium']['url']
    elif 'high' in response_Vd['snippet']['thumbnails'].keys():
        vth = response_Vd['snippet']['thumbnails']['high']['url']
    else:
        vth = ''
    if 'caption' in response_Vd['contentDetails'].keys():
        vcp = response_Vd['contentDetails']['caption']
    else:
        vcp = ''

    vid_stats = youtube.videos().list(
        part="statistics",
        id=vID
    ).execute()
    comment_count = vid_stats.get("items")[0].get("statistics").get("commentCount")

    #    if int(comment_count) >= 0:
    video_info = {
        "Video_Id": vid,
        "Video_Name": vtitle,
        "Video_Description": vdesc,
        "Tags": vtag,
        "PublishedAt": vpub,
        "View_Count": vwc,
        "Like_Count": vlc,
        "Dislike_Count": vdc,
        "Favorite_Count": vfc,
        "Comment_Count": vcc,
        "Duration": vdur,
        "Thumbnail": vth,
        "Caption_Status": vcp,
        "Comments": getComments(vid)
    }
    #    else:
    #       video_info = {
    #            "Video_Id": vid,
    #            "Video_Name": vtitle,
    #            "Video_Description": vdesc,
    #            "Tags": vtag,
    #            "PublishedAt": vpub,
    #            "View_Count": vwc,
    #            "Like_Count": vlc,
    #            "Dislike_Count": vdc,
    #            "Favorite_Count": vfc,
    #            "Comment_Count": vcc,
    #            "Duration": vdur,
    #            "Thumbnail": vth,
    #            "Caption_Status": vcp,
    #            "Comments": {}
    #            }

    return video_info


def getChannel(chlId):
    response = youtube.channels().list(
        id=chlId,
        part='snippet,statistics,contentDetails'
    )
    channel_data = response.execute()

    chnl = {}

    # Getting Channel Info
    chl_info = {
        'channel_name': channel_data['items'][0]['snippet']['title'],
        'channel_id': channel_data['items'][0]['id'],
        'subscription_count': channel_data['items'][0]['statistics']['subscriberCount'],
        'channel_views': channel_data['items'][0]['statistics']['viewCount'],
        'channel_description': channel_data['items'][0]['snippet']['description'],
        'playlists': channel_data['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
    }

    chnl['Channel_Name'] = chl_info

    # First we get playlist Id & from that we will retrive the video details
    request_playlist = youtube.playlistItems().list(
        part="snippet,contentDetails",
        maxResults=25,
        playlistId=channel_data['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    )
    response_playlist = request_playlist.execute()

    nextPageToken = response_playlist.get('nextPageToken')

    while ('nextPageToken' in response_playlist):
        nextPage = youtube.playlistItems().list(
            part="snippet",
            playlistId=channel_data['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
            maxResults="50",
            pageToken=nextPageToken
        ).execute()
        response_playlist['items'] = response_playlist['items'] + nextPage['items']

        if 'nextPageToken' not in nextPage:
            response_playlist.pop('nextPageToken', None)
        else:
            nextPageToken = nextPage['nextPageToken']

    # Getting Count of the total videos in the playlist
    vc = response_playlist['pageInfo']['totalResults']

    vds = {}

    if vc == 0:
        chnl['Video_Id_1'] = vds
    else:
        for i in range(vc):
            vedio_ID = response_playlist['items'][i]['snippet']['resourceId']['videoId']
            x = 'Video_Id_' + str(i + 1)
            chnl[x] = getVideos(vedio_ID)

    return chnl


def dateTrans(dt):
    return isodate.parse_duration(dt).total_seconds()


def convertDate(d):
    curr_date = d.replace("T", ' ')
    new_date = curr_date.replace('Z', '')
    return new_date


def getDetails(cId, data):
    x = data.shape[0]
    rw_c = data.shape[1]
    for i in range(x):
        if data.iat[i, 1]['channel_id'] == cId:
            chl_MDB = data['Channel_Name'][i]
            chl_df = pd.DataFrame.from_dict(chl_MDB, orient='index')
            chl_df = chl_df.transpose()
            chl_df = chl_df.drop(['playlists', 'subscription_count'], axis=1)
            chl_df = chl_df.assign(channel_status='Active')
            chl_df = chl_df.assign(channel_type='Education')
            chl_df = chl_df[['channel_id', 'channel_name', 'channel_type', 'channel_views', 'channel_description',
                             'channel_status']]

            vee_df = pd.DataFrame(columns=['Video_Id', 'Video_Name', 'Video_Description', 'Tags', 'PublishedAt',
                                           'View_Count', 'Like_Count', 'Dislike_Count', 'Favorite_Count',
                                           'Comment_Count',
                                           'Duration', 'Thumbnail', 'Caption_Status', 'Comments'])
            cd = data['Channel_Name'][i]['channel_id']
            for j in range(2, rw_c):
                ved_df = data.iat[i, j]
                v_df = pd.DataFrame.from_dict(ved_df, orient='index')
                v_df = v_df.transpose()
                vee_df = pd.concat([vee_df, v_df])
                xy = data.iat[i, j]['Duration']
                xyz = isodate.parse_duration(xy).total_seconds()
                vee_df['Duration'].replace(xy, xyz, inplace=True)
                vee_df['PublishedAt'] = vee_df['PublishedAt'].apply(convertDate)
                vee_df = vee_df.drop(['Tags', 'Comments'], axis=1)
                vee_df = vee_df.assign(Channel_Id=cd)
                vee_df = vee_df[['Video_Id', 'Channel_Id', 'Video_Name', 'Video_Description',
                                 'PublishedAt', 'View_Count', 'Like_Count', 'Dislike_Count',
                                 'Favorite_Count', 'Comment_Count', 'Duration', 'Thumbnail', 'Caption_Status']]

            cmtt_df = pd.DataFrame(
                columns=['Comment_Id', 'Comment_Text', 'Comment_Author', 'Comment_PublishedAt', 'Video_Id'])
            s = 0
            for k in range(2, rw_c):
                ct_df = data.iat[0, k]['Comments']
                s = len(data.iat[0, k]['Comments'].keys())
                if s == 0:
                    continue
                else:
                    for l in range(s):
                        t = 'Comment_Id_'
                        t = t + str(l)
                        ctt_df = ct_df[t]
                        c_df = pd.DataFrame.from_dict(ctt_df, orient='index')
                        c_df = c_df.transpose()
                        c_df['Video_Id'] = data.iat[0, k]['Video_Id']
                        cmtt_df = pd.concat([cmtt_df, c_df])
                        cmtt_df['Comment_PublishedAt'] = cmtt_df['Comment_PublishedAt'].apply(convertDate)
            # cmtt_df = [['Comment_Id', 'Video_Id', 'Comment_Text', 'Comment_Author','Comment_PublishedAt']]

    return chl_df, vee_df, cmtt_df


if "button1" not in st.session_state:
    st.session_state["button1"] = False

if "button2" not in st.session_state:
    st.session_state["button2"] = False

if "button3" not in st.session_state:
    st.session_state["button3"] = False

st.write("YT API DATA HARVESTING & WAREHOUSING")


x = st.text_input("Enter the Channel ID")

if st.button("Fetch"):
    st.session_state["button1"] = not st.session_state["button1"]
    st.write("Data is being fetched")
    xyz = getChannel(x)
    st.write(xyz)

if st.session_state["button1"]:
    if st.button("Insert to MongoDB"):
        st.session_state["button2"] = not st.session_state["button2"]
        xyz = getChannel(x)
        col.insert_one(xyz)
        st.write("Inserted to MongoDB Successfully")
        data = pd.DataFrame(list(collection.find()))
        st.write(data)


if st.session_state["button2"] and (not st.session_state["button1"]):
    st.session_state["button2"] = not st.session_state["button2"]

if st.session_state["button1"] and st.session_state["button2"]:
    inp = st.text_input("Enter Channel ID")
    if st.button("Insert to MYSQL"):
        # toggle button3 session state
        st.session_state["button3"] = not st.session_state["button3"]

        data = pd.DataFrame(list(collection.find()))
        c1 = data.shape[0]

        for i in range(c1):
            if data.iat[i, 1]['channel_id'] == inp:
                channel, video, comments = getDetails(inp, data)
                channel.to_sql('Channels', con=engine, if_exists='append', index=False)
                video.to_sql('Videos', con=engine, if_exists='append', index=False)
                comments.to_sql('Comments', con=engine, if_exists='append', index=False)
                st.write("Data Inserted to MY SQL Successfully")
            else:
                st.write("Enter Proper Channel")

if st.session_state["button3"] and (not st.session_state["button2"]):
    st.session_state["button3"] = not st.session_state["button3"]

if st.session_state["button3"]:
    genre = st.radio(
        "MYSQL Query",
        ["Sample Query", "Manual Query"],
        index=None,
        #    horizontal=st.session_state.horizontal,
    )
    if genre == "Sample Query" and st.session_state["button3"]:
        option = st.selectbox(
            "Select the Prewritten Queries",
            ("Select all channels", "Select all videos", "Select all comments"),
            index=None,
            placeholder="Select the sample query...",
        )

        st.write('You selected:', option)
    else:
        abc = st.text_input("Enter Query")
        abc = abc.lower()
        op = pd.read_sql_query(sql=text(abc), con=engine.connect())
        st.write(op)
