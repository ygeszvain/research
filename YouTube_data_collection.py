# !pip3 install --upgrade google-auth-oauthlib google-auth-httplib2 google-api-python-client
from datetime import timedelta, datetime
import ftplib
import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import logging
import os
from airflow.utils.email import send_email_smtp
from google.oauth2 import service_account
import googleapiclient.discovery
import pandas as pd
from datetime import datetime

today_date = datetime.now().strftime("%Y%m%d")


email_notification_recipient = ['yenyung@uwm.edu']


def task_success_callback(context):
    outer_task_success_callback(context, emails=email_notification_recipient)


def outer_task_success_callback(context, emails):
    subject = "[Airflow] DAG {0} - Task {1}: Success".format(
        context['task_instance_key_str'].split('__')[0],
        context['task_instance_key_str'].split('__')[1]
    )
    html_content = """
    DAG: {0}<br>
    Task: {1}<br>
    Ran on: {2}<br>
    """.format(
        context['task_instance_key_str'].split('__')[0],
        context['task_instance_key_str'].split('__')[1],
        datetime.now()
    )
    for email in emails:
        send_email_smtp(email, subject, html_content)


args = {
    'owner': 'Randy',
    'start_date': airflow.utils.dates.days_ago(2),
    'email': email_notification_recipient,
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=0)
}

dag = DAG(
    dag_id='YouTube_Data_Collection',
    default_args=args,
    schedule_interval='00 12 * * *',
    dagrun_timeout=timedelta(minutes=60),
)

def youtube_data_collection():
    videos_master_list = pd.read_csv("/Users/randygeszvain/Documents/Personal/Education/UWM Information Studies/projects (Jin Zhang)/CDC/YouTube/data_collection/videos_master_list.csv", index_col=False)

    SCOPES = ['https://www.googleapis.com/auth/youtube', 'https://www.googleapis.com/auth/plus.login']

    SERVICE_ACCOUNT_FILE = '/Users/randygeszvain/Documents/Personal/Education/UWM Information Studies/projects (Jin Zhang)/CDC/YouTube/data_collection/uwm-big-data-analytics-c2282cd307a6.json'

    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    api_service_name = "youtube"
    api_version = "v3"

    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, credentials=credentials)

    data = []
    pageToken = ""
    while True:
        request = youtube.activities().list(
            part="id,contentDetails,snippet",
            channelId="UCiMg06DjcUk5FRiM3g5sqoQ",
            maxResults=500,
            pageToken=pageToken if pageToken != "" else ""
        ).execute()
        v = request.get('items', [])
        if v:
            data.extend(v)
        pageToken = request.get('nextPageToken')
        if not pageToken:
            break
    activities = pd.json_normalize(data)
    videos_list = activities[['contentDetails.upload.videoId', 'snippet.description']].dropna().rename(
        columns={'contentDetails.upload.videoId': 'video_id', 'snippet.description': 'video_desc'}).drop_duplicates()
    videos_master_list = videos_master_list.append(videos_list).drop_duplicates()
    videos_master_list.to_csv("/Users/randygeszvain/Documents/Personal/Education/UWM Information Studies/projects (Jin Zhang)/CDC/YouTube/data_collection/videos_master_list.csv", index=False)

    videos = pd.DataFrame(columns=['kind',
                                'etag',
                                'id',
                                'snippet.publishedAt',
                                'snippet.channelId',
                                'snippet.title',
                                'snippet.description',
                                'snippet.thumbnails.default.url',
                                'snippet.thumbnails.default.width',
                                'snippet.thumbnails.default.height',
                                'snippet.thumbnails.medium.url',
                                'snippet.thumbnails.medium.width',
                                'snippet.thumbnails.medium.height',
                                'snippet.thumbnails.high.url',
                                'snippet.thumbnails.high.width',
                                'snippet.thumbnails.high.height',
                                'snippet.thumbnails.standard.url',
                                'snippet.thumbnails.standard.width',
                                'snippet.thumbnails.standard.height',
                                'snippet.channelTitle',
                                'snippet.tags',
                                'snippet.categoryId',
                                'snippet.liveBroadcastContent',
                                'snippet.localized.title',
                                'snippet.localized.description',
                                'snippet.defaultAudioLanguage',
                                'contentDetails.duration',
                                'contentDetails.dimension',
                                'contentDetails.definition',
                                'contentDetails.caption',
                                'contentDetails.licensedContent',
                                'contentDetails.projection',
                                'status.uploadStatus',
                                'status.privacyStatus',
                                'status.license',
                                'status.embeddable',
                                'status.publicStatsViewable',
                                'status.madeForKids',
                                'statistics.viewCount',
                                'statistics.likeCount',
                                'statistics.favoriteCount',
                                'player.embedHtml',
                                'topicDetails.topicCategories'])
    for video_id in videos_master_list['video_id'].tolist():

        data = []
        pageToken = ""
        while True:
            request = youtube.videos().list(
                part="contentDetails,id,liveStreamingDetails,player,recordingDetails,snippet,statistics,status,topicDetails",
                id=video_id
            ).execute()
            v = request.get('items', [])
            if v:
                data.extend(v)
            pageToken = request.get('nextPageToken')
            if not pageToken:
                break
        video_details = pd.json_normalize(data)
        videos = pd.concat([videos, video_details])

    now = datetime.now()
    videos['collect_timestamp'] = now.strftime("%Y%m%d%H%M%S")
    videos.to_csv(f'/Users/randygeszvain/Documents/Personal/Education/UWM Information Studies/projects (Jin Zhang)/CDC/YouTube/data_collection/videos_{now.strftime("%Y%m%d%H%M%S")}.csv', index=False)

youtube_data_collection_task = PythonOperator(
    task_id='youtube_data_collection',
    python_callable=youtube_data_collection,
    on_success_callback=task_success_callback,
    dag=dag
)

youtube_data_collection_task
