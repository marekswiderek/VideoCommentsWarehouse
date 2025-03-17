TRUNCATE TABLE bronze.videos, bronze.comments;

COPY bronze.videos(video_id, channel_name, video_title, published_at, view_count, like_count, comment_count)
FROM PROGRAM 'awk FNR-1 {{ task_instance.xcom_pull(task_ids='generate_source_files', key='return_value') }}*_video.csv | cat'
DELIMITER ',' CSV;

COPY bronze.comments(video_id, comment_author, comment_text, like_count, published_at, total_replies, is_public)
FROM PROGRAM 'awk FNR-1 {{ task_instance.xcom_pull(task_ids='generate_source_files', key='return_value') }}*_comments.csv | cat' 
DELIMITER ',' CSV;


