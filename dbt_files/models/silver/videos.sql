SELECT
    video_id,
    channel_name,
    video_title,
    published_at,
    view_count,
    like_count,
    comment_count,
    now()::timestamp(0) as created_at -- ADD CREATED_AT COLUMN
FROM 
    {{ source('bronze', 'videos') }}