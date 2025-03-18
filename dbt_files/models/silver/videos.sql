SELECT
    video_id,
    channel_name,
    video_title,
    published_at,
    view_count,
    like_count,
    comment_count,
    now()::timestamp(0) as created_at
FROM 
    {{ source('bronze', 'videos') }}