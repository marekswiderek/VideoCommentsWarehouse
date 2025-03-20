SELECT 
    ROW_NUMBER() OVER (ORDER BY v.video_id ASC) as video_id,
    v.video_id as youtube_video_id,
    v.channel_name,
    v.video_title,
    v.published_at,
    v.view_count,
    v.like_count,
    v.comment_count,
    {{ video_likes_percentage('v.view_count', 'v.like_count')}} as like_percentage
FROM 
    {{ ref('videos') }} as v