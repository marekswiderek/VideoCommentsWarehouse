SELECT
    ROW_NUMBER() OVER (ORDER BY v.video_id, c.comment_id) as activity_id,
    c.comment_id,
    v.video_id,
    u.user_id,
    c.like_count,
    c.published_at
FROM 
    {{ ref('comments') }} as c
LEFT JOIN
    {{ ref('dim_videos') }} as v 
    ON c.video_id = v.youtube_video_id
LEFT JOIN 
    {{ ref('dim_users') }} as u
    ON c.comment_author = u.username
    