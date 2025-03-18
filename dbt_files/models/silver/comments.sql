SELECT
    comment_id,
    video_id,
    comment_author,
    comment_text,
    like_count,
    published_at,
    total_replies,
    is_public,
    now()::timestamp(0) as created_at
FROM
    {{ source('bronze', 'comments') }}