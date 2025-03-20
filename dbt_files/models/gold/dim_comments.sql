SELECT 
    c.comment_id,
    c.comment_text,
    c.published_at,
    c.like_count,
    c.total_replies
FROM
    {{ ref('comments') }} as c
