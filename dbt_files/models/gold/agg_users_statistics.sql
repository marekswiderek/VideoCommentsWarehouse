SELECT
    user_id,
    count(comment_id) as total_comments_published,
    sum(like_count) as total_likes,
    min(published_at) as first_comment_published_at,
    max(published_at) as last_comment_published_at
FROM 
    {{ ref('fact_activities') }}
GROUP BY
    user_id
ORDER BY count(comment_id) DESC