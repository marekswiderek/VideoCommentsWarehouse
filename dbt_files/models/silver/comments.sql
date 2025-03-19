SELECT
    comment_id,
    video_id,
    comment_author,
    trim(regexp_replace(comment_text, '\s+', '_', 'g')) as comment_text, -- REMOVE EXTRA WHITESPACE CHARACTERS
    like_count,
    published_at,
    total_replies,
    is_public,
    now()::timestamp(0) as created_at -- ADD CREATED_AT COLUMN
FROM
    {{ source('bronze', 'comments') }}