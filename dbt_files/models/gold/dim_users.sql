SELECT
    ROW_NUMBER() OVER (ORDER BY uniq_auth.comment_author ASC) as user_id,
    uniq_auth.comment_author as username
FROM (
    SELECT 
        DISTINCT c.comment_author
    FROM 
        {{ ref('comments') }} as c
) as uniq_auth
