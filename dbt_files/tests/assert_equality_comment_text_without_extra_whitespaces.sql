-- Comment texts after removing following whitespace characters after first one
-- should be equal to the original comment text values
SELECT
    1
FROM 
    silver.comments
WHERE 
    comment_text != trim(regexp_replace(comment_text, '\s+', '_', 'g'))