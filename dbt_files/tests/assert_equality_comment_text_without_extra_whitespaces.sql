-- Comment texts after removing following whitespace characters after first one
-- should be equal to the original comment text values
SELECT
    comment_text,
    trim(regexp_replace(comment_text, '\s+', ' ', 'g'))
FROM 
    silver.comments
WHERE 
    comment_text != trim(regexp_replace(comment_text, '\s+', ' ', 'g'))