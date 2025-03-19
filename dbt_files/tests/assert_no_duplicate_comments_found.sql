-- RETURN 1(FAIL THE TEST) IF THERE ARE DUPLICATED COMMENTS
-- CHECKING ONLY AUTHORS WITH MULTIPLE COMMENTS
SELECT
    1
FROM 
    bronze.comments
WHERE
    comment_author IN (
        SELECT 
            comment_author 
        FROM 
            bronze.comments 
        GROUP BY 
            comment_author 
        HAVING 
            count(comment_id) > 1
)
GROUP BY
    1
HAVING COUNT(comment_text) > COUNT(DISTINCT(comment_text))
