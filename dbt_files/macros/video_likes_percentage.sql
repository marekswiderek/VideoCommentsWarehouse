-- Counts like percentage based on the total video views and likes count

{% macro video_likes_percentage(view_count, like_count, scale=2) %}
    round(({{ like_count }} * 100 / {{ view_count }}::numeric(10, {{scale}} )), 2)
{% endmacro %}