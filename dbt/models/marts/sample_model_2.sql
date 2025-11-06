with

temp_cte AS (
    SELECT
        release_year as "Release Year",
        mpaa_rating as "MPAA Rating",
        major_genres as "Major Genres",
        worldwide_gross
    FROM {{ ref('stg_movies') }}
)

SELECT
    "Release Year",
    "MPAA Rating",
    "Major Genres",
    sum(worldwide_gross) AS "Total Worldwide Gross"
FROM temp_cte
GROUP BY
    "Release Year",
    "MPAA Rating",
    "Major Genres"
