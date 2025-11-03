with

temp_cte AS (
    SELECT
        release_year,
        mpaa_rating,
        major_genres,
        worldwide_gross
    FROM {{ ref('stg_movies') }}
)


SELECT
    release_year,
    mpaa_rating,
    major_genres,
    sum(worldwide_gross) AS total_worldwide_gross
FROM temp_cte
GROUP BY
    release_year,
    mpaa_rating,
    major_genres
