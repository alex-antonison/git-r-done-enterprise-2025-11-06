WITH temp_cte AS (
    SELECT
        release_year,
        mpaa_rating,
        major_genres,
        title,
        worldwide_gross
    FROM {{ ref('stg_movies') }}
),

genre_totals AS (
    SELECT
        release_year,
        mpaa_rating,
        major_genres,
        SUM(worldwide_gross) AS total_worldwide_gross
    FROM temp_cte
    GROUP BY
        release_year,
        mpaa_rating,
        major_genres
),

highest_grossing_title AS (
    SELECT
        release_year,
        mpaa_rating,
        major_genres,
        title,
        worldwide_gross,
        ROW_NUMBER() OVER (
            PARTITION BY release_year, mpaa_rating, major_genres
            ORDER BY worldwide_gross DESC
        ) AS rank_within_genre
    FROM temp_cte
)

SELECT
    g.release_year,
    g.mpaa_rating,
    g.major_genres,
    g.total_worldwide_gross,
    t.title AS highest_grossing_title,
    t.worldwide_gross AS highest_grossing_title_gross
FROM genre_totals g
JOIN highest_grossing_title t
    ON g.release_year = t.release_year
    AND g.mpaa_rating = t.mpaa_rating
    AND g.major_genres = t.major_genres
    AND t.rank_within_genre = 1
ORDER BY
    g.release_year,
    g.mpaa_rating,
    g.total_worldwide_gross DESC;
