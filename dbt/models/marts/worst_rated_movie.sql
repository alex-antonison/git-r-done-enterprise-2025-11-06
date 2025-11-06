

SELECT title, imdb_rating
FROM {{ ref('stg_movies') }}
--ORDER BY imdb_rating ASC
--LIMIT 1
