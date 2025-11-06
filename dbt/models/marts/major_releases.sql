{{ config(
    materialized = 'table'
) }}

with staging_data as (

    select *
    from {{ ref('stg_movies') }}
),

rank_movies as (
    
    select title, 
        rank() over (order by worldwide_gross desc) as gross_rank
    from staging_data
)

select *
from rank_movies
where gross_rank <= 10
