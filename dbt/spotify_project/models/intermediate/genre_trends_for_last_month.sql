{{ config(
    materialized='table'
) }}

with latest_month_data as (
  select * from {{ source('spotify_source', 'spotify_tracks') }}
  where extraction_date >= date_trunc('month', current_date) - interval '1 month'
),

-- Flatten genres and exclude unclassified tracks
genre_flattened as (
    select
        date_trunc('day', extraction_date) as day,
        jsonb_array_elements_text(replace(genres, '''', '"')::jsonb) as genres  -- Convert genre to JSON and flatten
    from latest_month_data
    where jsonb_array_length(replace(genres, '''', '"')::jsonb) > 0  -- Exclude tracks with an empty genre list
)

-- Aggregate counts by genre and day
select
    day,
    genres,
    count(*) as track_count  -- Count the number of tracks per genre per day
from genre_flattened
group by day, genres
order by day, track_count desc
