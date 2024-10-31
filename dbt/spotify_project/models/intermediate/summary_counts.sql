{{ config(
    materialized='table'
) }}


with latest_month_data as (
  select * from {{ source('spotify_source', 'spotify_tracks') }}
  where extraction_date >= date_trunc('month', current_date) - interval '1 month'
),

daily_counts as (
    select
        date_trunc('day', extraction_date) as day,
        count(distinct artist) as unique_artists,    -- Count of unique artists
        count(*) as total_tracks                     -- Total track count
    from latest_month_data
    group by day
),

flattened_genres as (
    select
        date_trunc('day', extraction_date) as day,
        jsonb_array_elements_text(replace(genres, '''', '"')::jsonb) as genres
    from latest_month_data
    where jsonb_array_length(replace(genres, '''', '"')::jsonb) > 0  -- Exclude empty genre lists
),

unique_genres as (
    select
        day,
        count(distinct genres) as unique_genres  -- Count of unique genres per day
    from flattened_genres
    group by day
)

-- Combine counts into a single output
select
    d.day,
    d.unique_artists,
    d.total_tracks,
    u.unique_genres
from daily_counts d
left join unique_genres u on d.day = u.day
order by d.day