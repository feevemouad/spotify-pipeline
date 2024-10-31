{{ config(
    materialized='table'
) }}

with latest_month_data as (
  select * from {{ source('spotify_source', 'spotify_tracks') }}
  where extraction_date >= date_trunc('month', current_date) - interval '1 month'
)

select
    date_trunc('day', extraction_date) as day,
    avg(danceability) as avg_danceability,
    avg(energy) as avg_energy,
    avg(tempo) as avg_tempo,
    avg(loudness) as avg_loudness,
    avg(valence) as avg_valence,
    count(*) as track_count
from latest_month_data
group by day
order by day
