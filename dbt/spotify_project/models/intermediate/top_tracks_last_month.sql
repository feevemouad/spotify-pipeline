{{ config(
    materialized='table'
) }}


with latest_month_data as (
  select * from {{ source('spotify_source', 'spotify_tracks') }}
  where extraction_date >= date_trunc('month', current_date) - interval '1 month'
)

select
    concat(track,' - ',artist) as track_artist,
    max(danceability) as max_danceability,
    max(energy) as max_energy
from  latest_month_data
group by track, artist
order by max_danceability desc
limit 10