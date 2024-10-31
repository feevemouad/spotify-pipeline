{{ config(
    materialized='table'
) }}

select
    concat(track,' - ',artist) as track_artist,
    max(danceability) as max_danceability,
    max(energy) as max_energy
from {{ source('spotify_source', 'spotify_tracks') }}
group by track, artist
order by max_danceability desc
limit 10