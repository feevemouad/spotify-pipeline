{{ config(
    materialized='table'
) }}

WITH recent_tracks AS (
    SELECT
        track,
        artist,
        extraction_date,
        ROW_NUMBER() OVER (PARTITION BY extraction_date ORDER BY extraction_date) AS position
    FROM {{ source('spotify_source', 'spotify_tracks') }}
    WHERE extraction_date >= CURRENT_DATE - INTERVAL '1 day'
), 

track_position_change as (SELECT
    track,
    artist,
    extraction_date,
    position,
    LAG(position) OVER (PARTITION BY track ORDER BY extraction_date) AS previous_position,
    COALESCE(position - LAG(position) OVER (PARTITION BY track ORDER BY extraction_date), 0) AS position_change
FROM recent_tracks
ORDER BY extraction_date, position)

SELECT
    concat(track,' - ',artist) as track_artist,
    extraction_date,
    position,
    previous_position,
    position_change
FROM track_position_change
WHERE extraction_date = CURRENT_DATE
