WITH ranked_tracks AS (
  SELECT
    track,
    artist,
    extraction_date,
    ROW_NUMBER() OVER (PARTITION BY extraction_date ORDER BY id ASC) AS rank
  FROM
    {{ source('spotify_source', 'spotify_tracks') }}
)
SELECT
  track,
  artist,
  extraction_date,
  rank
FROM
  ranked_tracks
ORDER BY
  extraction_date, rank
