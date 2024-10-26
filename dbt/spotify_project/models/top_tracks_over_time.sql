WITH track_frequency AS (
    SELECT
        track,
        artist,
        COUNT(*) AS appearance_count
    FROM
        {{ source('spotify_source', 'spotify_tracks') }}
    GROUP BY
        track, artist
)
SELECT
    track,
    artist,
    appearance_count
FROM
    track_frequency
ORDER BY
    appearance_count DESC
LIMIT 10
