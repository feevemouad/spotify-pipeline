with base as (
    select
        track,
        artist,
        energy,
        danceability,
        genres
    from {{ source('spotify_source', 'spotify_tracks') }}
),

genre_cleaning as (
    select
        track,
        artist,
        energy,
        danceability,
        regexp_replace(genres, '[\[\]\"]', '') as genres_cleaned  -- removing brackets and quotes from genres
    from base
),

track_scores as (
    select
        track,
        artist,
        (energy + danceability) / 2 as track_score  -- example score based on energy and danceability
    from genre_cleaning
)

select * from track_scores
