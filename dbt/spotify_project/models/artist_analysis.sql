with artist_aggregation as (
    select
        artist,
        avg(danceability) as avg_danceability,
        avg(energy) as avg_energy,
        count(*) as total_tracks
    from {{ source('spotify_source', 'spotify_tracks') }}
    group by artist
)

select * from artist_aggregation
where total_tracks > 5  -- Filter to show only artists with more than 5 tracks
