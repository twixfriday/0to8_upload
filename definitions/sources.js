[
    "tiktok_snaps",
    "spotify_tracks",
    "spotify_timeseries",
    "spotify_source_streams",
    "spotify_streams_by_country",
    "ep_release",
    "ep_timeseries",
    "tiktok_videos",
    "tiktok_hashtags",
    "tiktok_media_urls",
    "payment_operations"
].forEach(name =>
    declare({
        database: "dotted-cedar-473703-a1", // your GCP project id
        schema: "raw_tiktok", // dataset
        name: name // BigQuery table name
    })
);
