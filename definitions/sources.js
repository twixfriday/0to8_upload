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
    "payment_operations",
    "promo_exp",
    "promo_expenses",
    "payment_operation",
    "promo_releases",
    "promo_tracks",
    "snapshots",
    "creator_videos"
].forEach(name =>
    declare({
        database: "dotted-cedar-473703-a1",
        schema: "raw_data",
        name: name
    })
);
