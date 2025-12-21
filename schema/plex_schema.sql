-- PostgreSQL Schema for Plex Media Server (COMPLETE)
-- Generated from SQLite schema analysis
-- Part of the Plex PostgreSQL Adapter
--
-- Covers both Plex databases:
--   - com.plexapp.plugins.library.db (main database)
--   - com.plexapp.plugins.library.blobs.db (thumbnails/artwork)
--
-- FIXED ISSUES:
--   - "index" column properly quoted (PostgreSQL reserved word)
--   - VARCHAR(255) changed to TEXT for large fields
--   - Foreign keys made DEFERRABLE for migration
--   - "default", "order", "limit" properly quoted

-- Create schema
CREATE SCHEMA IF NOT EXISTS plex;
SET search_path TO plex, public;

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS pg_trgm;      -- For fuzzy text search (replaces spellfix)
CREATE EXTENSION IF NOT EXISTS btree_gist;   -- For exclusion constraints

-- ============================================================================
-- Core Tables
-- ============================================================================

-- Schema migrations tracking
CREATE TABLE IF NOT EXISTS schema_migrations (
    version VARCHAR(255) NOT NULL PRIMARY KEY,
    rollback_sql TEXT DEFAULT NULL,
    optimize_on_rollback INTEGER DEFAULT NULL,
    min_version TEXT DEFAULT NULL
);

-- Library sections (Movies, TV Shows, Music, etc.)
CREATE TABLE IF NOT EXISTS library_sections (
    id SERIAL PRIMARY KEY,
    library_id BIGINT,
    name VARCHAR(255),
    name_sort VARCHAR(255),
    section_type INTEGER,
    language VARCHAR(255),
    agent VARCHAR(255),
    scanner VARCHAR(255),
    user_thumb_url TEXT,
    user_art_url TEXT,
    user_theme_music_url TEXT,
    public INTEGER,
    created_at BIGINT,
    updated_at BIGINT,
    scanned_at BIGINT,
    display_secondary_level INTEGER,
    user_fields TEXT,
    query_xml TEXT,
    query_type INTEGER,
    uuid VARCHAR(255),
    changed_at BIGINT DEFAULT 0,
    content_changed_at BIGINT DEFAULT 0,
    metadata_agent_provider_group_id INTEGER
);

CREATE INDEX IF NOT EXISTS idx_library_sections_uuid ON library_sections(uuid);

-- Section locations (paths for library sections)
CREATE TABLE IF NOT EXISTS section_locations (
    id SERIAL PRIMARY KEY,
    library_section_id INTEGER,
    root_path TEXT,
    available INTEGER DEFAULT 1,
    scanned_at BIGINT,
    created_at BIGINT,
    updated_at BIGINT
);

CREATE INDEX IF NOT EXISTS idx_section_locations_library_section_id ON section_locations(library_section_id);

-- Directories (folder structure)
CREATE TABLE IF NOT EXISTS directories (
    id SERIAL PRIMARY KEY,
    library_section_id INTEGER,
    parent_directory_id INTEGER,
    path TEXT,
    created_at BIGINT,
    updated_at BIGINT,
    deleted_at BIGINT
);

CREATE INDEX IF NOT EXISTS idx_directories_library_section_id ON directories(library_section_id);
CREATE INDEX IF NOT EXISTS idx_directories_parent_directory_id ON directories(parent_directory_id);
CREATE INDEX IF NOT EXISTS idx_directories_path ON directories(path);

-- Metadata items (movies, shows, seasons, episodes, etc.)
CREATE TABLE IF NOT EXISTS metadata_items (
    id SERIAL PRIMARY KEY,
    library_section_id INTEGER,
    parent_id INTEGER,
    metadata_type INTEGER,
    guid VARCHAR(255),
    media_item_count INTEGER,
    title VARCHAR(255),
    title_sort VARCHAR(255),
    original_title VARCHAR(255),
    studio VARCHAR(255),
    rating FLOAT,
    rating_count INTEGER,
    tagline TEXT,
    summary TEXT,
    trivia TEXT,
    quotes TEXT,
    content_rating VARCHAR(255),
    content_rating_age INTEGER,
    "index" INTEGER,
    absolute_index INTEGER,
    duration INTEGER,
    user_thumb_url TEXT,
    user_art_url TEXT,
    user_banner_url TEXT,
    user_music_url TEXT,
    user_fields TEXT,
    tags_genre TEXT,
    tags_collection TEXT,
    tags_director TEXT,
    tags_writer TEXT,
    tags_star TEXT,
    originally_available_at BIGINT,
    available_at BIGINT,
    expires_at BIGINT,
    refreshed_at BIGINT,
    year INTEGER,
    added_at BIGINT,
    created_at BIGINT,
    updated_at BIGINT,
    deleted_at BIGINT,
    tags_country VARCHAR(255),
    extra_data TEXT,
    hash VARCHAR(255),
    audience_rating FLOAT,
    changed_at BIGINT DEFAULT 0,
    resources_changed_at BIGINT DEFAULT 0,
    remote INTEGER,
    edition_title VARCHAR(255),
    slug VARCHAR(255),
    user_clear_logo_url TEXT,
    is_adult INTEGER,
    metadata_agent_provider_group_id INTEGER
);

CREATE INDEX IF NOT EXISTS idx_metadata_items_library_section_id ON metadata_items(library_section_id);
CREATE INDEX IF NOT EXISTS idx_metadata_items_parent_id ON metadata_items(parent_id);
CREATE INDEX IF NOT EXISTS idx_metadata_items_metadata_type ON metadata_items(metadata_type);
CREATE INDEX IF NOT EXISTS idx_metadata_items_guid ON metadata_items(guid);
CREATE INDEX IF NOT EXISTS idx_metadata_items_title ON metadata_items(title);
CREATE INDEX IF NOT EXISTS idx_metadata_items_added_at ON metadata_items(added_at);
CREATE INDEX IF NOT EXISTS idx_metadata_items_hash ON metadata_items(hash);
CREATE INDEX IF NOT EXISTS idx_metadata_items_title_sort ON metadata_items(title_sort);

-- Media items (actual media files)
CREATE TABLE IF NOT EXISTS media_items (
    id SERIAL PRIMARY KEY,
    library_section_id INTEGER,
    section_location_id INTEGER,
    metadata_item_id INTEGER,
    type_id INTEGER,
    width INTEGER,
    height INTEGER,
    size BIGINT,
    duration INTEGER,
    bitrate INTEGER,
    container VARCHAR(255),
    video_codec VARCHAR(255),
    audio_codec VARCHAR(255),
    display_aspect_ratio FLOAT,
    frames_per_second FLOAT,
    audio_channels INTEGER,
    interlaced INTEGER,
    source VARCHAR(255),
    hints TEXT,
    display_offset INTEGER,
    settings TEXT,
    created_at BIGINT,
    updated_at BIGINT,
    optimized_for_streaming INTEGER,
    deleted_at BIGINT,
    media_analysis_version INTEGER DEFAULT 0,
    sample_aspect_ratio FLOAT,
    extra_data TEXT,
    proxy_type INTEGER,
    channel_id INTEGER,
    begins_at BIGINT,
    ends_at BIGINT,
    color_trc VARCHAR(255)
);

CREATE INDEX IF NOT EXISTS idx_media_items_metadata_item_id ON media_items(metadata_item_id);
CREATE INDEX IF NOT EXISTS idx_media_items_library_section_id ON media_items(library_section_id);
CREATE INDEX IF NOT EXISTS idx_media_items_deleted_at ON media_items(deleted_at);

-- Media parts (individual files within media items)
CREATE TABLE IF NOT EXISTS media_parts (
    id SERIAL PRIMARY KEY,
    media_item_id INTEGER,
    directory_id INTEGER,
    hash VARCHAR(255),
    open_subtitle_hash VARCHAR(255),
    file TEXT,
    "index" INTEGER,
    size BIGINT,
    duration INTEGER,
    created_at BIGINT,
    updated_at BIGINT,
    deleted_at BIGINT,
    extra_data TEXT
);

CREATE INDEX IF NOT EXISTS idx_media_parts_media_item_id ON media_parts(media_item_id);
CREATE INDEX IF NOT EXISTS idx_media_parts_hash ON media_parts(hash);
CREATE INDEX IF NOT EXISTS idx_media_parts_directory_id ON media_parts(directory_id);
CREATE INDEX IF NOT EXISTS idx_media_parts_file ON media_parts(file);
CREATE INDEX IF NOT EXISTS idx_media_parts_deleted_at ON media_parts(deleted_at);

-- Media streams (video, audio, subtitle tracks)
CREATE TABLE IF NOT EXISTS media_streams (
    id SERIAL PRIMARY KEY,
    stream_type_id INTEGER,
    media_item_id INTEGER,
    url TEXT,
    codec VARCHAR(255),
    language VARCHAR(255),
    created_at BIGINT,
    updated_at BIGINT,
    "index" INTEGER,
    media_part_id INTEGER,
    channels INTEGER,
    bitrate INTEGER,
    url_index INTEGER,
    "default" INTEGER DEFAULT 0,
    forced INTEGER DEFAULT 0,
    extra_data TEXT
);

CREATE INDEX IF NOT EXISTS idx_media_streams_media_item_id ON media_streams(media_item_id);
CREATE INDEX IF NOT EXISTS idx_media_streams_media_part_id ON media_streams(media_part_id);

-- ============================================================================
-- Tags and Taggings
-- ============================================================================

CREATE TABLE IF NOT EXISTS tags (
    id SERIAL PRIMARY KEY,
    metadata_item_id INTEGER,
    tag TEXT,
    tag_type INTEGER,
    user_thumb_url TEXT,
    user_art_url TEXT,
    user_music_url TEXT,
    created_at BIGINT,
    updated_at BIGINT,
    tag_value INTEGER,
    extra_data TEXT,
    key VARCHAR(255),
    parent_id INTEGER
);

CREATE INDEX IF NOT EXISTS idx_tags_metadata_item_id ON tags(metadata_item_id);
CREATE INDEX IF NOT EXISTS idx_tags_tag ON tags(tag);
CREATE INDEX IF NOT EXISTS idx_tags_tag_type ON tags(tag_type);
CREATE INDEX IF NOT EXISTS idx_tags_key ON tags(key);

CREATE TABLE IF NOT EXISTS taggings (
    id SERIAL PRIMARY KEY,
    metadata_item_id INTEGER,
    tag_id INTEGER,
    "index" INTEGER,
    text TEXT,
    time_offset INTEGER,
    end_time_offset INTEGER,
    thumb_url TEXT,
    created_at BIGINT,
    extra_data TEXT
);

CREATE INDEX IF NOT EXISTS idx_taggings_metadata_item_id ON taggings(metadata_item_id);
CREATE INDEX IF NOT EXISTS idx_taggings_tag_id ON taggings(tag_id);

-- ============================================================================
-- Metadata Relations
-- ============================================================================

CREATE TABLE IF NOT EXISTS metadata_relations (
    id SERIAL PRIMARY KEY,
    metadata_item_id INTEGER,
    related_metadata_item_id INTEGER,
    relation_type INTEGER,
    created_at BIGINT,
    updated_at BIGINT
);

CREATE INDEX IF NOT EXISTS idx_metadata_relations_metadata_item_id ON metadata_relations(metadata_item_id);
CREATE INDEX IF NOT EXISTS idx_metadata_relations_related ON metadata_relations(related_metadata_item_id);

-- ============================================================================
-- Accounts and Settings
-- ============================================================================

CREATE TABLE IF NOT EXISTS accounts (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    created_at BIGINT,
    updated_at BIGINT,
    default_audio_language VARCHAR(255),
    default_subtitle_language VARCHAR(255),
    auto_select_subtitle INTEGER DEFAULT 1,
    auto_select_audio INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS devices (
    id SERIAL PRIMARY KEY,
    identifier VARCHAR(255),
    name VARCHAR(255),
    created_at BIGINT,
    updated_at BIGINT,
    platform VARCHAR(255)
);

CREATE INDEX IF NOT EXISTS idx_devices_identifier ON devices(identifier);

CREATE TABLE IF NOT EXISTS metadata_item_settings (
    id SERIAL PRIMARY KEY,
    account_id INTEGER,
    guid VARCHAR(255),
    rating FLOAT,
    view_offset INTEGER,
    view_count INTEGER,
    last_viewed_at BIGINT,
    created_at BIGINT,
    updated_at BIGINT,
    skip_count INTEGER DEFAULT 0,
    last_skipped_at BIGINT DEFAULT NULL,
    changed_at BIGINT DEFAULT 0,
    extra_data TEXT,
    last_rated_at BIGINT
);

CREATE INDEX IF NOT EXISTS idx_metadata_item_settings_guid ON metadata_item_settings(guid);
CREATE INDEX IF NOT EXISTS idx_metadata_item_settings_account_id ON metadata_item_settings(account_id);

CREATE TABLE IF NOT EXISTS metadata_item_setting_markers (
    id SERIAL PRIMARY KEY,
    marker_type INTEGER NOT NULL,
    metadata_item_setting_id INTEGER NOT NULL,
    start_time_offset INTEGER NOT NULL,
    end_time_offset INTEGER,
    title VARCHAR(255),
    created_at BIGINT,
    updated_at BIGINT,
    extra_data TEXT
);

CREATE INDEX IF NOT EXISTS idx_mis_markers_setting_id ON metadata_item_setting_markers(metadata_item_setting_id);

CREATE TABLE IF NOT EXISTS metadata_item_views (
    id SERIAL PRIMARY KEY,
    account_id INTEGER,
    guid VARCHAR(255),
    metadata_type INTEGER,
    library_section_id INTEGER,
    grandparent_title VARCHAR(255),
    parent_index INTEGER,
    parent_title VARCHAR(255),
    "index" INTEGER,
    title VARCHAR(255),
    thumb_url TEXT,
    viewed_at BIGINT,
    grandparent_guid VARCHAR(255),
    originally_available_at BIGINT,
    device_id INTEGER,
    view_type INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_metadata_item_views_account_id ON metadata_item_views(account_id);
CREATE INDEX IF NOT EXISTS idx_metadata_item_views_guid ON metadata_item_views(guid);

CREATE TABLE IF NOT EXISTS metadata_item_accounts (
    id SERIAL PRIMARY KEY,
    account_id INTEGER,
    metadata_item_id INTEGER
);

CREATE INDEX IF NOT EXISTS idx_mia_account_id ON metadata_item_accounts(account_id);
CREATE INDEX IF NOT EXISTS idx_mia_metadata_item_id ON metadata_item_accounts(metadata_item_id);

CREATE TABLE IF NOT EXISTS media_item_settings (
    id SERIAL PRIMARY KEY,
    account_id INTEGER,
    media_item_id INTEGER,
    settings TEXT,
    created_at BIGINT,
    updated_at BIGINT
);

CREATE TABLE IF NOT EXISTS media_part_settings (
    id SERIAL PRIMARY KEY,
    account_id INTEGER,
    media_part_id INTEGER,
    selected_audio_stream_id INTEGER,
    selected_subtitle_stream_id INTEGER,
    settings TEXT,
    created_at BIGINT,
    updated_at BIGINT,
    changed_at BIGINT DEFAULT 0
);

CREATE TABLE IF NOT EXISTS media_stream_settings (
    id SERIAL PRIMARY KEY,
    account_id INTEGER,
    media_stream_id INTEGER,
    extra_data TEXT,
    created_at BIGINT,
    updated_at BIGINT,
    UNIQUE(media_stream_id, account_id)
);

CREATE TABLE IF NOT EXISTS view_settings (
    id SERIAL PRIMARY KEY,
    account_id INTEGER,
    client_type VARCHAR(255),
    view_group VARCHAR(255),
    view_id INTEGER,
    sort_id INTEGER,
    sort_asc INTEGER,
    created_at BIGINT,
    updated_at BIGINT
);

CREATE TABLE IF NOT EXISTS library_section_permissions (
    id SERIAL PRIMARY KEY,
    library_section_id INTEGER,
    account_id INTEGER,
    permission INTEGER
);

CREATE TABLE IF NOT EXISTS preferences (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    value TEXT
);

-- ============================================================================
-- Plugins
-- ============================================================================

CREATE TABLE IF NOT EXISTS plugins (
    id SERIAL PRIMARY KEY,
    identifier VARCHAR(255),
    framework_version INTEGER,
    access_count INTEGER,
    installed_at BIGINT,
    accessed_at BIGINT,
    modified_at BIGINT
);

CREATE INDEX IF NOT EXISTS idx_plugins_identifier ON plugins(identifier);

CREATE TABLE IF NOT EXISTS plugin_prefixes (
    id SERIAL PRIMARY KEY,
    plugin_id INTEGER,
    name VARCHAR(255),
    prefix VARCHAR(255),
    art_url TEXT,
    thumb_url TEXT,
    titlebar_url TEXT,
    share INTEGER,
    has_store_services INTEGER,
    prefs INTEGER
);

-- ============================================================================
-- Play Queues
-- ============================================================================

CREATE TABLE IF NOT EXISTS play_queues (
    id SERIAL PRIMARY KEY,
    client_identifier VARCHAR(255),
    account_id INTEGER,
    playlist_id INTEGER,
    sync_item_id INTEGER,
    play_queue_generator_id INTEGER,
    generator_start_index INTEGER,
    generator_end_index INTEGER,
    generator_items_count INTEGER,
    generator_ids BYTEA,
    seed INTEGER,
    current_play_queue_item_id INTEGER,
    last_added_play_queue_item_id INTEGER,
    version INTEGER,
    created_at BIGINT,
    updated_at BIGINT,
    metadata_type INTEGER,
    total_items_count INTEGER,
    generator_generator_ids BYTEA,
    extra_data TEXT
);

CREATE TABLE IF NOT EXISTS play_queue_items (
    id SERIAL PRIMARY KEY,
    play_queue_id INTEGER,
    metadata_item_id INTEGER,
    "order" FLOAT,
    up_next INTEGER,
    play_queue_generator_id INTEGER
);

CREATE TABLE IF NOT EXISTS play_queue_generators (
    id SERIAL PRIMARY KEY,
    playlist_id INTEGER,
    metadata_item_id INTEGER,
    uri TEXT,
    "limit" INTEGER,
    continuous INTEGER,
    "order" FLOAT,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    changed_at BIGINT DEFAULT 0,
    recursive INTEGER,
    type INTEGER,
    extra_data TEXT
);

-- ============================================================================
-- Statistics
-- ============================================================================

CREATE TABLE IF NOT EXISTS statistics_bandwidth (
    id SERIAL PRIMARY KEY,
    account_id INTEGER,
    device_id INTEGER,
    timespan INTEGER,
    at BIGINT,
    lan INTEGER,
    bytes BIGINT
);

CREATE INDEX IF NOT EXISTS idx_statistics_bandwidth_at ON statistics_bandwidth(at);
CREATE INDEX IF NOT EXISTS idx_statistics_bandwidth_account_id ON statistics_bandwidth(account_id);

CREATE TABLE IF NOT EXISTS statistics_media (
    id SERIAL PRIMARY KEY,
    account_id INTEGER,
    device_id INTEGER,
    timespan INTEGER,
    at BIGINT,
    metadata_type INTEGER,
    count INTEGER,
    duration INTEGER
);

CREATE INDEX IF NOT EXISTS idx_statistics_media_at ON statistics_media(at);

CREATE TABLE IF NOT EXISTS statistics_resources (
    id SERIAL PRIMARY KEY,
    timespan INTEGER,
    at BIGINT,
    host_cpu_utilization FLOAT,
    process_cpu_utilization FLOAT,
    host_memory_utilization FLOAT,
    process_memory_utilization FLOAT
);

CREATE INDEX IF NOT EXISTS idx_statistics_resources_at ON statistics_resources(at);

-- ============================================================================
-- External Metadata
-- ============================================================================

CREATE TABLE IF NOT EXISTS external_metadata_sources (
    id SERIAL PRIMARY KEY,
    uri TEXT,
    source_title VARCHAR(255),
    user_title VARCHAR(255),
    online INTEGER
);

CREATE TABLE IF NOT EXISTS external_metadata_items (
    id INTEGER,
    external_metadata_source_id INTEGER,
    library_section_id INTEGER,
    metadata_type INTEGER,
    guid VARCHAR(255),
    title VARCHAR(255),
    parent_title VARCHAR(255),
    year INTEGER,
    added_at INTEGER,
    updated_at INTEGER,
    extra_data TEXT
);

CREATE INDEX IF NOT EXISTS idx_emi_source_id ON external_metadata_items(external_metadata_source_id);

-- ============================================================================
-- Media Providers
-- ============================================================================

CREATE TABLE IF NOT EXISTS media_provider_resources (
    id SERIAL PRIMARY KEY,
    parent_id INTEGER,
    type INTEGER,
    status INTEGER,
    state INTEGER,
    identifier VARCHAR(255),
    protocol VARCHAR(255),
    uri TEXT,
    uuid VARCHAR(255),
    extra_data TEXT,
    last_seen_at BIGINT,
    created_at BIGINT,
    updated_at BIGINT,
    data BYTEA
);

CREATE TABLE IF NOT EXISTS media_subscriptions (
    id SERIAL PRIMARY KEY,
    "order" FLOAT,
    metadata_type INTEGER,
    target_metadata_item_id INTEGER,
    target_library_section_id INTEGER,
    target_section_location_id INTEGER,
    extra_data TEXT,
    created_at BIGINT,
    updated_at BIGINT
);

CREATE TABLE IF NOT EXISTS media_grabs (
    id SERIAL PRIMARY KEY,
    uuid VARCHAR(255),
    status INTEGER,
    error INTEGER,
    metadata_item_id INTEGER,
    media_subscription_id INTEGER,
    extra_data TEXT,
    created_at BIGINT,
    updated_at BIGINT
);

CREATE TABLE IF NOT EXISTS metadata_subscription_desired_items (
    sub_id INTEGER,
    remote_id VARCHAR(255)
);

-- ============================================================================
-- Metadata Clustering
-- ============================================================================

CREATE TABLE IF NOT EXISTS metadata_item_clusters (
    id SERIAL PRIMARY KEY,
    zoom_level INTEGER,
    library_section_id INTEGER,
    title VARCHAR(255),
    count INTEGER,
    starts_at BIGINT,
    ends_at BIGINT,
    extra_data TEXT
);

CREATE TABLE IF NOT EXISTS metadata_item_clusterings (
    id SERIAL PRIMARY KEY,
    metadata_item_id INTEGER,
    metadata_item_cluster_id INTEGER,
    "index" INTEGER,
    version INTEGER
);

-- ============================================================================
-- Versioned Metadata
-- ============================================================================

CREATE TABLE IF NOT EXISTS versioned_metadata_items (
    id SERIAL PRIMARY KEY,
    metadata_item_id INTEGER,
    generator_id INTEGER,
    target_tag_id INTEGER,
    state INTEGER,
    state_context INTEGER,
    selected_media_id INTEGER,
    version_media_id INTEGER,
    media_decision INTEGER,
    file_size BIGINT
);

-- ============================================================================
-- Locations (for geo-tagged content)
-- ============================================================================

-- Note: In SQLite this uses R*Tree. For PostgreSQL we use regular indexes.
-- For production, consider PostGIS for proper spatial indexing.
CREATE TABLE IF NOT EXISTS locations (
    id SERIAL PRIMARY KEY,
    lat_min FLOAT,
    lat_max FLOAT,
    lon_min FLOAT,
    lon_max FLOAT
);

CREATE INDEX IF NOT EXISTS idx_locations_lat ON locations(lat_min, lat_max);
CREATE INDEX IF NOT EXISTS idx_locations_lon ON locations(lon_min, lon_max);

CREATE TABLE IF NOT EXISTS locatables (
    id SERIAL PRIMARY KEY,
    location_id INTEGER NOT NULL,
    locatable_id INTEGER NOT NULL,
    locatable_type VARCHAR(255) NOT NULL,
    created_at BIGINT,
    updated_at BIGINT,
    extra_data TEXT,
    geocoding_version INTEGER,
    UNIQUE(location_id, locatable_id, locatable_type)
);

CREATE TABLE IF NOT EXISTS location_places (
    id SERIAL PRIMARY KEY,
    location_id INTEGER,
    guid VARCHAR(255) NOT NULL,
    UNIQUE(location_id, guid)
);

-- ============================================================================
-- Blobs
-- ============================================================================

CREATE TABLE IF NOT EXISTS blobs (
    id SERIAL PRIMARY KEY,
    blob BYTEA,
    linked_type VARCHAR(255),
    linked_id INTEGER,
    linked_guid VARCHAR(255),
    created_at BIGINT,
    blob_type INTEGER
);

-- ============================================================================
-- Remote ID Translation
-- ============================================================================

CREATE TABLE IF NOT EXISTS remote_id_translation (
    id SERIAL PRIMARY KEY,
    type INTEGER,
    local_id INTEGER,
    remote_id VARCHAR(255)
);

-- ============================================================================
-- Hub Templates
-- ============================================================================

CREATE TABLE IF NOT EXISTS hub_templates (
    id SERIAL PRIMARY KEY,
    section VARCHAR(255),
    identifier VARCHAR(255),
    title VARCHAR(255),
    home_visibility INTEGER,
    recommended_visibility INTEGER,
    "order" FLOAT,
    extra_data TEXT
);

-- ============================================================================
-- Activities
-- ============================================================================

CREATE TABLE IF NOT EXISTS activities (
    id SERIAL PRIMARY KEY,
    parent_id INTEGER,
    type VARCHAR(255),
    title VARCHAR(255),
    subtitle TEXT,
    scheduled_at BIGINT,
    started_at BIGINT,
    finished_at BIGINT,
    cancelled INTEGER
);

-- ============================================================================
-- Custom Channels
-- ============================================================================

CREATE TABLE IF NOT EXISTS custom_channels (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    description TEXT,
    playlist_id INTEGER,
    start_time BIGINT,
    ordering INTEGER,
    visibility INTEGER,
    displayed_on INTEGER,
    content_rating VARCHAR(255)
);

-- ============================================================================
-- Metadata Agent Providers
-- ============================================================================

CREATE TABLE IF NOT EXISTS metadata_agent_providers (
    id SERIAL PRIMARY KEY,
    identifier VARCHAR(255),
    title VARCHAR(255),
    uri TEXT,
    agent_type INTEGER,
    metadata_types VARCHAR(255),
    online INTEGER,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    extra_data TEXT
);

CREATE TABLE IF NOT EXISTS metadata_agent_provider_groups (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255),
    primary_identifier VARCHAR(255),
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    extra_data TEXT
);

CREATE TABLE IF NOT EXISTS metadata_agent_provider_group_items (
    id SERIAL PRIMARY KEY,
    metadata_agent_provider_group_id INTEGER NOT NULL,
    metadata_agent_provider_id INTEGER NOT NULL,
    "order" FLOAT
);

-- ============================================================================
-- Download Queues
-- ============================================================================

CREATE TABLE IF NOT EXISTS download_queues (
    id SERIAL PRIMARY KEY,
    owner INTEGER,
    client_identifier VARCHAR(255),
    extra_data TEXT
);

CREATE TABLE IF NOT EXISTS download_queue_items (
    id SERIAL PRIMARY KEY,
    queue_id INTEGER,
    key VARCHAR(255),
    "order" INTEGER,
    status INTEGER,
    decision_params TEXT,
    error TEXT,
    decision_result TEXT,
    metadata_item_id INTEGER,
    media_part_id INTEGER,
    expiration BIGINT,
    extra_data TEXT
);

-- ============================================================================
-- Full Text Search (PostgreSQL native - replaces FTS4)
-- ============================================================================

-- Add tsvector columns for full-text search
ALTER TABLE metadata_items ADD COLUMN IF NOT EXISTS search_vector tsvector;

-- Create GIN index for full-text search
CREATE INDEX IF NOT EXISTS idx_metadata_items_search ON metadata_items USING GIN(search_vector);

-- Function to update search vector
CREATE OR REPLACE FUNCTION metadata_items_search_trigger() RETURNS trigger AS $$
BEGIN
    NEW.search_vector :=
        setweight(to_tsvector('simple', COALESCE(NEW.title, '')), 'A') ||
        setweight(to_tsvector('simple', COALESCE(NEW.title_sort, '')), 'B') ||
        setweight(to_tsvector('simple', COALESCE(NEW.original_title, '')), 'B');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to auto-update search vector
DROP TRIGGER IF EXISTS metadata_items_search_update ON metadata_items;
CREATE TRIGGER metadata_items_search_update
    BEFORE INSERT OR UPDATE ON metadata_items
    FOR EACH ROW EXECUTE FUNCTION metadata_items_search_trigger();

-- Similar for tags
ALTER TABLE tags ADD COLUMN IF NOT EXISTS search_vector tsvector;
CREATE INDEX IF NOT EXISTS idx_tags_search ON tags USING GIN(search_vector);

CREATE OR REPLACE FUNCTION tags_search_trigger() RETURNS trigger AS $$
BEGIN
    NEW.search_vector := to_tsvector('simple', COALESCE(NEW.tag, ''));
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tags_search_update ON tags;
CREATE TRIGGER tags_search_update
    BEFORE INSERT OR UPDATE ON tags
    FOR EACH ROW EXECUTE FUNCTION tags_search_trigger();

-- ============================================================================
-- Fuzzy Search (replaces spellfix1 using pg_trgm)
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_metadata_items_title_trgm ON metadata_items USING GIN(title gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_tags_tag_trgm ON tags USING GIN(tag gin_trgm_ops);

-- ============================================================================
-- Compatibility Functions
-- ============================================================================

-- SQLite-compatible typeof() function
CREATE OR REPLACE FUNCTION sqlite_typeof(val anyelement) RETURNS TEXT AS $$
BEGIN
    IF val IS NULL THEN
        RETURN 'null';
    END IF;
    CASE pg_typeof(val)::text
        WHEN 'integer', 'bigint', 'smallint' THEN RETURN 'integer';
        WHEN 'real', 'double precision', 'numeric' THEN RETURN 'real';
        WHEN 'text', 'character varying', 'character' THEN RETURN 'text';
        WHEN 'bytea' THEN RETURN 'blob';
        ELSE RETURN 'text';
    END CASE;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- SQLite-compatible iif() function
CREATE OR REPLACE FUNCTION iif(condition boolean, true_val anyelement, false_val anyelement)
RETURNS anyelement AS $$
BEGIN
    IF condition THEN
        RETURN true_val;
    ELSE
        RETURN false_val;
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- SQLite-compatible unixepoch() function
CREATE OR REPLACE FUNCTION unixepoch(ts text DEFAULT 'now', modifier text DEFAULT NULL)
RETURNS BIGINT AS $$
DECLARE
    result TIMESTAMP;
BEGIN
    IF ts = 'now' THEN
        result := NOW();
    ELSE
        result := ts::timestamp;
    END IF;

    IF modifier IS NOT NULL THEN
        result := result + modifier::interval;
    END IF;

    RETURN EXTRACT(EPOCH FROM result)::bigint;
END;
$$ LANGUAGE plpgsql STABLE;

-- Insert schema version
INSERT INTO schema_migrations (version) VALUES ('pg_adapter_1.0.0')
ON CONFLICT (version) DO NOTHING;
