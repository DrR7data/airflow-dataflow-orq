
DROP SCHEMA IF EXISTS track CASCADE;
CREATE SCHEMA track;
DROP TABLE IF EXISTS track.album CASCADE;
CREATE TABLE track.album (
    id SERIAL,
    title VARCHAR(128) UNIQUE,
    PRIMARY KEY(id)
);

DROP TABLE IF EXISTS track.artist CASCADE;
CREATE TABLE track.artist (
    id SERIAL,
    name VARCHAR(128) UNIQUE,
    PRIMARY KEY(id)
);

DROP TABLE IF EXISTS track.track CASCADE;
CREATE TABLE track.track (
    id SERIAL,
    title TEXT, 
    artist TEXT, 
    album TEXT, 
    album_id INTEGER REFERENCES track.album(id) ON DELETE CASCADE,
    count INTEGER, 
    rating INTEGER, 
    len INTEGER,
    PRIMARY KEY(id)
);

DROP TABLE IF EXISTS track.tracktoartist CASCADE;
CREATE TABLE track.tracktoartist (
    id SERIAL,
    name VARCHAR(128) UNIQUE,
    PRIMARY KEY(id)
);

DROP TABLE IF EXISTS track.tracktoartist CASCADE;
CREATE TABLE track.tracktoartist (
    id SERIAL,
    track VARCHAR(128),
    track_id INTEGER REFERENCES track.track(id) ON DELETE CASCADE,
    artist VARCHAR(128),
    artist_id INTEGER REFERENCES track.artist(id) ON DELETE CASCADE,
    PRIMARY KEY(id)
);