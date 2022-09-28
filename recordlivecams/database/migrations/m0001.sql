BEGIN TRANSACTION;

CREATE TABLE site (
    name TEXT PRIMARY KEY,
    display_name TEXT
);
INSERT INTO site (name, display_name) VALUES ('chaturbate', 'Chaturbate');
INSERT INTO site (name, display_name) VALUES ('stripchat', 'Stripchat');
INSERT INTO site (name, display_name) VALUES ('bongacams', 'BongaCams');
INSERT INTO site (name, display_name) VALUES ('mfc', 'MyFreeCams');
INSERT INTO site (name, display_name) VALUES ('cam4', 'Cam4');

CREATE TABLE streamer (
    id INTEGER PRIMARY KEY,
    first_online TEXT DEFAULT (datetime('now')),
    last_online TEXT DEFAULT (datetime('now'))
);

CREATE TABLE streamer_sites (
    streamer_id INTEGER,
    name TEXT,
    site_id TEXT,
    is_primary INTEGER DEFAULT 1,
    site_name TEXT,

    FOREIGN KEY (streamer_id) REFERENCES streamer (id),
    FOREIGN KEY (site_name) REFERENCES site (name),
    UNIQUE(streamer_id, site_name)
);

COMMIT;