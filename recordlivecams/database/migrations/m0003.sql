PRAGMA foreign_keys = 0;

CREATE TABLE sqlitestudio_temp_table AS SELECT *
                                          FROM streamer;

DROP TABLE streamer;

CREATE TABLE streamer (
    id           INTEGER,
    first_online TIMESTAMP DEFAULT (datetime('now') ),
    last_online  TIMESTAMP DEFAULT (datetime('now') ),
    days_online  INTEGER   DEFAULT (0),
    PRIMARY KEY (
        id
    )
);

INSERT INTO streamer (
                         id,
                         first_online,
                         last_online
                     )
                     SELECT id,
                            first_online,
                            last_online
                       FROM sqlitestudio_temp_table;

DROP TABLE sqlitestudio_temp_table;

PRAGMA foreign_keys = 1;
