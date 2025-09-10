BEGIN;

DROP SCHEMA IF EXISTS lol CASCADE;
CREATE SCHEMA lol;

CREATE TABLE lol.champions (
  champ_id   INT PRIMARY KEY,
  champ_name TEXT NOT NULL
);

CREATE TABLE lol.items (
  item_id    INT PRIMARY KEY,
  item_name  TEXT NOT NULL
);

CREATE TABLE lol.runes (
  rune_id    INT PRIMARY KEY,
  rune_name  TEXT NOT NULL
);

CREATE TABLE lol.matches (
  match_id       TEXT PRIMARY KEY,
  region         TEXT NOT NULL,
  queue_id       INT  NOT NULL,
  patch          TEXT NOT NULL,
  game_version   TEXT NOT NULL,
  game_start_ts  TIMESTAMPTZ NOT NULL,
  duration_s     INT  NOT NULL,
  skill_tier     TEXT,
  blue_win       BOOLEAN NOT NULL
);

CREATE TABLE lol.participants (
  match_id       TEXT REFERENCES lol.matches(match_id) ON DELETE CASCADE,
  puuid          TEXT NOT NULL,
  team_id        INT  NOT NULL CHECK (team_id IN (100,200)),
  side           TEXT GENERATED ALWAYS AS (CASE WHEN team_id=100 THEN 'BLUE' ELSE 'RED' END) STORED,
  champ_id       INT  NOT NULL REFERENCES lol.champions(champ_id),
  lane_raw       TEXT,
  role_raw       TEXT,
  lane_derived   TEXT NOT NULL,
  role_derived   TEXT NOT NULL,
  win            BOOLEAN NOT NULL,
  kills          INT NOT NULL,
  deaths         INT NOT NULL,
  assists        INT NOT NULL,
  cs             INT NOT NULL,
  gold_earned    INT NOT NULL,
  damage_dealt   INT,
  item0 INT, item1 INT, item2 INT, item3 INT, item4 INT, item5 INT, item6 INT,
  PRIMARY KEY (match_id, puuid)
);

CREATE TABLE lol.participant_frames (
  match_id   TEXT NOT NULL,
  puuid      TEXT NOT NULL,
  minute     INT  NOT NULL,
  gold       INT  NOT NULL,
  xp         INT  NOT NULL,
  cs         INT  NOT NULL,
  PRIMARY KEY (match_id, puuid, minute),
  FOREIGN KEY (match_id, puuid) REFERENCES lol.participants(match_id, puuid) ON DELETE CASCADE
);

CREATE TABLE lol.item_events (
  match_id     TEXT NOT NULL,
  puuid        TEXT NOT NULL,
  ts_ms        BIGINT NOT NULL,
  event_type   TEXT  NOT NULL,   -- PURCHASE/SELL/UNDO
  item_id      INT   NOT NULL REFERENCES lol.items(item_id),
  PRIMARY KEY (match_id, puuid, ts_ms, event_type, item_id),
  FOREIGN KEY (match_id, puuid) REFERENCES lol.participants(match_id, puuid) ON DELETE CASCADE
);

-- Indexes
CREATE INDEX ON lol.matches (patch, skill_tier, queue_id, region);
CREATE INDEX ON lol.participants (lane_derived, champ_id, match_id);
CREATE INDEX ON lol.participants (role_derived, champ_id, match_id);
CREATE INDEX ON lol.participants (team_id, match_id);
CREATE INDEX ON lol.participants (win);
CREATE INDEX ON lol.participant_frames (minute);
CREATE INDEX ON lol.participant_frames (match_id, puuid);
CREATE INDEX ON lol.item_events (puuid, match_id);
CREATE INDEX ON lol.item_events (item_id);

COMMIT;
