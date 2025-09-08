-- Runes (flattened)
create table if not exists participant_runes (
  match_id text not null,
  participant_id smallint not null,
  primary_style int,
  primary_keystone int,
  primary_minors int[],        -- 3 ids
  secondary_style int,
  secondary_minors int[],      -- 2 ids
  shards int[],                -- 3 ids
  primary key (match_id, participant_id)
);

-- Final items snapshot (end of game)
create table if not exists participant_items_final (
  match_id text not null,
  participant_id smallint not null,
  slot0 int, slot1 int, slot2 int, slot3 int, slot4 int, slot5 int, slot6 int,
  primary key (match_id, participant_id)
);

-- Timeline item stream (purchase/sell/undo/destroy)
create table if not exists participant_item_events (
  match_id text not null,
  participant_id smallint not null,
  ts_ms bigint not null,
  item_id int not null,
  event text not null,         -- 'PURCHASED'|'SOLD'|'UNDO'|'DESTROYED'
  gold_at_ts int,
  primary key (match_id, participant_id, ts_ms, item_id, event)
);

-- Core build summary (derived): mythic, boots, first completed legendary, first-back gold
create table if not exists participant_core_build (
  match_id text not null,
  participant_id smallint not null,
  mythic_id int,
  boots_id int,
  first_item_id int,
  first_back_gold int,
  primary key (match_id, participant_id)
);
