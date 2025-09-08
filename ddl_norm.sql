create table if not exists matches (
  match_id text primary key,
  patch text not null,
  region text not null,
  queue smallint not null,
  game_duration_s int not null,
  game_start timestamptz not null
);

create table if not exists participants (
  match_id text references matches(match_id),
  participant_id smallint,
  puuid text,
  summoner_id text,
  team_id smallint,
  champ_id int,
  champ_name text,
  team_position text, -- TOP/JUNGLE/MIDDLE/BOTTOM/UTILITY or UNKNOWN
  lane text,          -- legacy lane if present
  role text,          -- legacy role if present
  win boolean,
  kills int, deaths int, assists int,
  gold_earned int,
  cs10 numeric, xpd10 numeric, gd10 numeric,
  primary key (match_id, participant_id)
);

create table if not exists opponents (
  match_id text references matches(match_id),
  your_participant_id smallint,
  opp_participant_id smallint,
  team_position text,
  primary key (match_id, your_participant_id)
);
