create table if not exists champ_tags (
  champ_name text primary key,
  dmg text not null,           -- 'AD' | 'AP' | 'MIXED'
  has_engage boolean not null default false,
  is_tank boolean not null default false
);
