-- =========================
-- = FLEXIBLE AGG SUMMARY =
-- =========================
-- name: agg_summary
WITH
params AS (
  SELECT
    %(patch)s::TEXT               AS patch,
    %(skill_tier)s::TEXT          AS skill_tier,
    %(minute)s::INT               AS minute,
    %(min_n)s::INT                AS min_n,
    %(subject)s::JSONB            AS subject,
    %(ally_filters)s::JSONB       AS ally_filters,
    %(enemy_filters)s::JSONB      AS enemy_filters
),
subject_req AS (
  -- exactly one row describing "my pick"
  SELECT
    NULLIF(UPPER(p.subject->>'role'), '')  AS role,
    NULLIF(p.subject->>'champ_id','')::INT AS champ_id
  FROM params p
),
ally_req AS (
  -- zero or more extra ally constraints
  SELECT NULLIF(UPPER(f->>'role'), '')  AS role,
         NULLIF(f->>'champ_id','')::INT AS champ_id
  FROM params, LATERAL jsonb_array_elements(params.ally_filters) AS f
),
enemy_req AS (
  -- zero or more enemy constraints
  SELECT NULLIF(UPPER(f->>'role'), '')  AS role,
         NULLIF(f->>'champ_id','')::INT AS champ_id
  FROM params, LATERAL jsonb_array_elements(params.enemy_filters) AS f
),
ally_req_all AS (
  -- subject + any ally filters; all must be on the same team
  SELECT * FROM subject_req
  UNION ALL
  SELECT * FROM ally_req
),
candidate_matches AS (
  SELECT m.match_id
  FROM lol.matches m, params p
  WHERE (p.patch IS NULL OR m.patch = p.patch)
    AND (p.skill_tier IS NULL OR m.skill_tier = p.skill_tier)
),
ally_side_matches AS (
  -- A match qualifies for the ally side if ALL requested rows
  -- (subject + ally filters) are satisfied ON THE SAME TEAM.
  SELECT cm.match_id, p.team_id
  FROM candidate_matches cm
  JOIN lol.participants p ON p.match_id = cm.match_id
  JOIN ally_req_all ar
    ON (ar.role     IS NULL OR p.role_derived = ar.role)
   AND (ar.champ_id IS NULL OR p.champ_id     = ar.champ_id)
  GROUP BY cm.match_id, p.team_id
  HAVING COUNT(*) = (SELECT COUNT(*) FROM ally_req_all)
),
enemy_side_matches AS (
  -- Opposing team must satisfy ALL enemy filters (if any)
  SELECT cm.match_id, p.team_id
  FROM candidate_matches cm
  JOIN lol.participants p ON p.match_id = cm.match_id
  JOIN enemy_req er
    ON (er.role     IS NULL OR p.role_derived = er.role)
   AND (er.champ_id IS NULL OR p.champ_id     = er.champ_id)
  GROUP BY cm.match_id, p.team_id
  HAVING COUNT(*) = (SELECT COUNT(*) FROM enemy_req)
),
eligible AS (
  -- Keep matches where ally side is satisfied, and enemy side (if provided)
  -- is satisfied on the OPPOSITE team.
  SELECT a.match_id, a.team_id AS ally_team
  FROM ally_side_matches a
  LEFT JOIN enemy_side_matches e
    ON e.match_id = a.match_id AND e.team_id <> a.team_id
  WHERE (SELECT COUNT(*) FROM enemy_req) = 0
     OR e.team_id IS NOT NULL
),

-- Only the subject participant(s) on the ally team
subject_rows AS (
  SELECT DISTINCT p.match_id, p.puuid, p.team_id, p.win
  FROM eligible el
  JOIN lol.participants p
    ON p.match_id = el.match_id AND p.team_id = el.ally_team
  JOIN subject_req sr
    ON (sr.role     IS NULL OR p.role_derived = sr.role)
   AND (sr.champ_id IS NULL OR p.champ_id     = sr.champ_id)
),

subject_stats AS (
  SELECT s.match_id,
         AVG(fr.gold)::NUMERIC(10,2) AS gold_at_min,
         AVG(fr.xp)::NUMERIC(10,2)   AS xp_at_min
  FROM params prm
  JOIN subject_rows s ON TRUE
  JOIN lol.participant_frames fr
    ON fr.match_id = s.match_id
   AND fr.puuid    = s.puuid
   AND fr.minute   = prm.minute
  GROUP BY s.match_id
),
rolled AS (
  SELECT
    COUNT(DISTINCT s.match_id)                                AS n_games,
    AVG(CASE WHEN s.win THEN 1.0 ELSE 0.0 END)::NUMERIC(5,3)  AS winrate,
    AVG(st.gold_at_min)::NUMERIC(10,2)                        AS gold_at_min,
    AVG(st.xp_at_min)::NUMERIC(10,2)                          AS xp_at_min
  FROM subject_rows s
  JOIN subject_stats st ON st.match_id = s.match_id
)
SELECT n_games, winrate, gold_at_min, xp_at_min
FROM rolled
WHERE n_games >= (SELECT min_n FROM params);



-- ======================
-- = TOP ITEM POPULARS =
-- ======================
-- name: top_items
WITH
params AS (
  SELECT
    %(patch)s::TEXT               AS patch,
    %(skill_tier)s::TEXT          AS skill_tier,
    %(minute)s::INT               AS minute,
    %(min_n)s::INT                AS min_n,
    %(subject)s::JSONB            AS subject,
    %(ally_filters)s::JSONB       AS ally_filters,
    %(enemy_filters)s::JSONB      AS enemy_filters
),
subject_req AS (
  SELECT
    NULLIF(UPPER(p.subject->>'role'), '')  AS role,
    NULLIF(p.subject->>'champ_id','')::INT AS champ_id
  FROM params p
),
ally_req AS (
  SELECT NULLIF(UPPER(f->>'role'), '')  AS role,
         NULLIF(f->>'champ_id','')::INT AS champ_id
  FROM params, LATERAL jsonb_array_elements(params.ally_filters) AS f
),
enemy_req AS (
  SELECT NULLIF(UPPER(f->>'role'), '')  AS role,
         NULLIF(f->>'champ_id','')::INT AS champ_id
  FROM params, LATERAL jsonb_array_elements(params.enemy_filters) AS f
),
ally_req_all AS (
  SELECT * FROM subject_req
  UNION ALL
  SELECT * FROM ally_req
),
candidate_matches AS (
  SELECT m.match_id
  FROM lol.matches m, params p
  WHERE (p.patch IS NULL OR m.patch = p.patch)
    AND (p.skill_tier IS NULL OR m.skill_tier = p.skill_tier)
),
ally_side_matches AS (
  SELECT cm.match_id, p.team_id
  FROM candidate_matches cm
  JOIN lol.participants p ON p.match_id = cm.match_id
  JOIN ally_req_all ar
    ON (ar.role     IS NULL OR p.role_derived = ar.role)
   AND (ar.champ_id IS NULL OR p.champ_id     = ar.champ_id)
  GROUP BY cm.match_id, p.team_id
  HAVING COUNT(*) = (SELECT COUNT(*) FROM ally_req_all)
),
enemy_side_matches AS (
  SELECT cm.match_id, p.team_id
  FROM candidate_matches cm
  JOIN lol.participants p ON p.match_id = cm.match_id
  JOIN enemy_req er
    ON (er.role     IS NULL OR p.role_derived = er.role)
   AND (er.champ_id IS NULL OR p.champ_id     = er.champ_id)
  GROUP BY cm.match_id, p.team_id
  HAVING COUNT(*) = (SELECT COUNT(*) FROM enemy_req)
),
eligible AS (
  SELECT a.match_id, a.team_id AS ally_team
  FROM ally_side_matches a
  LEFT JOIN enemy_side_matches e
    ON e.match_id = a.match_id AND e.team_id <> a.team_id
  WHERE (SELECT COUNT(*) FROM enemy_req) = 0
     OR e.team_id IS NOT NULL
),

-- Subject-only purchases
subject_rows AS (
  SELECT DISTINCT p.match_id, p.puuid
  FROM eligible el
  JOIN lol.participants p
    ON p.match_id = el.match_id AND p.team_id = el.ally_team
  JOIN subject_req sr
    ON (sr.role     IS NULL OR p.role_derived = sr.role)
   AND (sr.champ_id IS NULL OR p.champ_id     = sr.champ_id)
),
subject_item_events AS (
  SELECT ie.item_id, COUNT(*) AS picks
  FROM subject_rows s
  JOIN lol.item_events ie
    ON ie.match_id = s.match_id
   AND ie.puuid    = s.puuid
  WHERE ie.event_type = 'PURCHASE'  -- only subject's purchases
  GROUP BY ie.item_id
),
n_base AS (
  SELECT COUNT(DISTINCT match_id) AS n_games FROM eligible
)
SELECT sie.item_id, it.item_name, sie.picks
FROM subject_item_events sie
JOIN lol.items it ON it.item_id = sie.item_id
CROSS JOIN n_base nb
WHERE nb.n_games >= (SELECT min_n FROM params)
ORDER BY sie.picks DESC
LIMIT 25;
