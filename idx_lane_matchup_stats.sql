-- ✅ Primary: unique covering index on the natural key
-- (future-proofs you for REFRESH CONCURRENTLY if you ever want it)
create unique index if not exists ux_lane_matchup_stats_key
on lane_matchup_stats
  (patch, team_position, champ, opponent)
include (n, winrate, avg_gd10, avg_xpd10, avg_cs10);

-- 🔎 Variant: when you filter without opponent (e.g., show all opponents for a champ)
create index if not exists idx_lane_matchup_stats_no_opp
on lane_matchup_stats
  (patch, team_position, champ)
include (opponent, n, winrate, avg_gd10, avg_xpd10, avg_cs10);

-- 📈 Optional helper: fast "top populated" within a patch (ORDER BY n DESC)
-- Useful for your “top populated matchups” debug query.
create index if not exists idx_lane_matchup_stats_pop
on lane_matchup_stats
  (patch, n desc);
