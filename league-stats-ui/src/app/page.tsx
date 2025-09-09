"use client";

import React, { useEffect, useMemo, useRef, useState } from "react";

// ---- Config --------------------------------------------------------------
const API_BASE =
  (typeof window !== "undefined" && (process.env.NEXT_PUBLIC_API_BASE || "")) ||
  "";

// ---- Tiny utilities ------------------------------------------------------
function qs(obj: Record<string, any>) {
  const entries = Object.entries(obj)
    .filter(([, v]) => v !== undefined && v !== null && v !== "")
    .map(([k, v]) => [k, String(v)] as [string, string]);
  return new URLSearchParams(entries).toString();
}

function fmtPct(x: number | null | undefined, digits = 1) {
  if (x === null || x === undefined || Number.isNaN(x)) return "—";
  return (x * 100).toFixed(digits) + "%";
}
function fmtInt(x: number | null | undefined) {
  if (x === null || x === undefined || Number.isNaN(x)) return "—";
  return new Intl.NumberFormat().format(x);
}

// ---- Types ---------------------------------------------------------------
export type Row = {
  patch: string;
  team_position?: string; // lane for some endpoints
  lane?: string; // normalized key for lane_jg
  champ: string;
  opponent?: string;
  enemy_jungler?: string | null;
  ally_jungler?: string | null;
  my_duo?: string | null;
  opp_duo?: string | null;
  n: number;
  winrate: number;
  smoothed_wr?: number | null;
  wr_ci_low?: number | null;
  wr_ci_high?: number | null;
  low_sample?: boolean;
  avg_gd10?: number | null;
  avg_xpd10?: number | null;
  avg_cs10?: number | null;
  // builds
  mythic_id?: number | null;
  boots_id?: number | null;
  primary_keystone?: number | null;
  secondary_style?: number | null;
};

export type Envelope = {
  params: Record<string, any>;
  rows: Row[];
  page: {
    limit: number;
    offset: number;
    total?: number | null;
    next_offset?: number | null;
    prev_offset?: number | null;
  };
};

// ---- Meta types for Data Dragon -----------------------------------------
export type MetaItems = Record<number, { name: string; icon: string }>;
export type MetaKeystones = Record<number, { name: string; icon: string; style_id: number }>;
export type MetaStyles = Record<number, { name: string; icon: string }>;

// ---- Tab mapping ---------------------------------------------------------
const TABS = [
  { key: "lane", label: "Lane Only", endpoint: "/matchup" },
  { key: "lanejg", label: "Lane + Enemy JG", endpoint: "/ctx/lane_jg" },
  { key: "bot2v2", label: "Bot 2v2", endpoint: "/ctx/bot2v2" },
] as const;

// ---- Fetch hook ----------------------------------------------------------
function useEnvelope(url: string | null) {
  const [data, setData] = useState<Envelope | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  useEffect(() => {
    if (!url) return;
    setLoading(true);
    setError(null);
    abortRef.current?.abort();
    const ac = new AbortController();
    abortRef.current = ac;
    fetch(url, { signal: ac.signal })
      .then(async (r) => {
        if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
        return r.json();
      })
      .then((json) => setData(json))
      .catch((e) => {
        if (e.name === "AbortError") return; // ignore
        setError(e.message || String(e));
      })
      .finally(() => setLoading(false));
    return () => ac.abort();
  }, [url]);

  return { data, loading, error };
}

// ---- Typeahead (champions) ----------------------------------------------
function useTypeahead(term: string) {
  const [items, setItems] = useState<string[]>([]);
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const tRef = useRef<number | null>(null);

  useEffect(() => {
    if (!term) {
      setItems([]);
      setOpen(false);
      return;
    }
    setLoading(true);
    if (tRef.current) window.clearTimeout(tRef.current);
    tRef.current = window.setTimeout(() => {
      const url = `${API_BASE}/champions?${qs({ search: term, limit: 20 })}`;
      fetch(url)
        .then((r) => r.json())
        .then((j) => {
          setItems(j.items || []);
          setOpen(true);
        })
        .finally(() => setLoading(false));
    }, 200);
    return () => {
      if (tRef.current) window.clearTimeout(tRef.current);
    };
  }, [term]);

  return { items, open, setOpen, loading };
}

// ---- Query Bar -----------------------------------------------------------
function QueryBar({
  tab,
  filters,
  setFilters,
}: {
  tab: typeof TABS[number]["key"];
  filters: Record<string, any>;
  setFilters: (patch: (f: Record<string, any>) => Record<string, any>) => void;
}) {
  const [champTerm, setChampTerm] = useState("");
  const [oppTerm, setOppTerm] = useState("");
  const [jgTerm, setJgTerm] = useState("");
  const [myDuoTerm, setMyDuoTerm] = useState("");
  const [oppDuoTerm, setOppDuoTerm] = useState("");

  const champTA = useTypeahead(champTerm);
  const oppTA = useTypeahead(oppTerm);
  const jgTA = useTypeahead(jgTerm);
  const myDuoTA = useTypeahead(myDuoTerm);
  const oppDuoTA = useTypeahead(oppDuoTerm);

  const applicable = {
    lane: true,
    champ: true,
    opponent: true,
    enemy_jungler: tab === "lanejg",
    ally_jungler: false, // could be enabled later
    my_duo: tab === "bot2v2",
    opp_duo: tab === "bot2v2",
  };

  return (
    <div className="grid gap-3 rounded-2xl border border-zinc-800 p-4">
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        {/* Lane */}
        <div>
          <label className="block text-xs text-zinc-400 mb-1">Lane</label>
          <select
            className="w-full rounded-xl bg-zinc-900 border border-zinc-700 p-2"
            value={filters.lane || ""}
            onChange={(e) => setFilters((f) => ({ ...f, lane: e.target.value || undefined }))}
          >
            <option value="">Any</option>
            <option value="TOP">TOP</option>
            <option value="JUNGLE">JUNGLE</option>
            <option value="MIDDLE">MIDDLE</option>
            <option value="BOTTOM">BOTTOM</option>
            <option value="UTILITY">UTILITY</option>
          </select>
        </div>

        {/* Patch */}
        <div>
          <label className="block text-xs text-zinc-400 mb-1">Patch</label>
          <input
            placeholder="e.g. 14.15"
            className="w-full rounded-xl bg-zinc-900 border border-zinc-700 p-2"
            value={filters.patch || ""}
            onChange={(e) => setFilters((f) => ({ ...f, patch: e.target.value || undefined }))}
          />
        </div>

        {/* Sort */}
        <div>
          <label className="block text-xs text-zinc-400 mb-1">Sort</label>
          <select
            className="w-full rounded-xl bg-zinc-900 border border-zinc-700 p-2"
            value={filters.sort || "-n"}
            onChange={(e) => setFilters((f) => ({ ...f, sort: e.target.value }))}
          >
            <option value="-n">Most games</option>
            <option value="-winrate">Highest WR</option>
            <option value="-smoothed_wr">Smoothed WR</option>
            <option value="-gd10">Best GD@10</option>
            <option value="n">Least games</option>
            <option value="winrate">Lowest WR</option>
            <option value="gd10">Worst GD@10</option>
            <option value="smoothed_wr">Smoothed (asc)</option>
          </select>
        </div>

        {/* Page Size */}
        <div>
          <label className="block text-xs text-zinc-400 mb-1">Page Size</label>
          <select
            className="w-full rounded-xl bg-zinc-900 border border-zinc-700 p-2"
            value={filters.limit || 50}
            onChange={(e) => setFilters((f) => ({ ...f, limit: Number(e.target.value) }))}
          >
            <option value={25}>25</option>
            <option value={50}>50</option>
            <option value={100}>100</option>
          </select>
        </div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
        {/* Champ */}
        <div className="relative">
          <label className="block text-xs text-zinc-400 mb-1">Your Champ</label>
          <input
            className="w-full rounded-xl bg-zinc-900 border border-zinc-700 p-2"
            placeholder="e.g. Aatrox"
            value={filters.champ || ""}
            onChange={(e) => {
              setFilters((f) => ({ ...f, champ: e.target.value || undefined }));
              setChampTerm(e.target.value);
            }}
            autoComplete="off"
          />
          {champTA.open && champTA.items.length > 0 && (
            <ul className="absolute z-10 mt-1 max-h-56 w-full overflow-auto rounded-xl border border-zinc-700 bg-zinc-900 shadow">
              {champTA.items.map((n) => (
                <li
                  key={n}
                  className="cursor-pointer px-3 py-2 hover:bg-zinc-800"
                  onClick={() => {
                    setFilters((f) => ({ ...f, champ: n }));
                    setChampTerm("");
                    champTA.setOpen(false);
                  }}
                >
                  {n}
                </li>
              ))}
            </ul>
          )}
        </div>

        {/* Opponent */}
        <div className="relative">
          <label className="block text-xs text-zinc-400 mb-1">Opponent</label>
          <input
            className="w-full rounded-xl bg-zinc-900 border border-zinc-700 p-2"
            placeholder="e.g. Darius"
            value={filters.opponent || ""}
            onChange={(e) => {
              setFilters((f) => ({ ...f, opponent: e.target.value || undefined }));
              setOppTerm(e.target.value);
            }}
            autoComplete="off"
          />
          {oppTA.open && oppTA.items.length > 0 && (
            <ul className="absolute z-10 mt-1 max-h-56 w-full overflow-auto rounded-xl border border-zinc-700 bg-zinc-900 shadow">
              {oppTA.items.map((n) => (
                <li
                  key={n}
                  className="cursor-pointer px-3 py-2 hover:bg-zinc-800"
                  onClick={() => {
                    setFilters((f) => ({ ...f, opponent: n }));
                    setOppTerm("");
                    oppTA.setOpen(false);
                  }}
                >
                  {n}
                </li>
              ))}
            </ul>
          )}
        </div>

        {/* Enemy JG or Duos depending on tab */}
        {applicable.enemy_jungler ? (
          <div className="relative">
            <label className="block text-xs text-zinc-400 mb-1">Enemy Jungler</label>
            <input
              className="w-full rounded-xl bg-zinc-900 border border-zinc-700 p-2"
              placeholder="e.g. Elise"
              value={filters.enemy_jungler || ""}
              onChange={(e) => {
                setFilters((f) => ({ ...f, enemy_jungler: e.target.value || undefined }));
                setJgTerm(e.target.value);
              }}
              autoComplete="off"
            />
            {jgTA.open && jgTA.items.length > 0 && (
              <ul className="absolute z-10 mt-1 max-h-56 w-full overflow-auto rounded-xl border border-zinc-700 bg-zinc-900 shadow">
                {jgTA.items.map((n) => (
                  <li
                    key={n}
                    className="cursor-pointer px-3 py-2 hover:bg-zinc-800"
                    onClick={() => {
                      setFilters((f) => ({ ...f, enemy_jungler: n }));
                      setJgTerm("");
                      jgTA.setOpen(false);
                    }}
                  >
                    {n}
                  </li>
                ))}
              </ul>
            )}
          </div>
        ) : applicable.my_duo ? (
          <div className="grid grid-cols-2 gap-3">
            <div className="relative">
              <label className="block text-xs text-zinc-400 mb-1">Your Duo</label>
              <input
                className="w-full rounded-xl bg-zinc-900 border border-zinc-700 p-2"
                placeholder="e.g. Brand"
                value={filters.my_duo || ""}
                onChange={(e) => {
                  setFilters((f) => ({ ...f, my_duo: e.target.value || undefined }));
                  setMyDuoTerm(e.target.value);
                }}
                autoComplete="off"
              />
              {myDuoTA.open && myDuoTA.items.length > 0 && (
                <ul className="absolute z-10 mt-1 max-h-56 w-full overflow-auto rounded-xl border border-zinc-700 bg-zinc-900 shadow">
                  {myDuoTA.items.map((n) => (
                    <li
                      key={n}
                      className="cursor-pointer px-3 py-2 hover:bg-zinc-800"
                      onClick={() => {
                        setFilters((f) => ({ ...f, my_duo: n }));
                        setMyDuoTerm("");
                        myDuoTA.setOpen(false);
                      }}
                    >
                      {n}
                    </li>
                  ))}
                </ul>
              )}
            </div>
            <div className="relative">
              <label className="block text-xs text-zinc-400 mb-1">Enemy Duo</label>
              <input
                className="w-full rounded-xl bg-zinc-900 border border-zinc-700 p-2"
                placeholder="e.g. Thresh"
                value={filters.opp_duo || ""}
                onChange={(e) => {
                  setFilters((f) => ({ ...f, opp_duo: e.target.value || undefined }));
                  setOppDuoTerm(e.target.value);
                }}
                autoComplete="off"
              />
              {oppDuoTA.open && oppDuoTA.items.length > 0 && (
                <ul className="absolute z-10 mt-1 max-h-56 w-full overflow-auto rounded-xl border border-zinc-700 bg-zinc-900 shadow">
                  {oppDuoTA.items.map((n) => (
                    <li
                      key={n}
                      className="cursor-pointer px-3 py-2 hover:bg-zinc-800"
                      onClick={() => {
                        setFilters((f) => ({ ...f, opp_duo: n }));
                        setOppDuoTerm("");
                        oppDuoTA.setOpen(false);
                      }}
                    >
                      {n}
                    </li>
                  ))}
                </ul>
              )}
            </div>
          </div>
        ) : (
          <div />
        )}
      </div>

      {/* Advanced toggles */}
      <details className="rounded-xl border border-zinc-800 p-3">
        <summary className="cursor-pointer text-sm text-zinc-300">Advanced</summary>
        <div className="mt-3 grid grid-cols-2 md:grid-cols-4 gap-3">
          <div>
            <label className="block text-xs text-zinc-400 mb-1">min_n</label>
            <input
              type="number"
              className="w-full rounded-xl bg-zinc-900 border border-zinc-700 p-2"
              value={filters.min_n ?? 10}
              onChange={(e) => setFilters((f) => ({ ...f, min_n: Number(e.target.value) }))}
            />
          </div>
          <div>
            <label className="block text-xs text-zinc-400 mb-1">alpha</label>
            <input
              type="number"
              className="w-full rounded-xl bg-zinc-900 border border-zinc-700 p-2"
              value={filters.alpha ?? 20}
              onChange={(e) => setFilters((f) => ({ ...f, alpha: Number(e.target.value) }))}
            />
          </div>
          <div>
            <label className="block text-xs text-zinc-400 mb-1">prior_wr</label>
            <input
              type="number" step="0.01" min={0} max={1}
              className="w-full rounded-xl bg-zinc-900 border border-zinc-700 p-2"
              value={filters.prior_wr ?? 0.5}
              onChange={(e) => setFilters((f) => ({ ...f, prior_wr: Number(e.target.value) }))}
            />
          </div>
          <div>
            <label className="block text-xs text-zinc-400 mb-1">warn_n</label>
            <input
              type="number"
              className="w-full rounded-xl bg-zinc-900 border border-zinc-700 p-2"
              value={filters.warn_n ?? 25}
              onChange={(e) => setFilters((f) => ({ ...f, warn_n: Number(e.target.value) }))}
            />
          </div>
        </div>
      </details>
    </div>
  );
}

// ---- Table ---------------------------------------------------------------
function MatchupTable({ url, tab, onSelectRow }: { url: string | null; tab: string; onSelectRow: (row: Row) => void }) {
  const { data, loading, error } = useEnvelope(url);
  const rows = data?.rows || [];
  const page = data?.page;

  const columns = useMemo(() => {
    const base = [
      { key: "champ", label: "Champ" },
      { key: "opponent", label: "Opponent" },
    ];
    if (tab === "lanejg") base.splice(2, 0, { key: "enemy_jungler", label: "Enemy JG" });
    if (tab === "bot2v2") {
      return [
        { key: "champ", label: "ADC" },
        { key: "my_duo", label: "SUP" },
        { key: "opponent", label: "Enemy ADC" },
        { key: "opp_duo", label: "Enemy SUP" },
        { key: "n", label: "N", tip: "Number of games in this sample." },
        { key: "winrate", label: "WR", tip: "Observed win rate: wins/games for this row (no smoothing)." },
        { key: "smoothed_wr", label: "WR (adj)", tip: "Bayesian-smoothed win rate = (winrate·n + prior_wr·alpha) / (n + alpha). Defaults: alpha=20, prior=50%." },
        { key: "wr_ci", label: "CI 95%", tip: "Wilson 95% confidence interval for the true win rate. Narrows with larger N." },
        { key: "avg_gd10", label: "GD@10", tip: "Average gold difference at 10 minutes (your side)." },
      ];
    }
    return [
      ...base,
      { key: "n", label: "N", tip: "Number of games in this sample." },
      { key: "winrate", label: "WR", tip: "Observed win rate: wins/games for this row (no smoothing)." },
      { key: "smoothed_wr", label: "WR (adj)", tip: "Bayesian-smoothed win rate = (winrate·n + prior_wr·alpha) / (n + alpha). Defaults: alpha=20, prior=50%." },
      { key: "wr_ci", label: "CI 95%", tip: "Wilson 95% confidence interval for the true win rate. Narrows with larger N." },
      { key: "avg_gd10", label: "GD@10", tip: "Average gold difference at 10 minutes (your side)." },
    ];
  }, [tab]);

  return (
    <>
      <div className="rounded-2xl border border-zinc-800 overflow-hidden">
        <div className="flex items-center justify-between px-3 py-2 text-xs text-zinc-400 border-b border-zinc-800">
          <div>{loading ? "Loading…" : error ? `Error: ${error}` : `${fmtInt(page?.total || 0)} results`}</div>
          {page && (
            <div className="flex items-center gap-2">
              <button
                className="rounded-lg border border-zinc-700 px-2 py-1 disabled:opacity-50"
                disabled={!page.prev_offset && page.prev_offset !== 0}
                onClick={() => {
                  const u = new URL(window.location.href);
                  const p = new URLSearchParams(u.search);
                  const limit = Number(p.get("limit") || 50);
                  const currentOffset = Number(p.get("offset") || 0);
                  const next = Math.max(currentOffset - limit, 0);
                  p.set("offset", String(next));
                  window.history.replaceState(null, "", `?${p.toString()}`);
                  window.dispatchEvent(new Event("popstate"));
                }}
              >
                Prev
              </button>
              <button
                className="rounded-lg border border-zinc-700 px-2 py-1 disabled:opacity-50"
                disabled={page.next_offset == null}
                onClick={() => {
                  const u = new URL(window.location.href);
                  const p = new URLSearchParams(u.search);
                  const limit = Number(p.get("limit") || 50);
                  const currentOffset = Number(p.get("offset") || 0);
                  const next = currentOffset + limit;
                  p.set("offset", String(next));
                  window.history.replaceState(null, "", `?${p.toString()}`);
                  window.dispatchEvent(new Event("popstate"));
                }}
              >
                Next
              </button>
            </div>
          )}
        </div>

        <table className="w-full text-sm">
          <thead className="bg-zinc-900/60">
            <tr className="text-left">
              {columns.map((c) => (
                <th key={c.key} className="px-3 py-2 font-medium text-zinc-300 border-b border-zinc-800">
                  <span className="inline-flex items-center gap-1">
                    {c.label}
                    {c.tip && (
                      <span className="text-zinc-400 cursor-help" title={c.tip}>?</span>
                    )}
                  </span>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.length === 0 && (
              <tr>
                <td className="px-3 py-6 text-center text-zinc-400" colSpan={columns.length}>
                  {loading ? "Loading…" : "No results. Try adjusting filters."}
                </td>
              </tr>
            )}
            {rows.map((r, i) => (
              <tr key={i} className="odd:bg-zinc-900/20 cursor-pointer hover:bg-zinc-900/40" onClick={() => onSelectRow(r)}>
                {columns.map((c) => {
                  let content: React.ReactNode = (r as any)[c.key as keyof Row];
                  if (c.key === "winrate" || c.key === "smoothed_wr") content = fmtPct((r as any)[c.key as keyof Row] as number);
                  if (c.key === "n") content = fmtInt(r.n);
                  if (c.key === "wr_ci") content = `${fmtPct(r.wr_ci_low)}–${fmtPct(r.wr_ci_high)}`;
                  if (c.key === "avg_gd10") content = (r.avg_gd10 ?? null) === null ? "—" : (r.avg_gd10 as number).toFixed(0);
                  return (
                    <td key={c.key} className="px-3 py-2 border-b border-zinc-900/50">
                      {content as any}
                      {c.key === "winrate" && r.low_sample && (
                        <span className="ml-2 rounded bg-amber-900/40 px-2 py-0.5 text-amber-300 text-[10px]">low n</span>
                      )}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <details className="rounded-2xl border border-zinc-800 p-4 mt-3">
        <summary className="cursor-pointer text-sm text-zinc-300">About these stats</summary>
        <div className="mt-3 text-sm text-zinc-300 space-y-2">
          <p><strong>WR (adj)</strong> uses Bayesian smoothing: <code>(winrate · n + prior_wr · alpha) / (n + alpha)</code>. This pulls low-sample rows toward the prior (default 50%) and fades out as <code>n</code> grows.</p>
          <p><strong>CI 95%</strong> is the Wilson confidence interval for the true win rate, which narrows as sample size increases.</p>
          <p><strong>Low n</strong> badge appears when <code>n &lt; warn_n</code> (default 25). Treat wide intervals and low-n rows with caution.</p>
          <p className="text-zinc-400">Tip: adjust <em>alpha</em> and <em>prior_wr</em> in Advanced to make smoothing stricter or looser.</p>
        </div>
      </details>
    </>
  );
}

// ---- Build Drawer (uses Data Dragon meta) ---------------------------------
function BuildDrawer({ row, tab, onClose, items, keystones, styles }: { row: Row | null; tab: string; onClose: () => void; items: MetaItems; keystones: MetaKeystones; styles: MetaStyles }) {
  if (!row) return null;
  const hasBuilds = row.mythic_id != null || row.boots_id != null || row.primary_keystone != null || row.secondary_style != null;

  const wr = typeof row.winrate === "number" ? (row.winrate * 100).toFixed(1) + "%" : "—";
  const wrAdj = typeof row.smoothed_wr === "number" ? (row.smoothed_wr * 100).toFixed(1) + "%" : "—";
  const ci = row.wr_ci_low != null && row.wr_ci_high != null ? `${(row.wr_ci_low * 100).toFixed(1)}%–${(row.wr_ci_high * 100).toFixed(1)}%` : "—";

  const title =
    tab === "bot2v2"
      ? `${row.champ} + ${row.my_duo ?? "?"} vs ${row.opponent ?? "?"} + ${row.opp_duo ?? "?"}`
      : tab === "lanejg"
        ? `${row.champ} vs ${row.opponent ?? "?"} (Enemy JG: ${row.enemy_jungler ?? "?"})`
        : `${row.champ} vs ${row.opponent ?? "?"}`;

  const mythic = row.mythic_id != null ? items[row.mythic_id] : undefined;
  const boots = row.boots_id != null ? items[row.boots_id] : undefined;
  const keystone = row.primary_keystone != null ? keystones[row.primary_keystone] : undefined;
  const secondary = row.secondary_style != null ? styles[row.secondary_style] : undefined;

  const runePage = {
    primary_keystone: row.primary_keystone ?? null,
    secondary_style: row.secondary_style ?? null,
  };

  return (
    <div className="fixed inset-0 z-40">
      <div className="absolute inset-0 bg-black/60" onClick={onClose} />
      <aside className="absolute right-0 top-0 h-full w-full sm:w-[480px] bg-zinc-950 border-l border-zinc-800 p-4 overflow-y-auto">
        <div className="flex items-start justify-between">
          <h2 className="text-lg font-semibold">{title}</h2>
          <button className="rounded-xl border border-zinc-700 px-2 py-1" onClick={onClose}>Close</button>
        </div>

        <div className="mt-3 grid grid-cols-2 gap-3 text-sm">
          <div className="rounded-xl border border-zinc-800 p-3"><div className="text-xs text-zinc-400">WR</div><div className="text-base">{wr}</div></div>
          <div className="rounded-xl border border-zinc-800 p-3"><div className="text-xs text-zinc-400">WR (adj)</div><div className="text-base">{wrAdj}</div></div>
          <div className="rounded-xl border border-zinc-800 p-3"><div className="text-xs text-zinc-400">CI 95%</div><div className="text-base">{ci}</div></div>
          <div className="rounded-xl border border-zinc-800 p-3"><div className="text-xs text-zinc-400">N</div><div className="text-base">{fmtInt(row.n)}</div></div>
        </div>

        <div className="mt-4 grid gap-3">
          <div className="rounded-2xl border border-zinc-800 p-3">
            <div className="text-sm font-medium mb-2">Recommended Runes</div>
            {hasBuilds ? (
              <div className="flex items-start gap-3 text-sm">
                <div className="flex items-center gap-2">
                  {keystone?.icon && <img src={keystone.icon} alt={keystone?.name} className="h-6 w-6 rounded" />}
                  <div>
                    <div className="text-xs text-zinc-400">Keystone</div>
                    <div className="font-medium">{keystone?.name ?? `Keystone ${row.primary_keystone ?? "—"}`}</div>
                  </div>
                </div>
                <div className="flex items-center gap-2">
                  {secondary?.icon && <img src={secondary.icon} alt={secondary?.name} className="h-6 w-6 rounded" />}
                  <div>
                    <div className="text-xs text-zinc-400">Secondary</div>
                    <div className="font-medium">{secondary?.name ?? `Style ${row.secondary_style ?? "—"}`}</div>
                  </div>
                </div>
                <button className="ml-auto rounded-lg border border-zinc-700 px-2 py-1 text-xs" onClick={() => navigator.clipboard.writeText(JSON.stringify(runePage))}>Copy JSON</button>
              </div>
            ) : (
              <div className="text-zinc-400 text-sm">No rune data in this view.</div>
            )}
          </div>

          <div className="rounded-2xl border border-zinc-800 p-3">
            <div className="text-sm font-medium mb-2">Recommended Items</div>
            {hasBuilds ? (
              <ul className="text-sm space-y-2">
                <li className="flex items-center gap-2">
                  {mythic?.icon && <img src={mythic.icon} alt={mythic?.name} className="h-6 w-6 rounded" />}
                  <span className="text-xs text-zinc-400 w-16">Core</span>
                  <span className="font-medium">{mythic?.name ?? (row.mythic_id != null ? `Item ${row.mythic_id}` : "—")}</span>
                </li>
                <li className="flex items-center gap-2">
                  {boots?.icon && <img src={boots.icon} alt={boots?.name} className="h-6 w-6 rounded" />}
                  <span className="text-xs text-zinc-400 w-16">Boots</span>
                  <span className="font-medium">{boots?.name ?? (row.boots_id != null ? `Item ${row.boots_id}` : "—")}</span>
                </li>
              </ul>
            ) : (
              <div className="text-zinc-400 text-sm">No item data in this view.</div>
            )}
          </div>
        </div>

        <p className="mt-3 text-xs text-zinc-500">Names/icons provided by Riot Data Dragon via your API's /meta endpoints.</p>
      </aside>
    </div>
  );
}

// ---- URL builder ---------------------------------------------------------
function buildUrl(tabKey: string, filters: Record<string, any>) {
  if (!API_BASE) return null;
  const tab = TABS.find((t) => t.key === tabKey)!;
  const base = tab.endpoint;
  const params: Record<string, any> = {
    // shared
    lane: filters.lane,
    champ: filters.champ,
    opponent: filters.opponent,
    patch: filters.patch,
    min_n: filters.min_n ?? 10,
    limit: filters.limit ?? 50,
    offset: filters.offset ?? 0,
    sort: filters.sort ?? "-n",
    alpha: filters.alpha ?? 20,
    prior_wr: filters.prior_wr ?? 0.5,
    warn_n: filters.warn_n ?? 25,
  };

  // view-specific
  if (tab.key === "lanejg") {
    params.enemy_jungler = filters.enemy_jungler;
    params.include_builds = true;
  } else if (tab.key === "bot2v2") {
    params.my_duo = filters.my_duo;
    params.opp_duo = filters.opp_duo;
    params.include_builds = true;
  }

  const url = `${API_BASE}${base}?${qs(params)}`;
  return url;
}

// ---- Tabs UI -------------------------------------------------------------
function Tabs({ value, onValueChange, children }: any) {
  return <div data-tabs-value={value}>{children}</div>;
}
function TabsList({ children }: any) {
  return <div className="flex gap-2">{children}</div>;
}
function TabsTrigger({ value, active, onClick, children }: any) {
  return (
    <button
      onClick={onClick}
      className={
        "rounded-2xl px-3 py-1.5 border " +
        (active
          ? "bg-zinc-100 text-black border-zinc-100"
          : "bg-zinc-900 border-zinc-800 text-zinc-300 hover:border-zinc-700")
      }
    >
      {children}
    </button>
  );
}
function TabsContent({ when, value, children }: any) {
  if (when !== value) return null;
  return <div className="mt-3">{children}</div>;
}

// ---- Page ---------------------------------------------------------------
export default function Page() {
  const [tab, setTab] = useState<(typeof TABS)[number]["key"]>("lanejg");
  const [filters, setFilters] = useState<Record<string, any>>({ limit: 50, sort: "-n", min_n: 10 });
  const [selected, setSelected] = useState<Row | null>(null);
  const [metaItems, setMetaItems] = useState<MetaItems>({});
  const [metaKeystones, setMetaKeystones] = useState<MetaKeystones>({});
  const [metaStyles, setMetaStyles] = useState<MetaStyles>({});

  // Sync offset in URL so back/forward works for pagination buttons
  useEffect(() => {
    const syncFromUrl = () => {
      const p = new URLSearchParams(window.location.search);
      const next: Record<string, any> = { ...filters };
      for (const k of [
        "lane",
        "champ",
        "opponent",
        "enemy_jungler",
        "my_duo",
        "opp_duo",
        "patch",
        "sort",
        "limit",
        "offset",
      ]) {
        if (p.has(k)) next[k] = p.get(k)!;
      }
      if (next.limit) next.limit = Number(next.limit);
      if (next.offset) next.offset = Number(next.offset);
      setFilters(next);
    };
    syncFromUrl();
    window.addEventListener("popstate", syncFromUrl);
    return () => window.removeEventListener("popstate", syncFromUrl);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Whenever filters change, push to URL (except for transient typeahead states)
  useEffect(() => {
    const p = new URLSearchParams();
    for (const [k, v] of Object.entries(filters)) if (v !== undefined && v !== "") p.set(k, String(v));
    const newQs = p.toString();
    const href = `${window.location.pathname}?${newQs}`;
    window.history.replaceState(null, "", href);
  }, [filters]);

  // Fetch Data Dragon meta once
  useEffect(() => {
    if (!API_BASE) return;
    fetch(`${API_BASE}/meta/items`).then(r => r.json()).then(j => setMetaItems(j.items || {})).catch(() => { });
    fetch(`${API_BASE}/meta/runes`).then(r => r.json()).then(j => { setMetaKeystones(j.keystones || {}); setMetaStyles(j.styles || {}); }).catch(() => { });
  }, []);

  const url = useMemo(() => buildUrl(tab, filters), [tab, filters]);

  return (
    <div className="mx-auto max-w-7xl p-4 space-y-4">
      <h1 className="text-xl font-semibold">League Context – Champ Select Assistant</h1>

      <QueryBar tab={tab} filters={filters} setFilters={(patch) => setFilters(patch)} />

      <div className="flex items-center justify-between">
        <Tabs value={tab} onValueChange={setTab}>
          <TabsList>
            {TABS.map((t) => (
              <TabsTrigger key={t.key} value={t.key} active={tab === t.key} onClick={() => setTab(t.key)}>
                {t.label}
              </TabsTrigger>
            ))}
          </TabsList>
        </Tabs>
        <div className="text-xs text-zinc-400">API: {API_BASE || "(set NEXT_PUBLIC_API_BASE)"}</div>
      </div>

      <TabsContent when={tab} value="lane">
        <MatchupTable url={url} tab={tab} onSelectRow={setSelected} />
      </TabsContent>
      <TabsContent when={tab} value="lanejg">
        <MatchupTable url={url} tab={tab} onSelectRow={setSelected} />
      </TabsContent>
      <TabsContent when={tab} value="bot2v2">
        <MatchupTable url={url} tab={tab} onSelectRow={setSelected} />
      </TabsContent>

      <BuildDrawer row={selected} tab={tab} onClose={() => setSelected(null)} items={metaItems} keystones={metaKeystones} styles={metaStyles} />
    </div>
  );
}
