import { useEffect, useMemo, useState } from "react";

// Point this at your FastAPI (or set NEXT_PUBLIC_API_BASE in .env.local)
const API_BASE = process.env.NEXT_PUBLIC_API_BASE ?? "http://localhost:8000";

type Role = "TOP" | "JUNGLE" | "MID" | "BOT_CARRY" | "SUPPORT";
type RoleFilter = { role?: Role | ""; champ_id?: number | "" };

type Summary = {
    n_games: number;
    winrate: number;        // 0..1
    gold_at_min: number;
    xp_at_min: number;
};

type TopItem = { item_id: number; item_name: string; picks: number };
type ChampOpt = { id: number; name: string };

const ROLES: Role[] = ["TOP", "JUNGLE", "MID", "BOT_CARRY", "SUPPORT"];

export default function FlexiblePage() {
    // Global filters
    const [patch, setPatch] = useState<string>("");
    const [skillTier, setSkillTier] = useState<string>("");
    const [minute, setMinute] = useState<number>(10);
    const [minN, setMinN] = useState<number>(20);

    // Subject (your pick)
    const [subject, setSubject] = useState<RoleFilter>({ role: "MID", champ_id: "" });

    // Optional constraints
    const [allyFilters, setAllyFilters] = useState<RoleFilter[]>([]);
    const [enemyFilters, setEnemyFilters] = useState<RoleFilter[]>([]);

    // Data Dragon champs
    const [champions, setChampions] = useState<ChampOpt[]>([]);
    const [loadingChamps, setLoadingChamps] = useState(false);

    // Query state
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [summary, setSummary] = useState<Summary | null>(null);
    const [topItems, setTopItems] = useState<TopItem[]>([]);

    // Load champions from Data Dragon (latest)
    useEffect(() => {
        let cancelled = false;
        async function loadChamps() {
            try {
                setLoadingChamps(true);
                setError(null);
                const vers = await fetch("https://ddragon.leagueoflegends.com/api/versions.json").then(r => r.json());
                const latest = vers[0];
                const data = await fetch(`https://ddragon.leagueoflegends.com/cdn/${latest}/data/en_US/champion.json`).then(r => r.json());
                const opts: ChampOpt[] = Object.values<any>(data.data)
                    .map((c: any) => ({ id: Number(c.key), name: c.name }))
                    .sort((a, b) => a.name.localeCompare(b.name));
                if (!cancelled) setChampions(opts);
            } catch (e) {
                if (!cancelled) setError("Failed to load champions from Data Dragon.");
            } finally {
                if (!cancelled) setLoadingChamps(false);
            }
        }
        loadChamps();
        return () => { cancelled = true; };
    }, []);

    const champById = useMemo(() => {
        const map = new Map<number, string>();
        champions.forEach(c => map.set(c.id, c.name));
        return map;
    }, [champions]);

    // Helpers to manage filter rows
    function updateFilter(list: RoleFilter[], setList: (v: RoleFilter[]) => void, idx: number, patch: Partial<RoleFilter>) {
        const copy = list.slice();
        copy[idx] = { ...copy[idx], ...patch };
        setList(copy);
    }
    function addFilter(list: RoleFilter[], setList: (v: RoleFilter[]) => void) {
        setList([...list, { role: "", champ_id: "" }]);
    }
    function removeFilter(list: RoleFilter[], setList: (v: RoleFilter[]) => void, idx: number) {
        const copy = list.slice();
        copy.splice(idx, 1);
        setList(copy);
    }

    async function runQuery() {
        setLoading(true);
        setError(null);
        setSummary(null);
        setTopItems([]);

        // Validate subject
        const subjRole = subject.role && String(subject.role).trim() !== "" ? subject.role : undefined;
        const subjChamp = subject.champ_id !== "" && subject.champ_id !== undefined && subject.champ_id !== null
            ? Number(subject.champ_id) : undefined;
        if (!subjRole && typeof subjChamp !== "number") {
            setLoading(false);
            setError("Please set your pick (role and/or champion).");
            return;
        }

        // Strip empty fields on arrays
        const clean = (arr: RoleFilter[]) =>
            arr
                .map(f => ({
                    role: f.role && String(f.role).trim() !== "" ? f.role : undefined,
                    champ_id:
                        f.champ_id === "" || f.champ_id === undefined || f.champ_id === null
                            ? undefined
                            : Number(f.champ_id),
                }))
                .filter(f => f.role || typeof f.champ_id === "number");

        const body = {
            patch: patch.trim() || null,
            skill_tier: skillTier.trim() || null,
            minute,
            min_n: minN,
            subject: { role: subjRole, champ_id: subjChamp },
            ally_filters: clean(allyFilters),
            enemy_filters: clean(enemyFilters),
        };

        try {
            const res = await fetch(`${API_BASE}/stats/flexible`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(body),
            });
            if (!res.ok) {
                const txt = await res.text();
                throw new Error(`${res.status} ${res.statusText}: ${txt}`);
            }
            const data = await res.json();
            setSummary(data.summary ?? null);
            setTopItems(Array.isArray(data.top_items) ? data.top_items : data.topItems ?? []);
        } catch (e: any) {
            setError(e.message || "Request failed.");
        } finally {
            setLoading(false);
        }
    }

    function resetForm() {
        setPatch("");
        setSkillTier("");
        setMinute(10);
        setMinN(20);
        setSubject({ role: "MID", champ_id: "" });
        setAllyFilters([]);
        setEnemyFilters([]);
        setSummary(null);
        setTopItems([]);
        setError(null);
    }

    // Purchase share: relative share among returned items (not a true pick-rate)
    const totalPurchases = topItems.reduce((acc, t) => acc + (t.picks || 0), 0);
    function pct(n: number, d: number) {
        if (!d) return "0.0%";
        return `${((n / d) * 100).toFixed(1)}%`;
    }

    return (
        <main className="min-h-screen bg-[#0b0f14] text-white px-6 py-8">
            <div className="max-w-6xl mx-auto">
                <header className="mb-8">
                    <h1 className="text-3xl font-bold">Pre-game Build Planner</h1>
                    <p className="text-gray-300 mt-1">
                        Tell me <span className="font-semibold">your pick</span> and the enemy draft. I’ll show
                        <span className="font-semibold"> your</span> gold/xp at minute and the items{" "}
                        <span className="font-semibold">you</span> buy most in those matchups.
                    </p>
                </header>

                {/* Query Card */}
                <section className="bg-[#101722] border border-[#1b2736] rounded-2xl shadow-md p-4 md:p-6 mb-6">
                    {/* Subject */}
                    <div className="mb-5">
                        <div className="flex items-center justify-between mb-2">
                            <h2 className="text-lg font-semibold">Your pick (subject)</h2>
                            {loadingChamps && <span className="text-sm text-gray-400">Loading champions…</span>}
                        </div>
                        <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
                            <div>
                                <label className="block text-sm text-gray-300 mb-1">Role</label>
                                <select
                                    value={subject.role ?? ""}
                                    onChange={(e) => setSubject(s => ({ ...s, role: (e.target.value as Role) || "" }))}
                                    className="w-full bg-[#0e1520] border border-[#223247] rounded-lg px-3 py-2"
                                >
                                    <option value="">(any role)</option>
                                    {ROLES.map(r => <option key={r} value={r}>{r}</option>)}
                                </select>
                            </div>
                            <div className="md:col-span-2">
                                <label className="block text-sm text-gray-300 mb-1">Champion</label>
                                <select
                                    value={String(subject.champ_id ?? "")}
                                    onChange={(e) => setSubject(s => ({ ...s, champ_id: e.target.value ? Number(e.target.value) : "" }))}
                                    className="w-full bg-[#0e1520] border border-[#223247] rounded-lg px-3 py-2"
                                >
                                    <option value="">(any champion)</option>
                                    {champions.map(c => <option key={c.id} value={c.id}>{c.name} (#{c.id})</option>)}
                                </select>
                            </div>
                        </div>
                    </div>

                    {/* Ally constraints */}
                    <div className="mb-5">
                        <h3 className="text-lg font-semibold mb-2">Ally constraints (optional)</h3>
                        {allyFilters.length === 0 && (
                            <p className="text-gray-400 text-sm mb-2">None. Add to require specific teammates (e.g., “ally JG Lee Sin”).</p>
                        )}
                        {allyFilters.map((f, idx) => (
                            <div key={`ally-${idx}`} className="bg-[#0e1520] border border-[#223247] rounded-xl p-3 mb-2 grid grid-cols-12 gap-2">
                                <div className="col-span-12 md:col-span-3">
                                    <label className="block text-xs text-gray-400 mb-1">Role</label>
                                    <select
                                        value={f.role ?? ""}
                                        onChange={e => updateFilter(allyFilters, setAllyFilters, idx, { role: (e.target.value as Role) || "" })}
                                        className="w-full bg-[#0b121c] border border-[#223247] rounded-lg px-3 py-2"
                                    >
                                        <option value="">(any)</option>
                                        {ROLES.map(r => <option key={r} value={r}>{r}</option>)}
                                    </select>
                                </div>
                                <div className="col-span-12 md:col-span-8">
                                    <label className="block text-xs text-gray-400 mb-1">Champion</label>
                                    <select
                                        value={String(f.champ_id ?? "")}
                                        onChange={e => updateFilter(allyFilters, setAllyFilters, idx, { champ_id: e.target.value ? Number(e.target.value) : "" })}
                                        className="w-full bg-[#0b121c] border border-[#223247] rounded-lg px-3 py-2"
                                    >
                                        <option value="">(any champion)</option>
                                        {champions.map(c => <option key={c.id} value={c.id}>{c.name} (#{c.id})</option>)}
                                    </select>
                                </div>
                                <div className="col-span-12 md:col-span-1 flex md:justify-end">
                                    <button
                                        onClick={() => removeFilter(allyFilters, setAllyFilters, idx)}
                                        className="w-full md:w-auto bg-[#152233] hover:bg-[#1a2b40] border border-[#2a3b52] rounded-lg px-3 py-2 text-sm"
                                    >
                                        Remove
                                    </button>
                                </div>
                            </div>
                        ))}
                        <button
                            onClick={() => addFilter(allyFilters, setAllyFilters)}
                            className="bg-[#1a2b40] hover:bg-[#213652] border border-[#2a3b52] rounded-lg px-3 py-2 text-sm"
                        >
                            + Add ally constraint
                        </button>
                    </div>

                    {/* Enemy filters */}
                    <div className="mb-5">
                        <h3 className="text-lg font-semibold mb-2">Enemy filters</h3>
                        {enemyFilters.length === 0 && (
                            <p className="text-gray-400 text-sm mb-2">
                                Add the opponents you expect (e.g., enemy MID Zed, enemy JG Lee Sin, enemy BOT Caitlyn).
                            </p>
                        )}
                        {enemyFilters.map((f, idx) => (
                            <div key={`enemy-${idx}`} className="bg-[#0e1520] border border-[#223247] rounded-xl p-3 mb-2 grid grid-cols-12 gap-2">
                                <div className="col-span-12 md:col-span-3">
                                    <label className="block text-xs text-gray-400 mb-1">Role</label>
                                    <select
                                        value={f.role ?? ""}
                                        onChange={e => updateFilter(enemyFilters, setEnemyFilters, idx, { role: (e.target.value as Role) || "" })}
                                        className="w-full bg-[#0b121c] border border-[#223247] rounded-lg px-3 py-2"
                                    >
                                        <option value="">(any)</option>
                                        {ROLES.map(r => <option key={r} value={r}>{r}</option>)}
                                    </select>
                                </div>
                                <div className="col-span-12 md:col-span-8">
                                    <label className="block text-xs text-gray-400 mb-1">Champion</label>
                                    <select
                                        value={String(f.champ_id ?? "")}
                                        onChange={e => updateFilter(enemyFilters, setEnemyFilters, idx, { champ_id: e.target.value ? Number(e.target.value) : "" })}
                                        className="w-full bg-[#0b121c] border border-[#223247] rounded-lg px-3 py-2"
                                    >
                                        <option value="">(any champion)</option>
                                        {champions.map(c => <option key={c.id} value={c.id}>{c.name} (#{c.id})</option>)}
                                    </select>
                                </div>
                                <div className="col-span-12 md:col-span-1 flex md:justify-end">
                                    <button
                                        onClick={() => removeFilter(enemyFilters, setEnemyFilters, idx)}
                                        className="w-full md:w-auto bg-[#152233] hover:bg-[#1a2b40] border border-[#2a3b52] rounded-lg px-3 py-2 text-sm"
                                    >
                                        Remove
                                    </button>
                                </div>
                            </div>
                        ))}
                        <button
                            onClick={() => addFilter(enemyFilters, setEnemyFilters)}
                            className="bg-[#1a2b40] hover:bg-[#213652] border border-[#2a3b52] rounded-lg px-3 py-2 text-sm"
                        >
                            + Add enemy filter
                        </button>
                    </div>

                    {/* Advanced */}
                    <details className="mb-4">
                        <summary className="cursor-pointer text-gray-200 select-none">Advanced options</summary>
                        <div className="grid grid-cols-1 md:grid-cols-4 gap-3 mt-3">
                            <div>
                                <label className="block text-sm text-gray-300 mb-1">Patch</label>
                                <input
                                    value={patch}
                                    onChange={e => setPatch(e.target.value)}
                                    placeholder="e.g. 25.17"
                                    className="w-full bg-[#0e1520] border border-[#223247] rounded-lg px-3 py-2"
                                />
                            </div>
                            <div>
                                <label className="block text-sm text-gray-300 mb-1">Skill tier</label>
                                <input
                                    value={skillTier}
                                    onChange={e => setSkillTier(e.target.value)}
                                    placeholder="e.g. MASTER_PLUS"
                                    className="w-full bg-[#0e1520] border border-[#223247] rounded-lg px-3 py-2"
                                />
                            </div>
                            <div>
                                <label className="block text-sm text-gray-300 mb-1">Minute</label>
                                <input
                                    type="number"
                                    min={3}
                                    max={30}
                                    value={minute}
                                    onChange={e => setMinute(Number(e.target.value))}
                                    className="w-full bg-[#0e1520] border border-[#223247] rounded-lg px-3 py-2"
                                />
                            </div>
                            <div>
                                <label className="block text-sm text-gray-300 mb-1">Min games (n)</label>
                                <input
                                    type="number"
                                    min={1}
                                    value={minN}
                                    onChange={e => setMinN(Number(e.target.value))}
                                    className="w-full bg-[#0e1520] border border-[#223247] rounded-lg px-3 py-2"
                                />
                            </div>
                        </div>
                    </details>

                    {/* Actions */}
                    <div className="flex flex-wrap gap-3">
                        <button
                            onClick={runQuery}
                            disabled={loading}
                            className="bg-[#2b7fff] hover:bg-[#1f6be0] disabled:opacity-60 text-black font-semibold rounded-lg px-4 py-2"
                        >
                            {loading ? "Loading..." : "Run query"}
                        </button>
                        <button
                            onClick={resetForm}
                            disabled={loading}
                            className="bg-[#0e1520] border border-[#223247] hover:bg-[#121b29] rounded-lg px-4 py-2"
                        >
                            Reset
                        </button>
                        {error && <span className="text-red-400 text-sm self-center">⚠️ {error}</span>}
                    </div>
                </section>

                {/* Results */}
                <section className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                    {/* Summary card */}
                    <div className="lg:col-span-1 bg-[#101722] border border-[#1b2736] rounded-2xl shadow-md p-5">
                        <h3 className="text-lg font-semibold mb-4">Summary</h3>
                        {!summary ? (
                            <div className="text-gray-400 text-sm">No results yet.</div>
                        ) : (
                            <div className="grid grid-cols-2 gap-4">
                                <Metric label="Games" value={summary.n_games.toLocaleString()} />
                                <Metric label="Winrate" value={`${(summary.winrate * 100).toFixed(1)}%`} />
                                <Metric label={`Gold @ ${minute}`} value={Math.round(summary.gold_at_min).toLocaleString()} />
                                <Metric label={`XP @ ${minute}`} value={Math.round(summary.xp_at_min).toLocaleString()} />
                            </div>
                        )}
                    </div>

                    {/* Top items card */}
                    <div className="lg:col-span-2 bg-[#101722] border border-[#1b2736] rounded-2xl shadow-md p-5">
                        <h3 className="text-lg font-semibold mb-4">Top items (your purchases)</h3>
                        {!topItems || topItems.length === 0 ? (
                            <div className="text-gray-400 text-sm">No item data for this filter set.</div>
                        ) : (
                            <div className="overflow-auto">
                                <table className="min-w-full text-sm">
                                    <thead>
                                        <tr className="text-gray-300">
                                            <th className="text-left pb-2">Item</th>
                                            <th className="text-left pb-2">ID</th>
                                            <th className="text-right pb-2">Purchases</th>
                                            <th className="text-left pb-2 w-48">Share</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {topItems.map((it) => {
                                            const share = totalPurchases ? it.picks / totalPurchases : 0;
                                            return (
                                                <tr key={it.item_id} className="border-t border-[#1b2736]">
                                                    <td className="py-2 pr-2">{it.item_name}</td>
                                                    <td className="py-2 pr-2 text-gray-300">#{it.item_id}</td>
                                                    <td className="py-2 pr-2 text-right">{it.picks.toLocaleString()}</td>
                                                    <td className="py-2 pr-2">
                                                        <div className="flex items-center gap-2">
                                                            <div className="flex-1 h-2 rounded bg-[#0e1520]">
                                                                <div className="h-2 rounded bg-[#2b7fff]" style={{ width: `${share * 100}%` }} />
                                                            </div>
                                                            <div className="w-14 text-right text-gray-300">{pct(it.picks, totalPurchases)}</div>
                                                        </div>
                                                    </td>
                                                </tr>
                                            );
                                        })}
                                    </tbody>
                                </table>
                                <div className="text-xs text-gray-400 mt-2">
                                    * “Share” is the proportion of purchases among items shown. For true per-match pick rates or per-item winrates, we can extend the API.
                                </div>
                            </div>
                        )}
                    </div>
                </section>
            </div>
        </main>
    );
}

function Metric(props: { label: string; value: string }) {
    return (
        <div className="bg-[#0e1520] border border-[#223247] rounded-xl p-3">
            <div className="text-gray-400 text-xs">{props.label}</div>
            <div className="text-xl font-bold mt-1">{props.value}</div>
        </div>
    );
}
