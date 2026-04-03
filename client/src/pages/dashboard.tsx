import { useState } from "react";
import { useQuery, useMutation } from "@tanstack/react-query";
import { apiRequest, queryClient } from "@/lib/queryClient";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { Badge } from "@/components/ui/badge";
import {
  BarChart, Bar, LineChart, Line, XAxis, YAxis, CartesianGrid,
  ResponsiveContainer, Tooltip as RechartsTooltip, Cell, PieChart, Pie,
  Area, AreaChart, ComposedChart, ReferenceLine
} from "recharts";
import {
  TrendingUp, TrendingDown, Activity, Users, DollarSign,
  BarChart3, Signal, Clock, AlertTriangle, Zap, Target,
  ArrowUpRight, ArrowDownRight, Minus, Database, Briefcase,
  LineChart as LineChartIcon, Settings, Shield, Link, ExternalLink,
  ChevronRight, Award, Percent, Layers, FlaskConical, Crosshair,
  GitCompareArrows, Info, Hash, Timer, Eye, EyeOff, Gauge, Scale
} from "lucide-react";

// ============================================================
// SHARED HELPERS
// ============================================================

function MetricTooltip({ children, title, formula, description }: {
  children: React.ReactNode;
  title: string;
  formula?: string;
  description: string;
}) {
  return (
    <Tooltip delayDuration={200}>
      <TooltipTrigger asChild>{children}</TooltipTrigger>
      <TooltipContent side="bottom" className="max-w-xs bg-popover border border-border p-3">
        <div className="space-y-1.5">
          <div className="font-semibold text-foreground text-xs uppercase tracking-wider">{title}</div>
          <div className="text-muted-foreground text-[11px] leading-relaxed">{description}</div>
          {formula && (
            <div className="text-primary font-mono text-[10px] bg-background/50 px-2 py-1 rounded mt-1">
              {formula}
            </div>
          )}
        </div>
      </TooltipContent>
    </Tooltip>
  );
}

function formatCurrency(val: number): string {
  const abs = Math.abs(val);
  const sign = val < 0 ? "-" : "";
  if (abs >= 1_000_000_000_000) return `${sign}$${(abs / 1_000_000_000_000).toFixed(1)}T`;
  if (abs >= 1_000_000_000) return `${sign}$${(abs / 1_000_000_000).toFixed(1)}B`;
  if (abs >= 1_000_000) return `${sign}$${(abs / 1_000_000).toFixed(1)}M`;
  if (abs >= 1_000) return `${sign}$${(abs / 1_000).toFixed(0)}K`;
  return `${sign}$${abs.toFixed(0)}`;
}

function formatNumber(val: number): string {
  return val.toLocaleString("en-US");
}

function formatPct(val: number, decimals = 2): string {
  const sign = val >= 0 ? "+" : "";
  return `${sign}${val.toFixed(decimals)}%`;
}

function formatRatio(val: number): string {
  return val.toFixed(2);
}

function timeAgo(dateStr: string | null): string {
  if (!dateStr) return "—";
  const d = new Date(dateStr);
  const now = new Date();
  const diffMs = now.getTime() - d.getTime();
  const diffMin = Math.floor(diffMs / 60000);
  if (diffMin < 1) return "just now";
  if (diffMin < 60) return `${diffMin}m ago`;
  const diffHr = Math.floor(diffMin / 60);
  if (diffHr < 24) return `${diffHr}h ago`;
  return `${Math.floor(diffHr / 24)}d ago`;
}

function getSignalClass(score: number): string {
  if (score >= 70) return "signal-high";
  if (score >= 40) return "signal-mid";
  return "signal-low";
}

function getSignalLabel(score: number): string {
  if (score >= 70) return "STRONG";
  if (score >= 40) return "MODERATE";
  return "WEAK";
}

function pnlColor(val: number): string {
  if (val > 0) return "text-gain";
  if (val < 0) return "text-loss";
  return "text-muted-foreground";
}

function pnlBg(val: number): string {
  if (val > 0) return "bg-gain";
  if (val < 0) return "bg-loss";
  return "";
}

function tierBadge(score: number): { bg: string; text: string; border: string } {
  if (score >= 80) return { bg: "bg-amber-500/15", text: "text-amber-400", border: "border-amber-500/30" };
  if (score >= 60) return { bg: "bg-amber-700/15", text: "text-amber-600", border: "border-amber-700/30" };
  if (score >= 40) return { bg: "bg-gray-500/15", text: "text-gray-400", border: "border-gray-500/30" };
  return { bg: "bg-gray-700/15", text: "text-gray-600", border: "border-gray-700/30" };
}

function actionBadge(action: string | undefined): { bg: string; text: string } {
  if (!action) return { bg: "bg-muted", text: "text-muted-foreground" };
  const a = action.toUpperCase();
  if (a.includes("BUY")) return { bg: "bg-green-500/15", text: "text-green-400" };
  if (a.includes("WATCH")) return { bg: "bg-amber-500/15", text: "text-amber-400" };
  return { bg: "bg-gray-500/15", text: "text-gray-500" };
}

// === KPI Card ===
function KPICard({ label, value, subtitle, icon: Icon, tooltip, tooltipFormula, tooltipDesc, valueClass }: {
  label: string;
  value: string;
  subtitle?: string;
  icon: any;
  tooltip: string;
  tooltipFormula?: string;
  tooltipDesc: string;
  valueClass?: string;
}) {
  return (
    <MetricTooltip title={tooltip} formula={tooltipFormula} description={tooltipDesc}>
      <div className="bg-card border border-border rounded-md p-3 flex flex-col gap-1 cursor-help" data-testid={`kpi-${label.toLowerCase().replace(/\s/g, "-")}`}>
        <div className="flex items-center justify-between">
          <span className="text-[10px] uppercase tracking-widest text-muted-foreground">{label}</span>
          <Icon className="w-3.5 h-3.5 text-muted-foreground" />
        </div>
        <div className={`text-lg font-bold leading-none ${valueClass || "text-foreground"}`} style={{ fontVariantNumeric: "tabular-nums" }}>{value}</div>
        {subtitle && <div className="text-[10px] text-muted-foreground">{subtitle}</div>}
      </div>
    </MetricTooltip>
  );
}

// === Signal Score Bar ===
function SignalBar({ score }: { score: number }) {
  const width = Math.max(score, 2);
  const color = score >= 70 ? "hsl(37, 90%, 55%)" : score >= 40 ? "hsl(200, 65%, 50%)" : "hsl(210, 6%, 40%)";
  return (
    <div className="w-full h-1.5 bg-muted rounded-full overflow-hidden">
      <div
        className="h-full rounded-full transition-all duration-500"
        style={{ width: `${width}%`, background: color }}
      />
    </div>
  );
}

// === Chart Tooltip ===
function ChartTooltipContent({ active, payload, label }: any) {
  if (!active || !payload?.length) return null;
  return (
    <div className="bg-popover border border-border rounded p-2 text-[11px]">
      <div className="text-muted-foreground mb-1">{label}</div>
      {payload.map((entry: any, i: number) => (
        <div key={i} className="flex items-center gap-2">
          <div className="w-2 h-2 rounded-full" style={{ background: entry.color }} />
          <span className="text-foreground">
            {entry.name}: {typeof entry.value === "number" && Math.abs(entry.value) >= 1000 ? formatCurrency(entry.value) : entry.value}
          </span>
        </div>
      ))}
    </div>
  );
}

function PerfTooltipContent({ active, payload, label }: any) {
  if (!active || !payload?.length) return null;
  return (
    <div className="bg-popover border border-border rounded p-2 text-[11px]">
      <div className="text-muted-foreground mb-1">{label}</div>
      {payload.map((entry: any, i: number) => (
        <div key={i} className="flex items-center gap-2">
          <div className="w-2 h-2 rounded-full" style={{ background: entry.color }} />
          <span className="text-foreground">
            {entry.name}: {formatPct(entry.value)}
          </span>
        </div>
      ))}
    </div>
  );
}

function EmptyState({ message, icon: Icon }: { message: string; icon?: any }) {
  const I = Icon || Info;
  return (
    <div className="flex flex-col items-center justify-center py-8 text-muted-foreground gap-2">
      <I className="w-5 h-5 opacity-40" />
      <span className="text-[11px] text-center max-w-xs">{message}</span>
    </div>
  );
}

// ====================================================================
// TAB 1: SIGNALS
// ====================================================================
function SignalsTab() {
  const { data: dashboard } = useQuery<any>({
    queryKey: ["/api/dashboard"],
    refetchInterval: 30000,
  });

  const { data: signals } = useQuery<any[]>({
    queryKey: ["/api/signals"],
    queryFn: async () => {
      const res = await apiRequest("GET", "/api/signals?limit=50");
      return res.json();
    },
    refetchInterval: 30000,
  });

  const { data: dailyVolume } = useQuery<any[]>({
    queryKey: ["/api/analytics/daily-volume"],
    queryFn: async () => {
      const res = await apiRequest("GET", "/api/analytics/daily-volume?days=30");
      return res.json();
    },
    refetchInterval: 60000,
  });

  const { data: clusterBuys } = useQuery<any[]>({
    queryKey: ["/api/analytics/cluster-buys"],
    queryFn: async () => {
      const res = await apiRequest("GET", "/api/analytics/cluster-buys?days=30");
      return res.json();
    },
    refetchInterval: 60000,
  });

  const kpis = dashboard?.kpis;

  return (
    <div className="space-y-3">
      {/* KPI Row — 7 cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-7 gap-2">
        <KPICard label="Purchases 30D" value={kpis ? formatNumber(kpis.purchaseCount30d) : "—"}
          subtitle={kpis ? `${formatNumber(kpis.purchaseCount7d)} in 7D` : undefined} icon={TrendingUp}
          tooltip="Purchase Count (30D)" tooltipFormula="COUNT(tx) WHERE type='P' AND date >= T-30"
          tooltipDesc="Total open-market purchase transactions filed via Form 4 in the trailing 30 calendar days." />
        <KPICard label="Volume 30D" value={kpis ? formatCurrency(kpis.volume30d) : "—"}
          subtitle={kpis ? `${formatCurrency(kpis.volume7d)} in 7D` : undefined} icon={DollarSign}
          tooltip="Purchase Volume (30D)" tooltipFormula="SUM(shares × price) WHERE type='P' AND date >= T-30"
          tooltipDesc="Aggregate dollar value of all insider open-market purchases in the trailing 30 days." />
        <KPICard label="Today" value={kpis ? formatNumber(kpis.purchaseCount1d) : "—"} icon={Zap}
          tooltip="Today's Purchases" tooltipFormula="COUNT(tx) WHERE type='P' AND date = TODAY"
          tooltipDesc="Number of open-market purchase transactions filed today." />
        <KPICard label="Cluster Buys" value={kpis ? formatNumber(kpis.clusterCount) : "—"} subtitle="2+ insiders" icon={Users}
          tooltip="Cluster Buy Count" tooltipFormula="COUNT(DISTINCT ticker) WHERE insider_count >= 2 AND date >= T-30"
          tooltipDesc="Companies where 2+ distinct insiders made open-market purchases in trailing 30 days. Cluster buying is a stronger predictor of abnormal returns." />
        <KPICard label="Avg Score" value={signals && signals.length > 0 ? `${Math.round(signals.slice(0, 10).reduce((sum: number, s: any) => sum + (s.signalScore || 0), 0) / Math.min(signals.length, 10))}` : "—"} subtitle="top 10" icon={Signal}
          tooltip="Average Signal Score" tooltipFormula="AVG(signal_score) across top 10 signals"
          tooltipDesc="Mean composite signal score of top 10 signals. Derived from 11 empirical factors weighted by predictive power." />
        <KPICard label="Total Filed" value={kpis ? formatNumber(kpis.totalTransactions) : "—"} icon={Database}
          tooltip="Total Transactions" tooltipDesc="All Form 4 transactions stored, including purchases, sales, grants, and options." />
        <KPICard label="Purchases 7D" value={kpis ? formatNumber(kpis.purchaseCount7d) : "—"} icon={Activity}
          tooltip="Purchase Count (7D)" tooltipFormula="COUNT(tx) WHERE type='P' AND date >= T-7"
          tooltipDesc="Open-market purchase count in trailing 7 days. Detect acceleration/deceleration vs 30D baseline." />
      </div>

      {/* Charts + Signal Feed */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-3">
        {/* Daily Volume Chart */}
        <div className="lg:col-span-2 bg-card border border-border rounded-md p-3">
          <MetricTooltip title="Daily Purchase Volume" description="Aggregated dollar value of insider open-market purchases by filing date. Volume spikes often precede positive price movements.">
            <h3 className="text-[10px] uppercase tracking-widest text-muted-foreground mb-3 cursor-help">Daily Purchase Volume (30D)</h3>
          </MetricTooltip>
          <div className="h-48">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={dailyVolume || []}>
                <defs>
                  <linearGradient id="volumeGrad" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="hsl(37, 90%, 55%)" stopOpacity={0.3} />
                    <stop offset="95%" stopColor="hsl(37, 90%, 55%)" stopOpacity={0} />
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="hsl(220, 10%, 14%)" />
                <XAxis dataKey="date" tick={{ fontSize: 9, fill: "hsl(210, 6%, 50%)" }}
                  tickFormatter={(v) => v ? v.slice(5) : ""} stroke="hsl(220, 10%, 14%)" />
                <YAxis tick={{ fontSize: 9, fill: "hsl(210, 6%, 50%)" }}
                  tickFormatter={(v) => formatCurrency(v)} stroke="hsl(220, 10%, 14%)" width={55} />
                <RechartsTooltip content={<ChartTooltipContent />} />
                <Area type="monotone" dataKey="volume" stroke="hsl(37, 90%, 55%)" fill="url(#volumeGrad)" strokeWidth={1.5} name="Volume" />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Signal Feed */}
        <div className="bg-card border border-border rounded-md p-3 flex flex-col">
          <MetricTooltip title="Signal Feed" description="Ranked insider purchase signals. Composite scores derived from 11 empirical factors weighted by forward-return predictive power." formula="Score = Σ(factor_weight × factor_value)">
            <h3 className="text-[10px] uppercase tracking-widest text-muted-foreground mb-2 cursor-help">Signal Feed — Top Ranked</h3>
          </MetricTooltip>
          <div className="flex-1 overflow-y-auto space-y-1 min-h-0" style={{ overscrollBehavior: "contain" }}>
            {(signals || []).slice(0, 15).map((sig: any, i: number) => {
              const tier = tierBadge(sig.compositeScore ?? sig.signalScore ?? 0);
              const score = sig.compositeScore ?? sig.signalScore ?? 0;
              const action = actionBadge(sig.recommendedAction);
              return (
                <div key={sig.id || i} className="terminal-row rounded px-2 py-1.5 border border-transparent hover:border-border transition-colors" data-testid={`signal-${i}`}>
                  <div className="flex items-center justify-between mb-0.5">
                    <div className="flex items-center gap-2">
                      <span className={`text-[11px] px-1.5 py-0.5 rounded font-bold border ${tier.bg} ${tier.text} ${tier.border}`} style={{ fontVariantNumeric: "tabular-nums" }}>{score}</span>
                      <span className="text-xs font-bold text-primary">{sig.issuerTicker || sig.ticker || "—"}</span>
                      <span className="text-[10px] text-muted-foreground truncate max-w-[100px]">{sig.issuerName || sig.companyName || ""}</span>
                    </div>
                    <div className="flex items-center gap-1.5">
                      {sig.recommendedAction && (
                        <span className={`text-[9px] px-1.5 py-0.5 rounded font-bold ${action.bg} ${action.text}`}>{sig.recommendedAction}</span>
                      )}
                      <span className="text-[9px] text-muted-foreground">{sig.signalDate || sig.filingDate || ""}</span>
                    </div>
                  </div>
                  {/* Factor breakdown bar */}
                  {sig.factorBreakdown && (
                    <div className="flex h-1 rounded-full overflow-hidden mb-1 gap-px">
                      {Object.entries(sig.factorBreakdown as Record<string, number>).map(([k, v]: [string, any]) => (
                        <div key={k} className="h-full" style={{ width: `${Math.max(v, 2)}%`, background: v > 15 ? "hsl(37, 90%, 55%)" : v > 8 ? "hsl(200, 65%, 50%)" : "hsl(210, 6%, 30%)" }} />
                      ))}
                    </div>
                  )}
                  {/* Comparable signals */}
                  {sig.comparableCount != null && sig.comparableCount > 0 && (
                    <div className="text-[9px] text-muted-foreground mb-0.5">
                      {sig.comparableCount} similar signals → avg 63d return: <span className={pnlColor(sig.comparableAvgReturn || 0)}>{formatPct(sig.comparableAvgReturn || 0)}</span>, win rate: {((sig.comparableWinRate || 0) * 100).toFixed(0)}%
                    </div>
                  )}
                  <div className="flex items-center gap-2 text-[10px] text-muted-foreground flex-wrap">
                    <span>{sig.clusterSize || sig.insiderCount || 1} insider{(sig.clusterSize || sig.insiderCount || 1) > 1 ? "s" : ""}</span>
                    <span>{formatCurrency(sig.totalPurchaseValue || sig.totalValue || 0)}</span>
                    {(sig.cSuiteCount > 0 || sig.hasCsuite) && <span className="text-primary">{sig.cSuiteCount || "+"} C-Suite</span>}
                    {sig.insiderName && <span className="truncate max-w-[90px]">{sig.insiderName}</span>}
                    {sig.insiderRole && <span className="text-[9px]">({sig.insiderRole})</span>}
                    {sig.ownershipChangePct != null && <span>{formatPct(sig.ownershipChangePct, 1)} own</span>}
                    {sig.filingLag != null && (
                      <span className={sig.filingLag <= 2 ? "text-green-500" : "text-amber-500"}>{sig.filingLag}d lag</span>
                    )}
                    {sig.isDirect != null && (
                      <span className={sig.isDirect ? "text-primary" : "text-muted-foreground"}>{sig.isDirect ? "Direct" : "Indirect"}</span>
                    )}
                    {sig.isOpportunistic && <span className="text-green-500">Opp</span>}
                  </div>
                  <SignalBar score={score} />
                </div>
              );
            })}
            {(!signals || signals.length === 0) && (
              <EmptyState message="No signals yet — collecting data..." icon={Signal} />
            )}
          </div>
        </div>
      </div>

      {/* Cluster Buys Table */}
      <div className="bg-card border border-border rounded-md p-3">
        <MetricTooltip title="Cluster Buy Analysis" description="Companies with multiple insiders buying within 30 days. Cluster buying generates 2-3x higher abnormal returns (Jeng, Metrick & Zeckhauser 2003).">
          <h3 className="text-[10px] uppercase tracking-widest text-muted-foreground mb-2 cursor-help">Cluster Buy Detection (30D)</h3>
        </MetricTooltip>
        <div className="overflow-auto max-h-48" style={{ overscrollBehavior: "contain" }}>
          <table className="w-full text-[11px]">
            <thead className="sticky top-0 bg-card z-10">
              <tr className="border-b border-border text-muted-foreground">
                <th className="text-left py-1.5 px-2 font-medium">TICKER</th>
                <th className="text-left py-1.5 px-2 font-medium">ISSUER</th>
                <th className="text-right py-1.5 px-2 font-medium"># INSIDERS</th>
                <th className="text-right py-1.5 px-2 font-medium">TOTAL VALUE</th>
                <th className="text-right py-1.5 px-2 font-medium">AVG PRICE</th>
                <th className="text-left py-1.5 px-2 font-medium">PERIOD</th>
              </tr>
            </thead>
            <tbody>
              {(clusterBuys || []).map((cluster: any, i: number) => (
                <tr key={i} className="terminal-row border-b border-border/30">
                  <td className="py-1.5 px-2 font-bold text-primary">{cluster.ticker}</td>
                  <td className="py-1.5 px-2 text-foreground truncate max-w-[180px]">{cluster.name}</td>
                  <td className="py-1.5 px-2 text-right">
                    <span className={cluster.insiderCount >= 3 ? "text-primary font-bold" : ""}>{cluster.insiderCount}</span>
                  </td>
                  <td className="py-1.5 px-2 text-right text-gain font-medium">{formatCurrency(cluster.totalValue)}</td>
                  <td className="py-1.5 px-2 text-right">${cluster.avgPrice?.toFixed(2)}</td>
                  <td className="py-1.5 px-2 text-muted-foreground text-[10px]">{cluster.dates}</td>
                </tr>
              ))}
              {(!clusterBuys || clusterBuys.length === 0) && (
                <tr><td colSpan={6} className="text-center py-6 text-muted-foreground">No cluster buys detected</td></tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}


// ====================================================================
// TAB 2: FACTOR RESEARCH
// ====================================================================
function FactorResearchTab() {
  const [selectedFactor, setSelectedFactor] = useState<string>("");

  const { data: effectiveness } = useQuery<any[]>({
    queryKey: ["/api/factors/effectiveness"],
    queryFn: async () => {
      const res = await apiRequest("GET", "/api/factors/effectiveness");
      return res.json();
    },
    refetchInterval: 120000,
    staleTime: 120000,
    retry: 3,
    retryDelay: 2000,
  });

  // Need to compute effectiveSelectedFactor early for heatmap query
  const firstFactorName = (effectiveness || [])[0]?.factor_name || (effectiveness || [])[0]?.factorName || "";
  // Default to "ownership_type" (known good factor) if no factor selected and effectiveness not loaded yet
  const heatmapFactor = selectedFactor || firstFactorName || "ownership_type";

  const { data: heatmapDataRaw, isLoading: heatmapLoading } = useQuery<any>({
    queryKey: ["/api/factors/heatmap", heatmapFactor],
    queryFn: async () => {
      if (!heatmapFactor) return null;
      const res = await apiRequest("GET", `/api/factors/heatmap/${encodeURIComponent(heatmapFactor)}`);
      return res.json();
    },
    enabled: !!heatmapFactor,
    staleTime: 120000,
    retry: 3,
    retryDelay: 2000,
    refetchInterval: 120000,
  });
  // Ensure heatmapData is always an array
  const heatmapData: any[] = Array.isArray(heatmapDataRaw) ? heatmapDataRaw : [];

  const { data: alphaDecayRaw, isLoading: alphaDecayLoading } = useQuery<any>({
    queryKey: ["/api/factors/alpha-decay"],
    queryFn: async () => {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 60000);
      try {
        const res = await fetch("/api/factors/alpha-decay", { signal: controller.signal });
        if (!res.ok) throw new Error(`${res.status}: ${res.statusText}`);
        return res.json();
      } finally {
        clearTimeout(timeout);
      }
    },
    refetchInterval: 300000,
    staleTime: 300000,
    retry: 3,
    retryDelay: 5000,
  });
  // Ensure alphaDecay is always an array (API may return array directly or wrapped)
  const alphaDecay: any[] = Array.isArray(alphaDecayRaw) ? alphaDecayRaw : [];

  const { data: modelWeightsRaw, isLoading: modelWeightsLoading } = useQuery<any[]>({
    queryKey: ["/api/factors/model-weights"],
    queryFn: async () => {
      const res = await apiRequest("GET", "/api/factors/model-weights");
      return res.json();
    },
    refetchInterval: 120000,
    staleTime: 120000,
    retry: 3,
    retryDelay: 2000,
  });
  // Ensure modelWeights is always an array (API may return array or object)
  const modelWeights: any[] = Array.isArray(modelWeightsRaw) ? modelWeightsRaw : [];

  // Auto-select first factor when data loads
  const factors = effectiveness || [];

  const effectiveSelectedFactor = heatmapFactor;

  // Significance color
  function sigColor(tStat: number): string {
    if (Math.abs(tStat) >= 2) return "border-green-500/40 bg-green-500/5";
    if (Math.abs(tStat) >= 1.5) return "border-amber-500/40 bg-amber-500/5";
    return "border-border bg-card";
  }

  function sigTextColor(tStat: number): string {
    if (Math.abs(tStat) >= 2) return "text-green-400";
    if (Math.abs(tStat) >= 1.5) return "text-amber-400";
    return "text-muted-foreground";
  }

  // Heatmap cell color
  function heatColor(val: number): string {
    if (val > 5) return "bg-green-500/40";
    if (val > 2) return "bg-green-500/25";
    if (val > 0) return "bg-green-500/10";
    if (val > -2) return "bg-red-500/10";
    if (val > -5) return "bg-red-500/25";
    return "bg-red-500/40";
  }

  const horizons = ["1d", "5d", "21d", "63d", "126d", "252d"];

  return (
    <div className="space-y-3">
      {/* Section A: Factor Effectiveness Grid */}
      <div>
        <MetricTooltip title="Factor Effectiveness" description="Each factor's predictive power measured by information ratio, t-statistic, and best-performing slice. Green border = statistically significant (t>2), amber = marginal (t>1.5).">
          <h3 className="text-[10px] uppercase tracking-widest text-muted-foreground mb-2 cursor-help">Factor Effectiveness Grid</h3>
        </MetricTooltip>
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-6 gap-2">
          {factors.map((f: any, i: number) => {
            const name = f.factor_name || f.factorName || f.name || `Factor ${i}`;
            const ir = f.best_ir ?? f.informationRatio ?? f.information_ratio ?? 0;
            const tStat = f.best_t_stat_63d ?? f.tStat ?? f.t_stat ?? 0;
            const sampleSize = f.avg_sample_size ?? f.sampleSize ?? f.sample_size ?? f.n ?? 0;
            const bestSlice = f.best_slice_63d ?? f.bestSlice ?? f.best_slice ?? "";
            const bestReturn = f.best_return_63d_pct ?? f.bestReturn ?? f.best_return ?? 0;
            const trend = f.trend ?? (ir > 0.3 ? "up" : ir < -0.1 ? "down" : "flat");
            return (
              <div key={i} className={`border rounded-md p-2.5 ${sigColor(tStat)} cursor-pointer hover:border-primary/40 transition-colors`}
                onClick={() => setSelectedFactor(name)} data-testid={`factor-card-${i}`}>
                <div className="flex items-center justify-between mb-1.5">
                  <span className="text-[10px] font-bold text-foreground uppercase tracking-wider truncate">{name}</span>
                  {trend === "up" ? <TrendingUp className="w-3 h-3 text-green-400" /> :
                   trend === "down" ? <TrendingDown className="w-3 h-3 text-red-400" /> :
                   <Minus className="w-3 h-3 text-muted-foreground" />}
                </div>
                <div className="space-y-1">
                  <div className="flex justify-between text-[10px]">
                    <MetricTooltip title="Information Ratio" description="Risk-adjusted predictive power. IR = mean excess return / std of excess return. Higher is better.">
                      <span className="text-muted-foreground cursor-help">IR</span>
                    </MetricTooltip>
                    <span className={`font-bold ${sigTextColor(tStat)}`} style={{ fontVariantNumeric: "tabular-nums" }}>{ir.toFixed(3)}</span>
                  </div>
                  <div className="flex justify-between text-[10px]">
                    <MetricTooltip title="T-Statistic" description="Statistical significance of the factor's predictive power. |t| > 2.0 is significant at 95% confidence. |t| > 1.5 is marginal.">
                      <span className="text-muted-foreground cursor-help">t-stat</span>
                    </MetricTooltip>
                    <span className={`font-medium ${sigTextColor(tStat)}`} style={{ fontVariantNumeric: "tabular-nums" }}>{tStat.toFixed(2)}</span>
                  </div>
                  <div className="flex justify-between text-[10px]">
                    <span className="text-muted-foreground">n</span>
                    <span className="text-foreground" style={{ fontVariantNumeric: "tabular-nums" }}>{formatNumber(sampleSize)}</span>
                  </div>
                  {bestSlice && (
                    <div className="text-[9px] text-muted-foreground border-t border-border/50 pt-1 mt-1">
                      Best: <span className="text-foreground">{bestSlice}</span>
                      {bestReturn != null && <span className={`ml-1 ${pnlColor(bestReturn)}`}>{formatPct(bestReturn)}</span>}
                    </div>
                  )}
                </div>
              </div>
            );
          })}
          {factors.length === 0 && (
            <div className="col-span-full">
              <EmptyState message="Factor analysis not yet computed — run the enrichment pipeline" icon={FlaskConical} />
            </div>
          )}
        </div>
      </div>

      {/* Section B: Factor Heatmap + Section C: Alpha Decay */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">
        {/* Heatmap */}
        <div className="bg-card border border-border rounded-md p-3">
          <div className="flex items-center justify-between mb-2">
            <MetricTooltip title="Factor Heatmap" description="Excess returns by factor slice and time horizon. Darker green = higher positive excess return. Darker red = negative. Shows which slices of each factor predict returns at which horizons.">
              <h3 className="text-[10px] uppercase tracking-widest text-muted-foreground cursor-help">Factor Heatmap</h3>
            </MetricTooltip>
            <select
              className="bg-background border border-border rounded px-2 py-1 text-[10px] text-foreground outline-none focus:ring-1 focus:ring-primary"
              value={effectiveSelectedFactor}
              onChange={(e) => setSelectedFactor(e.target.value)}
              data-testid="factor-select"
            >
              {factors.map((f: any, i: number) => {
                const name = f.factor_name || f.factorName || f.name || `Factor ${i}`;
                return <option key={i} value={name}>{name}</option>;
              })}
              {factors.length === 0 && <option value="">No factors</option>}
            </select>
          </div>
          <div className="overflow-auto" style={{ overscrollBehavior: "contain" }}>
            {heatmapData.length > 0 ? (() => {
              // Pivot raw rows (sliceName, horizon, meanExcessReturn, sampleSize) into table format
              const horizonNums = [1, 5, 21, 63, 126, 252];
              const sliceMap = new Map<string, Record<string, { ret: number; n: number }>>(); 
              heatmapData.forEach((row: any) => {
                const slice = row.sliceName || row.slice_name || "unknown";
                const h = row.horizon ?? row.trading_days ?? 0;
                if (!sliceMap.has(slice)) sliceMap.set(slice, {});
                sliceMap.get(slice)![String(h)] = {
                  ret: (row.meanExcessReturn ?? row.mean_excess_return ?? 0) * 100,
                  n: row.sampleSize ?? row.sample_size ?? 0
                };
              });
              const slices = Array.from(sliceMap.keys());
              return (
                <table className="w-full text-[10px]">
                  <thead>
                    <tr className="border-b border-border text-muted-foreground">
                      <th className="text-left py-1 px-1.5 font-medium">SLICE</th>
                      {horizonNums.map(h => <th key={h} className="text-center py-1 px-1.5 font-medium">{h}D</th>)}
                    </tr>
                  </thead>
                  <tbody>
                    {slices.map((slice, i) => (
                      <tr key={i} className="border-b border-border/30">
                        <td className="py-1 px-1.5 text-foreground font-medium whitespace-nowrap">{slice}</td>
                        {horizonNums.map(h => {
                          const cell = sliceMap.get(slice)?.[String(h)];
                          return (
                            <td key={h} className={`py-1 px-1.5 text-center ${cell ? heatColor(cell.ret) : ""}`}>
                              {cell ? (
                                <div>
                                  <span className={pnlColor(cell.ret)} style={{ fontVariantNumeric: "tabular-nums" }}>{formatPct(cell.ret, 1)}</span>
                                  <div className="text-[8px] text-muted-foreground">n={cell.n}</div>
                                </div>
                              ) : "—"}
                            </td>
                          );
                        })}
                      </tr>
                    ))}
                  </tbody>
                </table>
              );
            })() : (
              <EmptyState message={heatmapLoading ? "Loading heatmap data..." : effectiveSelectedFactor ? "No heatmap data available for this factor" : "Select a factor to view heatmap"} icon={Layers} />
            )}
          </div>
        </div>

        {/* Alpha Decay Curve */}
        <div className="bg-card border border-border rounded-md p-3">
          <MetricTooltip title="Alpha Decay Curve" description="Average cumulative excess return over time across all enriched signals. Shows how quickly the insider signal alpha dissipates. Peak typically occurs at 40-80 trading days.">
            <h3 className="text-[10px] uppercase tracking-widest text-muted-foreground mb-2 cursor-help">Alpha Decay Curve</h3>
          </MetricTooltip>
          <div className="h-52">
            {alphaDecay && alphaDecay.length > 0 ? (
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={alphaDecay}>
                  <CartesianGrid strokeDasharray="3 3" stroke="hsl(220, 10%, 14%)" />
                  <XAxis dataKey="trading_day" tick={{ fontSize: 9, fill: "hsl(210, 6%, 50%)" }}
                    label={{ value: "Trading Days", position: "insideBottom", offset: -2, fontSize: 9, fill: "hsl(210, 6%, 50%)" }}
                    stroke="hsl(220, 10%, 14%)" />
                  <YAxis tick={{ fontSize: 9, fill: "hsl(210, 6%, 50%)" }}
                    tickFormatter={(v) => `${v}%`} stroke="hsl(220, 10%, 14%)" width={40} />
                  <RechartsTooltip content={({ active, payload, label }: any) => {
                    if (!active || !payload?.length) return null;
                    return (
                      <div className="bg-popover border border-border rounded p-2 text-[11px]">
                        <div className="text-muted-foreground">Day {label}</div>
                        <div className={pnlColor(payload[0].value)}>Cumulative Excess: {formatPct(payload[0].value)}</div>
                        {payload[0]?.payload?.sample_size && <div className="text-muted-foreground">n={payload[0].payload.sample_size}</div>}
                      </div>
                    );
                  }} />
                  <ReferenceLine y={0} stroke="hsl(210, 6%, 30%)" strokeDasharray="3 3" />
                  <Line type="monotone" dataKey="avg_excess_pct" stroke="hsl(37, 90%, 55%)" strokeWidth={2} dot={false} name="Excess Return" />
                </LineChart>
              </ResponsiveContainer>
            ) : (
              <EmptyState message={alphaDecayLoading ? "Loading alpha decay data (this may take a moment)..." : "Alpha decay data not yet computed"} icon={TrendingDown} />
            )}
          </div>
        </div>
      </div>

      {/* Section D: Model Weights */}
      <div className="bg-card border border-border rounded-md p-3">
        <MetricTooltip title="Model Weights" description="Current effective weight per factor in the composite scoring model. Weights are derived empirically from forward-return data, not from academic assumptions. Higher weight = stronger predictive signal historically.">
          <h3 className="text-[10px] uppercase tracking-widest text-muted-foreground mb-2 cursor-help">Model Weights — Current Factor Allocation</h3>
        </MetricTooltip>
        {modelWeights.length > 0 ? (
          <div style={{ height: Math.max(200, modelWeights.length * 28 + 40) }}>
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={modelWeights} layout="vertical" margin={{ left: 120, right: 30, top: 5, bottom: 5 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="hsl(220, 10%, 14%)" horizontal={false} />
                <XAxis type="number" tick={{ fontSize: 9, fill: "hsl(210, 6%, 50%)" }}
                  tickFormatter={(v) => `${(v * 100).toFixed(0)}%`} stroke="hsl(220, 10%, 14%)" domain={[0, 'auto']} />
                <YAxis type="category" dataKey={modelWeights[0]?.factorName ? "factorName" : modelWeights[0]?.factor_name ? "factor_name" : "name"}
                  tick={{ fontSize: 9, fill: "hsl(210, 6%, 50%)" }} stroke="hsl(220, 10%, 14%)" width={95} />
                <RechartsTooltip content={({ active, payload }: any) => {
                  if (!active || !payload?.length) return null;
                  const d = payload[0].payload;
                  const w = d.weight ?? d.effectiveWeight ?? d.effective_weight ?? 0;
                  const conf = d.confidence ?? d.confidenceLevel ?? d.confidence_level ?? null;
                  return (
                    <div className="bg-popover border border-border rounded p-2 text-[11px]">
                      <div className="text-foreground font-bold">{d.factorName || d.factor_name || d.name}</div>
                      <div className="text-muted-foreground">Weight: {(w * 100).toFixed(1)}%</div>
                      {conf != null && <div className="text-muted-foreground">Confidence: {conf}</div>}
                    </div>
                  );
                }} />
                <Bar dataKey={modelWeights[0]?.weight != null ? "weight" : modelWeights[0]?.effectiveWeight != null ? "effectiveWeight" : "effective_weight"}
                  fill="hsl(37, 90%, 55%)" radius={[0, 3, 3, 0]} barSize={16}>
                  {modelWeights.map((_: any, idx: number) => (
                    <Cell key={idx} fill={idx % 2 === 0 ? "hsl(37, 90%, 55%)" : "hsl(37, 80%, 45%)"} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        ) : (
          <EmptyState message={modelWeightsLoading ? "Loading model weights..." : "Model weights not yet computed — run factor analysis pipeline"} icon={Scale} />
        )}
      </div>
    </div>
  );
}


// ====================================================================
// TAB 3: PORTFOLIO
// ====================================================================
function PortfolioTab() {
  const { data: portfolioData, isLoading: portfolioLoading } = useQuery<any>({
    queryKey: ["/api/portfolio/positions"],
    queryFn: async () => {
      const res = await apiRequest("GET", "/api/portfolio/positions");
      return res.json();
    },
    refetchInterval: 30000,
    staleTime: 30000,
    retry: 3,
    retryDelay: 2000,
  });

  const { data: schwabPositions } = useQuery<any[]>({
    queryKey: ["/api/schwab/positions"],
    queryFn: async () => {
      const res = await apiRequest("GET", "/api/schwab/positions");
      return res.json();
    },
    refetchInterval: 60000,
    retry: false,
  });

  const { data: schwabStatus } = useQuery<any>({
    queryKey: ["/api/schwab/status"],
  });

  const { data: closedTrades } = useQuery<any[]>({
    queryKey: ["/api/portfolio/closed-trades"],
    queryFn: async () => {
      const res = await apiRequest("GET", "/api/portfolio/closed-trades?limit=50");
      return res.json();
    },
    refetchInterval: 60000,
  });

  const { data: executions } = useQuery<any[]>({
    queryKey: ["/api/portfolio/executions"],
    queryFn: async () => {
      const res = await apiRequest("GET", "/api/portfolio/executions?limit=50");
      return res.json();
    },
    refetchInterval: 60000,
  });

  const positions = portfolioData?.positions || [];
  const summary = portfolioData?.summary || {};

  const signalAlignedPct = positions.length > 0
    ? (positions.filter((p: any) => p.signalTag?.includes("Aligned") || p.signalScoreAtEntry != null).length / positions.length * 100)
    : 0;
  const winRate = closedTrades && closedTrades.length > 0
    ? closedTrades.filter((t: any) => t.realizedPnl > 0).length / closedTrades.length * 100
    : 0;
  const totalRealizedPnl = closedTrades ? closedTrades.reduce((s: number, t: any) => s + (t.realizedPnl || 0), 0) : 0;
  const winners = closedTrades ? closedTrades.filter((t: any) => t.realizedPnl > 0) : [];
  const losers = closedTrades ? closedTrades.filter((t: any) => t.realizedPnl < 0) : [];
  const grossProfits = winners.reduce((s: number, t: any) => s + t.realizedPnl, 0);
  const grossLosses = Math.abs(losers.reduce((s: number, t: any) => s + t.realizedPnl, 0));
  const profitFactor = grossLosses > 0 ? grossProfits / grossLosses : (grossProfits > 0 ? Infinity : 0);
  const avgHoldDays = closedTrades && closedTrades.length > 0
    ? closedTrades.reduce((s: number, t: any) => s + (t.holdingDays || 0), 0) / closedTrades.length
    : 0;

  function healthColor(health: string | undefined): string {
    if (!health) return "";
    if (health === "at_risk") return "text-red-400";
    if (health === "underperforming") return "text-orange-400";
    if (health === "monitor") return "text-yellow-400";
    if (health === "past_optimal_hold") return "text-loss";
    if (health === "approaching_exit") return "text-amber-400";
    return "text-green-400";
  }

  function healthLabel(health: string | undefined): string {
    if (!health) return "";
    if (health === "at_risk") return "AT RISK";
    if (health === "underperforming") return "UNDERPERFORM";
    if (health === "monitor") return "MONITOR";
    if (health === "past_optimal_hold") return "PAST OPT";
    if (health === "approaching_exit") return "APPROACH EXIT";
    return "ON TRACK";
  }

  return (
    <div className="space-y-3">
      {/* Portfolio KPIs */}
      <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-8 gap-2">
        <KPICard label="Portfolio Value" value={summary.totalValue ? formatCurrency(summary.totalValue) : portfolioLoading ? "..." : "—"}
          icon={Briefcase} tooltip="Total Market Value" tooltipFormula="SUM(quantity × current_price)"
          tooltipDesc="Current market value of all open positions." />
        <KPICard label="Unrealized P&L" value={summary.totalPnl != null ? formatCurrency(summary.totalPnl) : portfolioLoading ? "..." : "—"}
          subtitle={summary.totalPnlPct != null ? formatPct(summary.totalPnlPct) : undefined}
          icon={summary.totalPnl >= 0 ? TrendingUp : TrendingDown} tooltip="Unrealized Profit/Loss"
          tooltipFormula="SUM(market_value - cost_basis)" tooltipDesc="Total unrealized gain/loss across all open positions."
          valueClass={summary.totalPnl >= 0 ? "text-gain" : "text-loss"} />
        <KPICard label="Realized P&L" value={closedTrades && closedTrades.length > 0 ? formatCurrency(totalRealizedPnl) : "—"}
          icon={DollarSign} tooltip="Total Realized P&L" tooltipFormula="SUM(closed_trade_pnl)"
          tooltipDesc="Total profit/loss from all completed round-trip trades."
          valueClass={totalRealizedPnl >= 0 ? "text-gain" : "text-loss"} />
        <KPICard label="Day Change" value={summary.totalDayChange != null ? formatCurrency(summary.totalDayChange) : "—"}
          subtitle={summary.totalDayChangePct != null ? formatPct(summary.totalDayChangePct) : undefined}
          icon={Activity} tooltip="Today's Change" tooltipDesc="Net change in portfolio value today."
          valueClass={(summary.totalDayChange || 0) >= 0 ? "text-gain" : "text-loss"} />
        <KPICard label="Win Rate" value={closedTrades && closedTrades.length > 0 ? formatPct(winRate, 0) : "—"}
          subtitle={closedTrades && closedTrades.length > 0 ? `${winners.length}W/${losers.length}L` : undefined}
          icon={Award} tooltip="Win Rate" tooltipFormula="winning_trades / total_trades × 100"
          tooltipDesc="Percentage of closed trades that were profitable."
          valueClass={winRate >= 50 ? "text-gain" : "text-loss"} />
        <KPICard label="Profit Factor" value={closedTrades && closedTrades.length > 0 ? (profitFactor === Infinity ? "∞" : formatRatio(profitFactor)) : "—"}
          icon={Scale} tooltip="Profit Factor" tooltipFormula="gross_profits / gross_losses"
          tooltipDesc="Ratio of total gains to total losses. Above 1.5 is good, above 2.0 is excellent."
          valueClass={profitFactor >= 1.5 ? "text-gain" : profitFactor >= 1.0 ? "text-foreground" : "text-loss"} />
        <KPICard label="Avg Hold" value={closedTrades && closedTrades.length > 0 ? `${avgHoldDays.toFixed(0)}d` : "—"}
          icon={Timer} tooltip="Average Holding Period" tooltipFormula="AVG(exit_date - entry_date)"
          tooltipDesc="Average calendar days positions are held." />
        <KPICard label="Positions" value={summary.positionCount?.toString() || positions.length.toString() || "0"} icon={BarChart3}
          tooltip="Open Positions" tooltipDesc="Number of distinct tickers currently held." />
      </div>

      {/* Positions Table */}
      <div className="bg-card border border-border rounded-md p-3">
        <MetricTooltip title="Open Positions" description="Current portfolio holdings with real-time P&L, signal attribution, and holding health status.">
          <h3 className="text-[10px] uppercase tracking-widest text-muted-foreground mb-2 cursor-help">Open Positions</h3>
        </MetricTooltip>
        <div className="overflow-auto" style={{ overscrollBehavior: "contain" }}>
          <table className="w-full text-[11px]" data-testid="positions-table">
            <thead className="sticky top-0 bg-card z-10">
              <tr className="border-b border-border text-muted-foreground">
                <th className="text-left py-1.5 px-2 font-medium">TICKER</th>
                <th className="text-left py-1.5 px-2 font-medium">NAME</th>
                <th className="text-right py-1.5 px-2 font-medium">QTY</th>
                <th className="text-right py-1.5 px-2 font-medium">
                  <MetricTooltip title="Average Cost Basis" description="Volume-weighted average purchase price."><span className="cursor-help">AVG COST</span></MetricTooltip>
                </th>
                <th className="text-right py-1.5 px-2 font-medium">CURRENT</th>
                <th className="text-right py-1.5 px-2 font-medium">
                  <MetricTooltip title="Unrealized P&L" description="Profit or loss on open position." formula="(current - avg_cost) × quantity"><span className="cursor-help">P&L</span></MetricTooltip>
                </th>
                <th className="text-right py-1.5 px-2 font-medium">P&L %</th>
                <th className="text-right py-1.5 px-2 font-medium">DAY CHG</th>
                <th className="text-center py-1.5 px-2 font-medium">
                  <MetricTooltip title="Signal Tag" description="Shows whether this position aligns with an insider signal, is independent (no signal), or contrarian (entered against signal direction)."><span className="cursor-help">SIGNAL</span></MetricTooltip>
                </th>
                <th className="text-center py-1.5 px-2 font-medium">
                  <MetricTooltip title="Signal Health" description="Holding period status relative to the signal's optimal exit window. 'On Track' = within expected hold. 'Past Optimal' = held too long relative to alpha decay curve."><span className="cursor-help">HEALTH</span></MetricTooltip>
                </th>
              </tr>
            </thead>
            <tbody>
              {positions.map((pos: any) => {
                const tag = pos.signalTag || (pos.signalScoreAtEntry != null ? `Aligned-${pos.signalScoreAtEntry}` : "Independent");
                return (
                  <tr key={pos.ticker} className="terminal-row border-b border-border/30">
                    <td className="py-1.5 px-2 font-bold text-primary">{pos.ticker}</td>
                    <td className="py-1.5 px-2 text-foreground truncate max-w-[120px]">{pos.companyName}</td>
                    <td className="py-1.5 px-2 text-right" style={{ fontVariantNumeric: "tabular-nums" }}>{formatNumber(pos.quantity)}</td>
                    <td className="py-1.5 px-2 text-right" style={{ fontVariantNumeric: "tabular-nums" }}>${pos.avgCostBasis?.toFixed(2)}</td>
                    <td className="py-1.5 px-2 text-right" style={{ fontVariantNumeric: "tabular-nums" }}>${pos.currentPrice?.toFixed(2)}</td>
                    <td className={`py-1.5 px-2 text-right font-medium ${pnlColor(pos.unrealizedPnl || 0)}`} style={{ fontVariantNumeric: "tabular-nums" }}>{formatCurrency(pos.unrealizedPnl || 0)}</td>
                    <td className={`py-1.5 px-2 text-right ${pnlColor(pos.unrealizedPnlPct || 0)}`} style={{ fontVariantNumeric: "tabular-nums" }}>{formatPct(pos.unrealizedPnlPct || 0)}</td>
                    <td className={`py-1.5 px-2 text-right ${pnlColor(pos.dayChangePct || 0)}`} style={{ fontVariantNumeric: "tabular-nums" }}>{formatPct(pos.dayChangePct || 0)}</td>
                    <td className="py-1.5 px-2 text-center">
                      <span className={`text-[9px] px-1.5 py-0.5 rounded font-bold ${tag.includes("Aligned") ? "bg-amber-500/15 text-amber-400 border border-amber-500/30" : tag === "Contra" ? "bg-red-500/15 text-red-400 border border-red-500/30" : "bg-gray-500/15 text-gray-400 border border-gray-500/30"}`}>
                        {tag}
                      </span>
                    </td>
                    <td className="py-1.5 px-2 text-center">
                      <span className={`text-[9px] font-bold ${healthColor(pos.signalHealth)}`}>{healthLabel(pos.signalHealth)}</span>
                    </td>
                  </tr>
                );
              })}
              {positions.length === 0 && (
                <tr><td colSpan={10} className="text-center py-6 text-muted-foreground">
                  {portfolioLoading ? <span className="flex items-center justify-center gap-2"><Activity className="w-4 h-4 animate-pulse" /> Loading positions...</span> : "No open positions — connect Schwab to start tracking"}
                </td></tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* Schwab Live Positions */}
      {schwabStatus?.isConnected && schwabPositions && schwabPositions.length > 0 && (
        <div className="bg-card border border-border rounded-md p-3">
          <h3 className="text-[10px] uppercase tracking-widest text-muted-foreground mb-2">
            Schwab Live Positions
            <span className="ml-2 text-[9px] text-green-400">LIVE</span>
          </h3>
          <div className="overflow-auto" style={{ overscrollBehavior: "contain" }}>
            <table className="w-full text-[11px]" data-testid="schwab-positions-table">
              <thead className="sticky top-0 bg-card z-10">
                <tr className="border-b border-border text-muted-foreground">
                  <th className="text-left py-1.5 px-2 font-medium">TICKER</th>
                  <th className="text-left py-1.5 px-2 font-medium">DESCRIPTION</th>
                  <th className="text-left py-1.5 px-2 font-medium">TYPE</th>
                  <th className="text-right py-1.5 px-2 font-medium">QTY</th>
                  <th className="text-right py-1.5 px-2 font-medium">AVG PRICE</th>
                  <th className="text-right py-1.5 px-2 font-medium">MKT VALUE</th>
                  <th className="text-right py-1.5 px-2 font-medium">DAY P&L</th>
                  <th className="text-right py-1.5 px-2 font-medium">DAY %</th>
                </tr>
              </thead>
              <tbody>
                {schwabPositions.map((pos: any, i: number) => (
                  <tr key={i} className="terminal-row border-b border-border/30">
                    <td className="py-1.5 px-2 font-bold text-primary">{pos.ticker}</td>
                    <td className="py-1.5 px-2 text-foreground truncate max-w-[160px]">{pos.description}</td>
                    <td className="py-1.5 px-2 text-muted-foreground text-[10px]">{pos.assetType}</td>
                    <td className="py-1.5 px-2 text-right" style={{ fontVariantNumeric: "tabular-nums" }}>{formatNumber(pos.quantity)}</td>
                    <td className="py-1.5 px-2 text-right" style={{ fontVariantNumeric: "tabular-nums" }}>${pos.averagePrice?.toFixed(2)}</td>
                    <td className="py-1.5 px-2 text-right font-medium" style={{ fontVariantNumeric: "tabular-nums" }}>{formatCurrency(pos.marketValue)}</td>
                    <td className={`py-1.5 px-2 text-right font-medium ${pnlColor(pos.currentDayPnl)}`} style={{ fontVariantNumeric: "tabular-nums" }}>{formatCurrency(pos.currentDayPnl)}</td>
                    <td className={`py-1.5 px-2 text-right ${pnlColor(pos.currentDayPnlPct)}`} style={{ fontVariantNumeric: "tabular-nums" }}>{formatPct(pos.currentDayPnlPct)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Closed Trades */}
      <div className="bg-card border border-border rounded-md p-3">
        <MetricTooltip title="Closed Trades" description="Completed round-trip trades with realized P&L and signal attribution.">
          <h3 className="text-[10px] uppercase tracking-widest text-muted-foreground mb-2 cursor-help">Closed Trades — P&L History</h3>
        </MetricTooltip>
        <div className="overflow-auto max-h-48" style={{ overscrollBehavior: "contain" }}>
          <table className="w-full text-[11px]" data-testid="closed-trades-table">
            <thead className="sticky top-0 bg-card z-10">
              <tr className="border-b border-border text-muted-foreground">
                <th className="text-left py-1.5 px-2 font-medium">TICKER</th>
                <th className="text-left py-1.5 px-2 font-medium">ENTRY</th>
                <th className="text-left py-1.5 px-2 font-medium">EXIT</th>
                <th className="text-right py-1.5 px-2 font-medium">ENTRY $</th>
                <th className="text-right py-1.5 px-2 font-medium">EXIT $</th>
                <th className="text-right py-1.5 px-2 font-medium">QTY</th>
                <th className="text-right py-1.5 px-2 font-medium">P&L</th>
                <th className="text-right py-1.5 px-2 font-medium">RETURN</th>
                <th className="text-center py-1.5 px-2 font-medium">SIGNAL</th>
                <th className="text-right py-1.5 px-2 font-medium">DAYS</th>
              </tr>
            </thead>
            <tbody>
              {(closedTrades || []).map((trade: any, i: number) => (
                <tr key={i} className="terminal-row border-b border-border/30">
                  <td className="py-1 px-2 font-bold text-primary">{trade.ticker}</td>
                  <td className="py-1 px-2 text-muted-foreground">{trade.entryDate}</td>
                  <td className="py-1 px-2 text-muted-foreground">{trade.exitDate}</td>
                  <td className="py-1 px-2 text-right" style={{ fontVariantNumeric: "tabular-nums" }}>${trade.entryPrice?.toFixed(2)}</td>
                  <td className="py-1 px-2 text-right" style={{ fontVariantNumeric: "tabular-nums" }}>${trade.exitPrice?.toFixed(2)}</td>
                  <td className="py-1 px-2 text-right" style={{ fontVariantNumeric: "tabular-nums" }}>{formatNumber(trade.quantity)}</td>
                  <td className={`py-1 px-2 text-right font-medium ${pnlColor(trade.realizedPnl)}`} style={{ fontVariantNumeric: "tabular-nums" }}>{formatCurrency(trade.realizedPnl)}</td>
                  <td className={`py-1 px-2 text-right font-medium ${pnlColor(trade.realizedPnlPct)}`} style={{ fontVariantNumeric: "tabular-nums" }}>{formatPct(trade.realizedPnlPct)}</td>
                  <td className="py-1 px-2 text-center">
                    {trade.signalScoreAtEntry != null && (
                      <span className={`text-[10px] px-1.5 py-0.5 rounded font-bold ${getSignalClass(trade.signalScoreAtEntry)}`}>{trade.signalScoreAtEntry}</span>
                    )}
                  </td>
                  <td className="py-1 px-2 text-right text-muted-foreground" style={{ fontVariantNumeric: "tabular-nums" }}>{trade.holdingDays || "—"}</td>
                </tr>
              ))}
              {(!closedTrades || closedTrades.length === 0) && (
                <tr><td colSpan={10} className="text-center py-6 text-muted-foreground">
                  {!closedTrades ? <span className="flex items-center justify-center gap-2"><Activity className="w-4 h-4 animate-pulse" /> Loading closed trades...</span> : "No closed trades yet — run trade matching to compute P&L"}
                </td></tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* Execution Log */}
      <div className="bg-card border border-border rounded-md p-3">
        <MetricTooltip title="Execution Log" description="All buy and sell executions with signal attribution.">
          <h3 className="text-[10px] uppercase tracking-widest text-muted-foreground mb-2 cursor-help">Execution Log</h3>
        </MetricTooltip>
        <div className="overflow-auto max-h-40" style={{ overscrollBehavior: "contain" }}>
          <table className="w-full text-[11px]" data-testid="executions-table">
            <thead className="sticky top-0 bg-card z-10">
              <tr className="border-b border-border text-muted-foreground">
                <th className="text-left py-1.5 px-2 font-medium">DATE</th>
                <th className="text-left py-1.5 px-2 font-medium">SIDE</th>
                <th className="text-left py-1.5 px-2 font-medium">TICKER</th>
                <th className="text-right py-1.5 px-2 font-medium">QTY</th>
                <th className="text-right py-1.5 px-2 font-medium">AVG PRICE</th>
                <th className="text-right py-1.5 px-2 font-medium">TOTAL</th>
                <th className="text-center py-1.5 px-2 font-medium">SIGNAL</th>
                <th className="text-left py-1.5 px-2 font-medium">SOURCE</th>
              </tr>
            </thead>
            <tbody>
              {(executions || []).map((exec: any, i: number) => (
                <tr key={i} className="terminal-row border-b border-border/30">
                  <td className="py-1 px-2 text-muted-foreground">{exec.executionDate}</td>
                  <td className="py-1 px-2">
                    <span className={`text-[10px] px-1.5 py-0.5 rounded font-bold ${exec.side === "BUY" ? "bg-gain text-[hsl(142,55%,15%)]" : "bg-loss text-[hsl(0,72%,15%)]"}`}>
                      {exec.side}
                    </span>
                  </td>
                  <td className="py-1 px-2 font-bold text-primary">{exec.ticker}</td>
                  <td className="py-1 px-2 text-right" style={{ fontVariantNumeric: "tabular-nums" }}>{formatNumber(exec.quantity)}</td>
                  <td className="py-1 px-2 text-right" style={{ fontVariantNumeric: "tabular-nums" }}>${exec.avgPrice?.toFixed(2)}</td>
                  <td className="py-1 px-2 text-right font-medium" style={{ fontVariantNumeric: "tabular-nums" }}>{formatCurrency(exec.totalCost)}</td>
                  <td className="py-1 px-2 text-center">
                    {exec.signalScore != null ? (
                      <span className={`text-[10px] px-1.5 py-0.5 rounded font-bold ${getSignalClass(exec.signalScore)}`}>{exec.signalScore}</span>
                    ) : <span className="text-muted-foreground">—</span>}
                  </td>
                  <td className="py-1 px-2 text-muted-foreground text-[10px]">{exec.source}</td>
                </tr>
              ))}
              {(!executions || executions.length === 0) && (
                <tr><td colSpan={8} className="text-center py-6 text-muted-foreground">No executions yet — connect Schwab to start tracking</td></tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}


// ====================================================================
// TAB 4: PERFORMANCE
// ====================================================================
function PerformanceTab() {
  const { data: summary, isLoading: summaryLoading } = useQuery<any>({
    queryKey: ["/api/performance/summary"],
    queryFn: async () => {
      const res = await apiRequest("GET", "/api/performance/summary");
      return res.json();
    },
    refetchInterval: 60000,
    staleTime: 60000,
    retry: 3,
  });

  const { data: equityCurve, isLoading: curveLoading } = useQuery<any[]>({
    queryKey: ["/api/performance/equity-curve"],
    queryFn: async () => {
      const res = await apiRequest("GET", "/api/performance/equity-curve");
      return res.json();
    },
    refetchInterval: 60000,
    staleTime: 60000,
    retry: 3,
  });

  const { data: schwabPositions } = useQuery<any[]>({
    queryKey: ["/api/schwab/positions"],
    queryFn: async () => {
      const res = await apiRequest("GET", "/api/schwab/positions");
      return res.json();
    },
    refetchInterval: 120000,
    retry: false,
  });

  const { data: schwabStatus } = useQuery<any>({
    queryKey: ["/api/schwab/status"],
  });

  const m = summary || {};
  const schwabTotalValue = schwabPositions?.reduce((sum: number, p: any) => sum + (p.marketValue || 0), 0) || 0;
  const schwabDayPnl = schwabPositions?.reduce((sum: number, p: any) => sum + (p.currentDayPnl || 0), 0) || 0;

  return (
    <div className="space-y-3">
      {/* KPI Row — Trade-based analytics */}
      <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-7 gap-2">
        <KPICard label="Realized P&L" value={m.totalRealizedPnl != null ? formatCurrency(m.totalRealizedPnl) : summaryLoading ? "..." : "—"}
          icon={DollarSign} tooltip="Total Realized P&L" tooltipFormula="SUM(closed_trade_pnl)"
          tooltipDesc="Total profit/loss from all closed round-trip trades."
          valueClass={m.totalRealizedPnl >= 0 ? "text-gain" : "text-loss"} />
        <KPICard label="Unrealized P&L" value={m.totalUnrealizedPnl != null ? formatCurrency(m.totalUnrealizedPnl) : summaryLoading ? "..." : "—"}
          icon={TrendingUp} tooltip="Total Unrealized P&L" tooltipFormula="SUM(position_pnl)"
          tooltipDesc="Total unrealized gain/loss on open positions."
          valueClass={(m.totalUnrealizedPnl || 0) >= 0 ? "text-gain" : "text-loss"} />
        <KPICard label="Combined P&L" value={m.combinedPnl != null ? formatCurrency(m.combinedPnl) : summaryLoading ? "..." : "—"}
          icon={Briefcase} tooltip="Combined P&L" tooltipFormula="realized + unrealized"
          tooltipDesc="Total realized + unrealized P&L across all trades."
          valueClass={(m.combinedPnl || 0) >= 0 ? "text-gain" : "text-loss"} />
        <KPICard label="Win Rate" value={m.winRate != null ? formatPct(m.winRate, 0) : summaryLoading ? "..." : "—"}
          subtitle={m.winCount != null ? `${m.winCount}W / ${m.lossCount}L` : undefined}
          icon={Award} tooltip="Win Rate" tooltipFormula="winning_trades / total_closed × 100"
          tooltipDesc="Percentage of closed trades that were profitable."
          valueClass={m.winRate >= 50 ? "text-gain" : "text-loss"} />
        <KPICard label="Profit Factor" value={m.profitFactor != null ? (m.profitFactor === Infinity ? "∞" : formatRatio(m.profitFactor)) : summaryLoading ? "..." : "—"}
          icon={Scale} tooltip="Profit Factor" tooltipFormula="gross_profits / gross_losses"
          tooltipDesc="Ratio of total gains to total losses. Above 1.5 is good, above 2.0 is excellent."
          valueClass={m.profitFactor >= 1.5 ? "text-gain" : m.profitFactor >= 1.0 ? "text-foreground" : "text-loss"} />
        <KPICard label="Avg Hold" value={m.avgHoldingPeriod != null ? `${m.avgHoldingPeriod.toFixed(0)}d` : summaryLoading ? "..." : "—"}
          icon={Timer} tooltip="Average Holding Period" tooltipFormula="AVG(exit_date - entry_date)"
          tooltipDesc="Average number of calendar days positions are held before closing." />
        <KPICard label="Closed Trades" value={m.closedTradeCount != null ? m.closedTradeCount.toString() : summaryLoading ? "..." : "—"}
          subtitle={m.openPositionCount != null ? `${m.openPositionCount} open` : undefined}
          icon={BarChart3} tooltip="Trade Count" tooltipDesc="Total number of completed round-trip trades." />
      </div>

      {/* Trade Statistics */}
      <div className="bg-card border border-border rounded-md p-3">
        <h3 className="text-[10px] uppercase tracking-widest text-muted-foreground mb-3">Trade Statistics</h3>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          {[
            { label: "Avg Win", value: m.avgWinDollar != null ? formatCurrency(m.avgWinDollar) : "—", sub: m.avgWinPct != null ? formatPct(m.avgWinPct) : undefined, desc: "Average profit on winning trades", color: "text-gain" },
            { label: "Avg Loss", value: m.avgLossDollar != null ? formatCurrency(-m.avgLossDollar) : "—", sub: m.avgLossPct != null ? formatPct(m.avgLossPct) : undefined, desc: "Average loss on losing trades", color: "text-loss" },
            { label: "Best Trade", value: m.bestTrade ? `${m.bestTrade.ticker} ${formatCurrency(m.bestTrade.pnl)}` : "—", sub: m.bestTrade ? formatPct(m.bestTrade.pnlPct) : undefined, desc: "Largest single winning trade", color: "text-gain" },
            { label: "Worst Trade", value: m.worstTrade ? `${m.worstTrade.ticker} ${formatCurrency(m.worstTrade.pnl)}` : "—", sub: m.worstTrade ? formatPct(m.worstTrade.pnlPct) : undefined, desc: "Largest single losing trade", color: "text-loss" },
            { label: "Gross Profits", value: m.grossProfits != null ? formatCurrency(m.grossProfits) : "—", desc: "Sum of all winning trade P&L", color: "text-gain" },
            { label: "Gross Losses", value: m.grossLosses != null ? formatCurrency(-m.grossLosses) : "—", desc: "Sum of all losing trade P&L", color: "text-loss" },
            { label: "Expectancy", value: m.expectancy != null ? formatCurrency(m.expectancy) : "—", desc: "Expected dollar value per trade: (winRate × avgWin) - (lossRate × avgLoss)", formula: "E = P(win)×W - P(loss)×L", color: m.expectancy >= 0 ? "text-gain" : "text-loss" },
            { label: "Market Value", value: m.totalMarketValue != null ? formatCurrency(m.totalMarketValue) : "—", desc: "Current total market value of open positions", color: "text-foreground" },
          ].map((row, i) => (
            <MetricTooltip key={i} title={row.label} description={row.desc} formula={(row as any).formula}>
              <div className="bg-background rounded border border-border/50 p-2.5 cursor-help">
                <div className="text-[10px] text-muted-foreground uppercase tracking-wider">{row.label}</div>
                <div className={`text-sm font-bold mt-1 ${row.color || "text-foreground"}`} style={{ fontVariantNumeric: "tabular-nums" }}>{row.value}</div>
                {row.sub && <div className={`text-[10px] mt-0.5 ${row.color || "text-muted-foreground"}`}>{row.sub}</div>}
              </div>
            </MetricTooltip>
          ))}
        </div>
      </div>

      {/* Equity Curve */}
      <div className="bg-card border border-border rounded-md p-3">
        <MetricTooltip title="Equity Curve" description="Cumulative realized P&L over time from closed trades. Each point represents cumulative profit after a trade exit.">
          <h3 className="text-[10px] uppercase tracking-widest text-muted-foreground mb-3 cursor-help">Equity Curve — Cumulative P&L</h3>
        </MetricTooltip>
        <div className="h-56">
          {equityCurve && equityCurve.length > 0 ? (
            <ResponsiveContainer width="100%" height="100%">
              <ComposedChart data={equityCurve}>
                <defs>
                  <linearGradient id="pnlGrad" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="hsl(37, 90%, 55%)" stopOpacity={0.3} />
                    <stop offset="95%" stopColor="hsl(37, 90%, 55%)" stopOpacity={0} />
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="hsl(220, 10%, 14%)" />
                <XAxis dataKey="date" tick={{ fontSize: 9, fill: "hsl(210, 6%, 50%)" }}
                  tickFormatter={(v) => v ? v.slice(5) : ""} stroke="hsl(220, 10%, 14%)" />
                <YAxis tick={{ fontSize: 9, fill: "hsl(210, 6%, 50%)" }}
                  tickFormatter={(v) => `$${(v/1000).toFixed(0)}K`} stroke="hsl(220, 10%, 14%)" width={55} />
                <RechartsTooltip
                  contentStyle={{ background: "hsl(220, 14%, 10%)", border: "1px solid hsl(220, 10%, 20%)", borderRadius: 6, fontSize: 11 }}
                  formatter={(value: any) => [`$${Number(value).toLocaleString("en-US", { maximumFractionDigits: 0 })}`, "Cumulative P&L"]}
                  labelFormatter={(label) => `Exit: ${label}`} />
                <ReferenceLine y={0} stroke="hsl(210, 6%, 30%)" strokeDasharray="3 3" />
                <Area type="monotone" dataKey="cumulativePnl" stroke="hsl(37, 90%, 55%)" fill="url(#pnlGrad)" strokeWidth={2} name="Cumulative P&L" />
              </ComposedChart>
            </ResponsiveContainer>
          ) : (
            <div className="flex items-center justify-center h-full text-muted-foreground text-[11px]">
              {curveLoading ? (
                <div className="flex items-center gap-2"><Activity className="w-4 h-4 animate-pulse" /> Loading equity curve...</div>
              ) : "No closed trades yet — equity curve will appear after trade matching"}
            </div>
          )}
        </div>
      </div>

      {/* Schwab Account Summary */}
      {schwabStatus?.isConnected && schwabPositions && schwabPositions.length > 0 && (
        <div className="bg-card border border-border rounded-md p-3">
          <h3 className="text-[10px] uppercase tracking-widest text-muted-foreground mb-3">
            Schwab Account Summary
            <span className="ml-2 text-[9px] text-green-400">LIVE</span>
          </h3>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            <div className="bg-background rounded border border-border/50 p-2.5">
              <div className="text-[10px] text-muted-foreground uppercase tracking-wider">Portfolio Value</div>
              <div className="text-base font-bold text-foreground mt-1" style={{ fontVariantNumeric: "tabular-nums" }}>{formatCurrency(schwabTotalValue)}</div>
            </div>
            <div className="bg-background rounded border border-border/50 p-2.5">
              <div className="text-[10px] text-muted-foreground uppercase tracking-wider">Day P&L</div>
              <div className={`text-base font-bold mt-1 ${pnlColor(schwabDayPnl)}`} style={{ fontVariantNumeric: "tabular-nums" }}>{formatCurrency(schwabDayPnl)}</div>
            </div>
            <div className="bg-background rounded border border-border/50 p-2.5">
              <div className="text-[10px] text-muted-foreground uppercase tracking-wider">Positions</div>
              <div className="text-base font-bold text-foreground mt-1" style={{ fontVariantNumeric: "tabular-nums" }}>{schwabPositions.length}</div>
            </div>
            <div className="bg-background rounded border border-border/50 p-2.5">
              <div className="text-[10px] text-muted-foreground uppercase tracking-wider">Account</div>
              <div className="text-base font-bold text-foreground mt-1">{schwabStatus.accountNumber || "—"}</div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}


// ====================================================================
// TAB 5: EXECUTION ANALYSIS
// ====================================================================
function ExecutionTab() {
  const { data: execSummary, isLoading: execLoading } = useQuery<any>({
    queryKey: ["/api/execution/summary"],
    queryFn: async () => {
      const res = await apiRequest("GET", "/api/execution/summary");
      return res.json();
    },
    refetchInterval: 60000,
    staleTime: 60000,
    retry: 3,
    retryDelay: 2000,
  });

  const { data: missedSignals, isLoading: missedLoading } = useQuery<any[]>({
    queryKey: ["/api/execution/missed-signals"],
    queryFn: async () => {
      const res = await apiRequest("GET", "/api/execution/missed-signals?days=365&minScore=70");
      return res.json();
    },
    refetchInterval: 120000,
    staleTime: 120000,
    retry: 3,
    retryDelay: 2000,
  });

  const { data: deviations, isLoading: deviationsLoading } = useQuery<any[]>({
    queryKey: ["/api/execution/deviations"],
    queryFn: async () => {
      const res = await apiRequest("GET", "/api/execution/deviations");
      return res.json();
    },
    refetchInterval: 120000,
    staleTime: 120000,
    retry: 3,
    retryDelay: 2000,
  });

  const { data: schwabOrders } = useQuery<any[]>({
    queryKey: ["/api/schwab/orders"],
    queryFn: async () => {
      const res = await apiRequest("GET", "/api/schwab/orders");
      return res.json();
    },
    refetchInterval: 120000,
    retry: false,
  });

  const { data: schwabStatus } = useQuery<any>({
    queryKey: ["/api/schwab/status"],
  });

  const s = execSummary || {};

  return (
    <div className="space-y-3">
      {/* KPI Row — 6 cards */}
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-2">
        <KPICard label="Signal Coverage" value={s.signalCoverageCount || (execLoading ? "..." : `${((s.signalCoverage || 0) * 100).toFixed(0)}%`)}
          subtitle={s.signalCoverage != null ? `${(s.signalCoverage * 100).toFixed(0)}% (target 80%+)` : undefined}
          icon={Target} tooltip="Signal Coverage" tooltipFormula="signals_traded / total_tier1_tier2 × 100"
          tooltipDesc="Percentage of Tier 1 and Tier 2 signals that you actually traded. Target is 80%+ for full strategy capture." />
        <KPICard label="Avg Entry Delay" value={s.avgEntryDelay != null ? `${Number(s.avgEntryDelay).toFixed(1)}d` : execLoading ? "..." : "—"}
          subtitle={s.avgEntryDelay != null && s.avgEntryDelay > 0 ? (s.avgEntryDelay < 1 ? "Good (<1d)" : "Late (>1d)") : undefined}
          icon={Clock} tooltip="Average Entry Delay" tooltipFormula="AVG(entry_date - signal_date)"
          tooltipDesc="Average trading days between signal generation and your actual entry. Target is <1 day — alpha decays rapidly after signal."
          valueClass={s.avgEntryDelay != null && s.avgEntryDelay > 1 ? "text-loss" : ""} />
        <KPICard label="Exit Discipline" value={s.exitDiscipline != null ? `${(s.exitDiscipline * 100).toFixed(0)}%` : "—"}
          icon={Shield} tooltip="Exit Discipline" tooltipFormula="rule_following_exits / total_exits × 100"
          tooltipDesc="Percentage of exits that followed the model's exit rules (time-based or stop-loss). Higher = more disciplined." />
        <KPICard label="Deviation Cost" value={s.totalDeviationCost != null ? formatPct(s.totalDeviationCost * 100) : s.deviationCost != null ? formatPct(s.deviationCost * 100) : "—"}
          icon={AlertTriangle} tooltip="Deviation Cost" tooltipFormula="strategy_return - your_return"
          tooltipDesc="Total return lost due to deviations from model recommendations. Includes missed signals, late entries, wrong sizing."
          valueClass="text-loss" />
        <KPICard label="Indep. Alpha" value={s.independentAlpha != null ? formatPct(s.independentAlpha * 100) : "—"}
          icon={Zap} tooltip="Independent Alpha" tooltipFormula="return_on_non_signal_trades"
          tooltipDesc="Return generated from trades that were NOT based on insider signals. Positive = you add value beyond the model."
          valueClass={s.independentAlpha >= 0 ? "text-gain" : "text-loss"} />
        <KPICard label="Total Trades" value={s.totalTrades != null ? s.totalTrades.toString() : "—"}
          icon={Hash} tooltip="Total Trades" tooltipDesc="Total number of completed round-trip trades across all sources." />
      </div>

      {/* Missed Signals Panel */}
      <div className="bg-card border border-border rounded-md p-3">
        <MetricTooltip title="Missed Signals" description="Tier 1 and Tier 2 signals that you did NOT trade. Shows what you left on the table. Focus on reducing misses for high-score signals.">
          <h3 className="text-[10px] uppercase tracking-widest text-muted-foreground mb-2 cursor-help">Missed Signals — What You Left on the Table (1Y)</h3>
        </MetricTooltip>
        <div className="overflow-auto max-h-48" style={{ overscrollBehavior: "contain" }}>
          {missedSignals && missedSignals.length > 0 ? (
            <table className="w-full text-[11px]" data-testid="missed-signals-table">
              <thead className="sticky top-0 bg-card z-10">
                <tr className="border-b border-border text-muted-foreground">
                  <th className="text-left py-1.5 px-2 font-medium">TICKER</th>
                  <th className="text-center py-1.5 px-2 font-medium">SCORE</th>
                  <th className="text-left py-1.5 px-2 font-medium">DATE</th>
                  <th className="text-right py-1.5 px-2 font-medium">
                    <MetricTooltip title="63-Day Return" description="Actual return 63 trading days after the signal. This is what you missed."><span className="cursor-help">63D RETURN</span></MetricTooltip>
                  </th>
                  <th className="text-right py-1.5 px-2 font-medium">
                    <MetricTooltip title="63-Day Excess Return" description="Return above the S&P 500 benchmark over 63 trading days."><span className="cursor-help">63D EXCESS</span></MetricTooltip>
                  </th>
                </tr>
              </thead>
              <tbody>
                {missedSignals.map((sig: any, i: number) => {
                  const ticker = sig.ticker || sig.issuer_ticker || sig.issuerTicker || "";
                  const score = sig.signalScore || sig.score || sig.signal_score || sig.compositeScore || 0;
                  const date = sig.signalDate || sig.date || sig.signal_date || "";
                  const ret63 = sig.return63dPct ?? sig.return_63d_pct ?? sig.return63d ?? sig.fwdReturn63d ?? null;
                  const excess63 = sig.excess63dPct ?? sig.excess_63d_pct ?? sig.excessReturn63d ?? sig.excess63d ?? sig.estimatedAlphaMissed ?? null;
                  return (
                    <tr key={i} className="terminal-row border-b border-border/30">
                      <td className="py-1 px-2 font-bold text-primary">{ticker}</td>
                      <td className="py-1 px-2 text-center">
                        <span className={`text-[10px] px-1.5 py-0.5 rounded font-bold ${getSignalClass(score)}`}>{score}</span>
                      </td>
                      <td className="py-1 px-2 text-muted-foreground">{date}</td>
                      <td className={`py-1 px-2 text-right font-medium ${pnlColor(ret63 || 0)}`} style={{ fontVariantNumeric: "tabular-nums" }}>
                        {ret63 != null ? formatPct(ret63) : "—"}
                      </td>
                      <td className={`py-1 px-2 text-right font-medium ${pnlColor(excess63 || 0)}`} style={{ fontVariantNumeric: "tabular-nums" }}>
                        {excess63 != null ? formatPct(excess63) : "—"}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          ) : (
            <EmptyState message={missedLoading ? "Loading missed signals..." : "No missed signals found — either all high-score signals were traded, or no signals have score >= 70 in the last 90 days"} icon={EyeOff} />
          )}
        </div>
      </div>

      {/* Trade Deviations — Split into Signal-Aligned and Independent */}
      {(() => {
        const signalAligned = (deviations || []).filter((item: any) => {
          const dev = item.deviation || item;
          return dev.classification === "signal_aligned";
        });
        const independent = (deviations || []).filter((item: any) => {
          const dev = item.deviation || item;
          return dev.classification !== "signal_aligned";
        });
        return (
          <>
            {/* Signal-Aligned Trades */}
            <div className="bg-card border border-border rounded-md p-3">
              <MetricTooltip title="Signal-Aligned Trades" description="Trades that matched an insider purchase signal. Compares your actual execution against the model's recommendation.">
                <h3 className="text-[10px] uppercase tracking-widest text-muted-foreground mb-2 cursor-help">Signal-Aligned Trades</h3>
              </MetricTooltip>
              <div className="overflow-auto max-h-56" style={{ overscrollBehavior: "contain" }}>
                {signalAligned.length > 0 ? (
                  <table className="w-full text-[11px]" data-testid="deviations-table">
                    <thead className="sticky top-0 bg-card z-10">
                      <tr className="border-b border-border text-muted-foreground">
                        <th className="text-left py-1.5 px-2 font-medium">TICKER</th>
                        <th className="text-center py-1.5 px-2 font-medium">SCORE</th>
                        <th className="text-right py-1.5 px-2 font-medium">
                          <MetricTooltip title="Entry Delay" description="Days between signal and your actual buy."><span className="cursor-help">ENTRY DELAY</span></MetricTooltip>
                        </th>
                        <th className="text-right py-1.5 px-2 font-medium">
                          <MetricTooltip title="Price Gap" description="How much the price moved against you due to late entry."><span className="cursor-help">PRICE GAP</span></MetricTooltip>
                        </th>
                        <th className="text-right py-1.5 px-2 font-medium">
                          <MetricTooltip title="Sizing Deviation" description="Difference between model's recommended position size and your actual."><span className="cursor-help">SIZE DEV</span></MetricTooltip>
                        </th>
                        <th className="text-right py-1.5 px-2 font-medium">
                          <MetricTooltip title="Hold Deviation" description="Difference between model's recommended holding period and your actual."><span className="cursor-help">HOLD DEV</span></MetricTooltip>
                        </th>
                        <th className="text-right py-1.5 px-2 font-medium">
                          <MetricTooltip title="P&L Difference" description="Return difference between your actual trade and the model's recommendation."><span className="cursor-help">P&L DIFF</span></MetricTooltip>
                        </th>
                      </tr>
                    </thead>
                    <tbody>
                      {signalAligned.map((item: any, i: number) => {
                        const dev = item.deviation || item;
                        const trade = item.trade || item;
                        const ticker = trade.ticker || dev.ticker || "";
                        const score = dev.signalScore || trade.signalScore || dev.score || 0;
                        return (
                          <tr key={i} className="terminal-row border-b border-border/30">
                            <td className="py-1 px-2 font-bold text-primary">{ticker}</td>
                            <td className="py-1 px-2 text-center">
                              <span className={`text-[10px] px-1.5 py-0.5 rounded font-bold ${getSignalClass(score)}`}>{score || "ALN"}</span>
                            </td>
                            <td className={`py-1 px-2 text-right ${(dev.entryDelayDays || dev.entryDelay || 0) > 1 ? "text-loss" : "text-foreground"}`} style={{ fontVariantNumeric: "tabular-nums" }}>
                              {dev.entryDelayDays != null ? `${dev.entryDelayDays}d` : dev.entryDelay != null ? `${dev.entryDelay}d` : "—"}
                            </td>
                            <td className={`py-1 px-2 text-right ${pnlColor(-(dev.entryPriceGapPct || dev.priceGap || 0))}`} style={{ fontVariantNumeric: "tabular-nums" }}>
                              {dev.entryPriceGapPct != null ? formatPct(dev.entryPriceGapPct) : dev.priceGap != null ? formatPct(dev.priceGap) : "—"}
                            </td>
                            <td className="py-1 px-2 text-right" style={{ fontVariantNumeric: "tabular-nums" }}>
                              {dev.sizingDeviationPct != null ? formatPct(dev.sizingDeviationPct) : dev.sizingDeviation != null ? formatPct(dev.sizingDeviation) : "—"}
                            </td>
                            <td className="py-1 px-2 text-right" style={{ fontVariantNumeric: "tabular-nums" }}>
                              {dev.holdDeviationDays != null ? `${dev.holdDeviationDays}d` : dev.holdDeviation != null ? `${dev.holdDeviation}d` : "—"}
                            </td>
                            <td className={`py-1 px-2 text-right font-medium ${pnlColor(dev.pnlDifference || dev.pnlDiff || 0)}`} style={{ fontVariantNumeric: "tabular-nums" }}>
                              {dev.pnlDifference != null ? formatPct(dev.pnlDifference) : dev.pnlDiff != null ? formatPct(dev.pnlDiff) : "—"}
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                ) : (
                  <EmptyState message={deviationsLoading ? "Loading trade deviations..." : "No signal-aligned trades found — your trades did not match any insider purchase signals"} icon={GitCompareArrows} />
                )}
              </div>
            </div>

            {/* Independent / Discretionary Trades */}
            <div className="bg-card border border-border rounded-md p-3">
              <MetricTooltip title="Independent Trades" description="Discretionary trades that were not based on any insider purchase signal. Shows your own alpha generation outside the signal model.">
                <h3 className="text-[10px] uppercase tracking-widest text-muted-foreground mb-2 cursor-help">Independent / Discretionary Trades</h3>
              </MetricTooltip>
              <div className="overflow-auto max-h-56" style={{ overscrollBehavior: "contain" }}>
                {independent.length > 0 ? (
                  <table className="w-full text-[11px]" data-testid="independent-trades-table">
                    <thead className="sticky top-0 bg-card z-10">
                      <tr className="border-b border-border text-muted-foreground">
                        <th className="text-left py-1.5 px-2 font-medium">TICKER</th>
                        <th className="text-left py-1.5 px-2 font-medium">DATE</th>
                        <th className="text-center py-1.5 px-2 font-medium">SIDE</th>
                        <th className="text-right py-1.5 px-2 font-medium">QTY</th>
                        <th className="text-right py-1.5 px-2 font-medium">AVG PRICE</th>
                        <th className="text-right py-1.5 px-2 font-medium">TOTAL</th>
                        <th className="text-right py-1.5 px-2 font-medium">
                          <MetricTooltip title="P&L Impact" description="Estimated return impact of this independent trade."><span className="cursor-help">P&L</span></MetricTooltip>
                        </th>
                      </tr>
                    </thead>
                    <tbody>
                      {independent.map((item: any, i: number) => {
                        const dev = item.deviation || item;
                        const trade = item.trade || item;
                        const ticker = trade.ticker || dev.ticker || "";
                        const date = trade.executionDate || dev.executionDate || "";
                        const side = trade.side || "";
                        const qty = trade.quantity || 0;
                        const avgPrice = trade.avgPrice || 0;
                        const total = trade.totalCost || (qty * avgPrice);
                        const pnl = dev.pnlDifference ?? dev.pnlDiff ?? null;
                        return (
                          <tr key={i} className="terminal-row border-b border-border/30">
                            <td className="py-1 px-2 font-bold text-primary">{ticker}</td>
                            <td className="py-1 px-2 text-muted-foreground">{date}</td>
                            <td className="py-1 px-2 text-center">
                              <span className={`text-[10px] px-1.5 py-0.5 rounded font-bold ${side === "BUY" ? "bg-gain text-[hsl(142,55%,15%)]" : side === "SELL" ? "bg-loss text-[hsl(0,72%,15%)]" : "bg-muted text-muted-foreground"}`}>
                                {side || "—"}
                              </span>
                            </td>
                            <td className="py-1 px-2 text-right" style={{ fontVariantNumeric: "tabular-nums" }}>{qty > 0 ? formatNumber(qty) : "—"}</td>
                            <td className="py-1 px-2 text-right" style={{ fontVariantNumeric: "tabular-nums" }}>{avgPrice > 0 ? `$${avgPrice.toFixed(2)}` : "—"}</td>
                            <td className="py-1 px-2 text-right font-medium" style={{ fontVariantNumeric: "tabular-nums" }}>{total > 0 ? formatCurrency(total) : "—"}</td>
                            <td className={`py-1 px-2 text-right font-medium ${pnlColor(pnl || 0)}`} style={{ fontVariantNumeric: "tabular-nums" }}>
                              {pnl != null ? formatPct(pnl) : "—"}
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                ) : (
                  <EmptyState message={deviationsLoading ? "Loading trades..." : "No independent trades found"} icon={Briefcase} />
                )}
              </div>
            </div>
          </>
        );
      })()}

      {/* Schwab Order History */}
      {schwabStatus?.isConnected && schwabOrders && schwabOrders.length > 0 && (
        <div className="bg-card border border-border rounded-md p-3">
          <h3 className="text-[10px] uppercase tracking-widest text-muted-foreground mb-2">
            Schwab Order History (60D)
            <span className="ml-2 text-[9px] text-green-400">LIVE</span>
          </h3>
          <div className="overflow-auto max-h-56" style={{ overscrollBehavior: "contain" }}>
            <table className="w-full text-[11px]" data-testid="schwab-orders-table">
              <thead className="sticky top-0 bg-card z-10">
                <tr className="border-b border-border text-muted-foreground">
                  <th className="text-left py-1.5 px-2 font-medium">DATE</th>
                  <th className="text-left py-1.5 px-2 font-medium">SIDE</th>
                  <th className="text-left py-1.5 px-2 font-medium">TICKER</th>
                  <th className="text-right py-1.5 px-2 font-medium">QTY</th>
                  <th className="text-right py-1.5 px-2 font-medium">PRICE</th>
                  <th className="text-left py-1.5 px-2 font-medium">STATUS</th>
                  <th className="text-left py-1.5 px-2 font-medium">TYPE</th>
                </tr>
              </thead>
              <tbody>
                {schwabOrders.map((order: any, i: number) => {
                  const legs = order.orderLegCollection || [];
                  const leg = legs[0] || {};
                  const ticker = leg.instrument?.symbol || "—";
                  const side = leg.instruction || "—";
                  const qty = order.filledQuantity || order.quantity || 0;
                  const price = order.price || order.stopPrice || 0;
                  const status = order.status || "—";
                  const date = order.closeTime || order.enteredTime || "";
                  const dateStr = date ? new Date(date).toLocaleDateString() : "—";
                  return (
                    <tr key={i} className="terminal-row border-b border-border/30">
                      <td className="py-1 px-2 text-muted-foreground">{dateStr}</td>
                      <td className="py-1 px-2">
                        <span className={`text-[10px] px-1.5 py-0.5 rounded font-bold ${side === "BUY" ? "bg-gain text-[hsl(142,55%,15%)]" : "bg-loss text-[hsl(0,72%,15%)]"}`}>
                          {side}
                        </span>
                      </td>
                      <td className="py-1 px-2 font-bold text-primary">{ticker}</td>
                      <td className="py-1 px-2 text-right" style={{ fontVariantNumeric: "tabular-nums" }}>{qty}</td>
                      <td className="py-1 px-2 text-right" style={{ fontVariantNumeric: "tabular-nums" }}>${price.toFixed(2)}</td>
                      <td className="py-1 px-2">
                        <span className={`text-[10px] ${status === "FILLED" ? "text-green-400" : status === "CANCELED" ? "text-red-400" : "text-muted-foreground"}`}>
                          {status}
                        </span>
                      </td>
                      <td className="py-1 px-2 text-muted-foreground text-[10px]">{order.orderType || "—"}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}


// ====================================================================
// TAB 6: SETTINGS
// ====================================================================
function SettingsTab() {
  const [appKey, setAppKey] = useState("");
  const [appSecret, setAppSecret] = useState("");
  const [redirectUrl, setRedirectUrl] = useState("");
  const [oauthStep, setOauthStep] = useState<"idle" | "waiting_redirect" | "exchanging" | "done">("idle");
  const [oauthError, setOauthError] = useState<string | null>(null);
  const [syncStatus, setSyncStatus] = useState<string | null>(null);

  const CALLBACK_URL = "https://127.0.0.1";

  const { data: schwabStatus } = useQuery<any>({
    queryKey: ["/api/schwab/status"],
    refetchInterval: 30000,
  });

  const { data: pipelineStatus } = useQuery<any>({
    queryKey: ["/api/settings/pipeline-status"],
    queryFn: async () => {
      const res = await apiRequest("GET", "/api/settings/pipeline-status");
      return res.json();
    },
    refetchInterval: 30000,
  });

  const { data: modelWeights } = useQuery<any[]>({
    queryKey: ["/api/factors/model-weights"],
    queryFn: async () => {
      const res = await apiRequest("GET", "/api/factors/model-weights");
      return res.json();
    },
  });

  const handleConnect = async () => {
    setOauthError(null);
    if (!appKey.trim() || !appSecret.trim()) {
      setOauthError("Both App Key and App Secret are required.");
      return;
    }
    try {
      const res = await apiRequest("POST", "/api/schwab/configure", { appKey: appKey.trim(), appSecret: appSecret.trim(), callbackUrl: CALLBACK_URL });
      const data = await res.json();
      if (data.authUrl) {
        window.open(data.authUrl, "_blank");
        setOauthStep("waiting_redirect");
      }
    } catch (err: any) {
      setOauthError(err.message || "Failed to configure Schwab.");
    }
  };

  const handleCallback = async () => {
    setOauthError(null);
    if (!redirectUrl.trim()) {
      setOauthError("Please paste the redirect URL from your browser.");
      return;
    }
    // Extract code parameter from URL
    let code = "";
    try {
      const match = redirectUrl.match(/code=([^&]+)/);
      if (match) {
        code = decodeURIComponent(match[1]);
        // Ensure code ends with @
        if (!code.endsWith("@")) code += "@";
      }
    } catch {
      // fallback: try as plain code
      code = redirectUrl.trim();
    }
    if (!code) {
      setOauthError("Could not extract authorization code from the URL. Make sure you copied the full URL.");
      return;
    }
    setOauthStep("exchanging");
    try {
      const res = await apiRequest("POST", "/api/schwab/callback", { code, callbackUrl: CALLBACK_URL });
      const data = await res.json();
      if (data.success) {
        setOauthStep("done");
        setRedirectUrl("");
        queryClient.invalidateQueries({ queryKey: ["/api/schwab/status"] });
      } else {
        setOauthError(data.error || "Token exchange failed.");
        setOauthStep("waiting_redirect");
      }
    } catch (err: any) {
      setOauthError(err.message || "Token exchange failed.");
      setOauthStep("waiting_redirect");
    }
  };

  const handleSync = async () => {
    setSyncStatus("syncing");
    try {
      const res = await apiRequest("POST", "/api/schwab/sync");
      const data = await res.json();
      if (data.success) {
        setSyncStatus(`Synced ${data.syncedPositions} positions`);
        queryClient.invalidateQueries({ queryKey: ["/api/schwab/status"] });
        queryClient.invalidateQueries({ queryKey: ["/api/portfolio/positions"] });
        queryClient.invalidateQueries({ queryKey: ["/api/schwab/positions"] });
      } else {
        setSyncStatus(`Error: ${data.error}`);
      }
    } catch (err: any) {
      setSyncStatus(`Error: ${err.message}`);
    }
    setTimeout(() => setSyncStatus(null), 5000);
  };

  const ps = pipelineStatus || {};
  const enrichedCount = ps.enrichedSignals ?? ps.signalsEnriched ?? 0;
  const totalSignals = ps.totalSignals ?? ps.signalsTotal ?? 0;
  const enrichProgress = totalSignals ? (enrichedCount / totalSignals * 100) : 0;

  return (
    <div className="space-y-4 max-w-4xl">
      {/* Schwab Integration */}
      <div className="bg-card border border-border rounded-md p-4">
        <div className="flex items-center gap-3 mb-4">
          <div className="w-8 h-8 rounded bg-muted flex items-center justify-center">
            <Link className="w-4 h-4 text-primary" />
          </div>
          <div>
            <h3 className="text-sm font-bold text-foreground">Charles Schwab / thinkorswim Integration</h3>
            <p className="text-[11px] text-muted-foreground">Connect your Schwab account to sync positions, orders, and track execution performance against insider signals.</p>
          </div>
        </div>

        <div className="space-y-3">
          {/* Status */}
          <div className="flex items-center justify-between p-3 bg-background rounded border border-border/50">
            <div className="flex items-center gap-2">
              <div className={`w-2 h-2 rounded-full ${schwabStatus?.isConnected ? "bg-green-500 animate-pulse-live" : "bg-red-500"}`} />
              <span className="text-[11px] text-foreground">{schwabStatus?.isConnected ? "Connected" : "Not Connected"}</span>
              {schwabStatus?.accountNumber && (
                <span className="text-[10px] text-muted-foreground">Account: {schwabStatus.accountNumber}</span>
              )}
            </div>
            {schwabStatus?.lastSyncAt && (
              <span className="text-[10px] text-muted-foreground">Last sync: {timeAgo(schwabStatus.lastSyncAt)}</span>
            )}
          </div>

          {/* Setup Instructions */}
          <div className="p-3 bg-background rounded border border-border/50 space-y-2">
            <h4 className="text-[11px] font-bold text-foreground uppercase tracking-wider">Setup Instructions</h4>
            <ol className="space-y-2 text-[11px] text-muted-foreground">
              <li className="flex gap-2">
                <span className="text-primary font-bold shrink-0">1.</span>
                <span>Go to <span className="text-primary">developer.schwab.com</span> and create a Trader API (Individual) app</span>
              </li>
              <li className="flex gap-2">
                <span className="text-primary font-bold shrink-0">2.</span>
                <span>Set the callback URL to <span className="text-primary font-mono">https://127.0.0.1</span></span>
              </li>
              <li className="flex gap-2">
                <span className="text-primary font-bold shrink-0">3.</span>
                <span>Copy your App Key and App Secret from the Schwab developer portal</span>
              </li>
              <li className="flex gap-2">
                <span className="text-primary font-bold shrink-0">4.</span>
                <span>Enter them below and click "Connect" to begin the OAuth2 authorization flow</span>
              </li>
              <li className="flex gap-2">
                <span className="text-primary font-bold shrink-0">5.</span>
                <span>After authorizing, copy the full redirect URL and paste it below to complete the connection</span>
              </li>
            </ol>
          </div>

          {/* API Key Fields — actual inputs */}
          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="text-[10px] uppercase tracking-widest text-muted-foreground mb-1 block">App Key</label>
              <input
                type="text"
                value={appKey}
                onChange={(e) => setAppKey(e.target.value)}
                placeholder={schwabStatus?.isConnected ? "••••••••••••" : "Enter your Schwab App Key"}
                className="w-full bg-background border border-border rounded px-3 py-2 text-[11px] text-foreground placeholder:text-muted-foreground focus:outline-none focus:border-primary transition-colors"
                data-testid="schwab-app-key"
              />
            </div>
            <div>
              <label className="text-[10px] uppercase tracking-widest text-muted-foreground mb-1 block">App Secret</label>
              <input
                type="password"
                value={appSecret}
                onChange={(e) => setAppSecret(e.target.value)}
                placeholder={schwabStatus?.isConnected ? "••••••••••••" : "Enter your Schwab App Secret"}
                className="w-full bg-background border border-border rounded px-3 py-2 text-[11px] text-foreground placeholder:text-muted-foreground focus:outline-none focus:border-primary transition-colors"
                data-testid="schwab-app-secret"
              />
            </div>
          </div>

          {/* Redirect URL field — shown after clicking Connect */}
          {(oauthStep === "waiting_redirect" || oauthStep === "exchanging") && (
            <div>
              <label className="text-[10px] uppercase tracking-widest text-muted-foreground mb-1 block">
                Paste the redirect URL here
              </label>
              <p className="text-[10px] text-muted-foreground mb-1">
                After authorizing on Schwab, you'll be redirected to a page that says "can't be reached". Copy the <span className="text-primary">full URL</span> from your browser's address bar and paste it below.
              </p>
              <input
                type="text"
                value={redirectUrl}
                onChange={(e) => setRedirectUrl(e.target.value)}
                placeholder="https://127.0.0.1?code=...&session=..."
                className="w-full bg-background border border-border rounded px-3 py-2 text-[11px] text-foreground placeholder:text-muted-foreground focus:outline-none focus:border-primary transition-colors font-mono"
                data-testid="schwab-redirect-url"
              />
              <button
                onClick={handleCallback}
                disabled={oauthStep === "exchanging"}
                className="mt-2 bg-green-600 text-white text-[11px] font-bold px-4 py-2 rounded hover:opacity-90 transition-opacity disabled:opacity-50"
                data-testid="schwab-submit-code"
              >
                {oauthStep === "exchanging" ? "Exchanging token..." : "Complete Connection"}
              </button>
            </div>
          )}

          {/* Error message */}
          {oauthError && (
            <div className="p-2 bg-red-500/10 border border-red-500/30 rounded text-[11px] text-red-400">
              {oauthError}
            </div>
          )}

          {/* Success message */}
          {oauthStep === "done" && (
            <div className="p-2 bg-green-500/10 border border-green-500/30 rounded text-[11px] text-green-400">
              Schwab account connected successfully!
            </div>
          )}

          {/* Sync status */}
          {syncStatus && (
            <div className={`p-2 rounded text-[11px] ${syncStatus.startsWith("Error") ? "bg-red-500/10 border border-red-500/30 text-red-400" : "bg-green-500/10 border border-green-500/30 text-green-400"}`}>
              {syncStatus === "syncing" ? "Syncing positions..." : syncStatus}
            </div>
          )}

          <div className="flex gap-2">
            <button
              onClick={handleConnect}
              className="bg-primary text-primary-foreground text-[11px] font-bold px-4 py-2 rounded hover:opacity-90 transition-opacity"
              data-testid="schwab-connect-btn"
            >
              {schwabStatus?.isConnected ? "Reconnect" : "Connect Schwab Account"}
            </button>
            {schwabStatus?.isConnected && (
              <button
                onClick={handleSync}
                disabled={syncStatus === "syncing"}
                className="bg-muted text-muted-foreground text-[11px] px-4 py-2 rounded hover:bg-muted/80 transition-colors disabled:opacity-50"
                data-testid="schwab-sync-btn"
              >
                {syncStatus === "syncing" ? "Syncing..." : "Sync Now"}
              </button>
            )}
          </div>
        </div>
      </div>

      {/* Data Pipeline Status */}
      <div className="bg-card border border-border rounded-md p-4">
        <h3 className="text-[11px] font-bold text-foreground uppercase tracking-wider mb-3">Data Pipeline Status</h3>
        <div className="grid grid-cols-2 md:grid-cols-3 gap-3">
          {[
            { label: "Total Purchases", value: ps.totalPurchases != null ? formatNumber(ps.totalPurchases) : "—", desc: "Open-market purchase transactions loaded" },
            { label: "Signals Generated", value: totalSignals > 0 ? formatNumber(totalSignals) : "—", desc: "Composite signals computed from transactions" },
            { label: "Signals Enriched", value: enrichedCount > 0 || totalSignals > 0 ? `${formatNumber(enrichedCount)} / ${formatNumber(totalSignals)}` : "—", desc: "Signals with forward returns and factor analysis", progress: enrichProgress },
            { label: "Forward Returns", value: ps.forwardReturnDataPoints != null ? formatNumber(ps.forwardReturnDataPoints) : "—", desc: "Daily forward return data points computed" },
            { label: "Factor Analysis", value: ps.factorAnalysisResults != null ? formatNumber(ps.factorAnalysisResults) : "—", desc: "Factor effectiveness analysis results" },
            { label: "Model Factors", value: ps.modelFactors != null ? ps.modelFactors.toString() : "—", desc: "Active factors in the scoring model" },
          ].map((item, i) => (
            <MetricTooltip key={i} title={item.label} description={item.desc}>
              <div className="bg-background rounded border border-border/50 p-3 cursor-help">
                <div className="text-[10px] text-muted-foreground uppercase tracking-wider">{item.label}</div>
                <div className="text-sm font-bold text-foreground mt-1" style={{ fontVariantNumeric: "tabular-nums" }}>{item.value}</div>
                {item.progress != null && item.progress > 0 && (
                  <div className="mt-1.5 h-1 bg-muted rounded-full overflow-hidden">
                    <div className="h-full bg-primary rounded-full transition-all" style={{ width: `${Math.min(item.progress, 100)}%` }} />
                  </div>
                )}
              </div>
            </MetricTooltip>
          ))}
        </div>
      </div>

      {/* Signal Methodology — V3 Multi-Factor Model */}
      <div className="bg-card border border-border rounded-md p-4">
        <h3 className="text-[11px] font-bold text-foreground uppercase tracking-wider mb-3">V3 Signal Methodology — Multi-Factor Scoring Model</h3>
        <div className="space-y-3 text-[11px] text-muted-foreground leading-relaxed">
          <p>The V3 composite score is derived from <span className="text-foreground font-bold">11 empirical factors</span>, each weighted by its actual predictive power for forward returns. Unlike V2's equal-weighted 4-factor model, V3 weights are <span className="text-primary">derived from data</span>, not academic assumptions.</p>

          {/* Current Model Weights Table */}
          {modelWeights && modelWeights.length > 0 ? (
            <div className="overflow-auto">
              <table className="w-full text-[11px]">
                <thead>
                  <tr className="border-b border-border text-muted-foreground">
                    <th className="text-left py-1.5 px-2 font-medium">FACTOR</th>
                    <th className="text-right py-1.5 px-2 font-medium">WEIGHT</th>
                    <th className="text-right py-1.5 px-2 font-medium">CONFIDENCE</th>
                  </tr>
                </thead>
                <tbody>
                  {modelWeights.map((w: any, i: number) => {
                    const weight = w.weight ?? w.effectiveWeight ?? w.effective_weight ?? 0;
                    const name = w.factorName || w.factor_name || w.name || `Factor ${i}`;
                    const conf = w.confidence ?? w.confidenceLevel ?? w.confidence_level ?? "—";
                    return (
                      <tr key={i} className="terminal-row border-b border-border/30">
                        <td className="py-1 px-2 text-foreground">{name}</td>
                        <td className="py-1 px-2 text-right text-primary font-bold" style={{ fontVariantNumeric: "tabular-nums" }}>{(weight * 100).toFixed(1)}%</td>
                        <td className="py-1 px-2 text-right text-muted-foreground">{conf}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="text-[10px] text-muted-foreground">Model weights not yet computed.</div>
          )}

          <p className="text-[10px] border-t border-border/50 pt-2 mt-2">
            <span className="text-primary font-bold">Note:</span> Weights are derived from data, not academic assumptions. The model is retrained periodically as new forward-return data becomes available.
            Academic basis: Jeng, Metrick & Zeckhauser (2003), Lakonishok & Lee (2001).
          </p>
        </div>
      </div>
    </div>
  );
}


// ====================================================================
// MAIN DASHBOARD — 6-Tab Layout
// ====================================================================
export default function Dashboard() {
  const [activeTab, setActiveTab] = useState("signals");

  // Force dark mode
  if (typeof document !== "undefined") {
    document.documentElement.classList.add("dark");
  }

  const { data: dashboard } = useQuery<any>({
    queryKey: ["/api/dashboard"],
    refetchInterval: 30000,
  });

  const { data: pipelineStatus } = useQuery<any>({
    queryKey: ["/api/settings/pipeline-status"],
    queryFn: async () => {
      const res = await apiRequest("GET", "/api/settings/pipeline-status");
      return res.json();
    },
    refetchInterval: 30000,
  });

  const pollingStatus = dashboard?.pollingStatus;

  const tabs = [
    { id: "signals", label: "SIGNALS", icon: Signal },
    { id: "factors", label: "FACTOR RESEARCH", icon: FlaskConical },
    { id: "portfolio", label: "PORTFOLIO", icon: Briefcase },
    { id: "performance", label: "PERFORMANCE", icon: LineChartIcon },
    { id: "execution", label: "EXECUTION", icon: Crosshair },
    { id: "settings", label: "SETTINGS", icon: Settings },
  ];

  return (
    <div className="h-screen flex flex-col bg-background overflow-hidden" data-testid="dashboard-root">
      {/* Header Bar */}
      <header className="flex items-center justify-between px-4 py-2 border-b border-border bg-card shrink-0">
        <div className="flex items-center gap-3">
          <div className="flex items-center gap-2">
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" aria-label="Insider Signal Dashboard">
              <rect x="2" y="2" width="20" height="20" rx="3" stroke="hsl(37, 90%, 55%)" strokeWidth="1.5" />
              <path d="M7 14l3-4 3 2 4-5" stroke="hsl(37, 90%, 55%)" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
              <circle cx="17" cy="7" r="2" fill="hsl(37, 90%, 55%)" />
            </svg>
            <span className="text-sm font-bold text-primary tracking-wide">INSIDER SIGNAL</span>
          </div>
          <span className="text-[10px] text-muted-foreground border border-border rounded px-1.5 py-0.5">V3</span>
        </div>

        {/* Navigation Tabs */}
        <div className="flex items-center gap-0.5">
          {tabs.map((tab) => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              className={`flex items-center gap-1 px-2.5 py-1.5 rounded text-[9px] font-bold uppercase tracking-wider transition-colors ${
                activeTab === tab.id
                  ? "bg-primary/15 text-primary border border-primary/30"
                  : "text-muted-foreground hover:text-foreground hover:bg-muted/50"
              }`}
              data-testid={`tab-${tab.id}`}
            >
              <tab.icon className="w-3 h-3" />
              <span className="hidden md:inline">{tab.label}</span>
            </button>
          ))}
        </div>

        <div className="flex items-center gap-4">
          <MetricTooltip title="Data Feed Status" description="Real-time polling of SEC EDGAR for new Form 4 filings.">
            <div className="flex items-center gap-2 cursor-help" data-testid="status-indicator">
              <div className={`w-1.5 h-1.5 rounded-full ${(pollingStatus?.active || pollingStatus?.isActive) ? "bg-green-500 animate-pulse-live" : "bg-red-500"}`} />
              <span className="text-[10px] text-muted-foreground">
                {(pollingStatus?.active || pollingStatus?.isActive) ? "LIVE" : "OFFLINE"}
                {pollingStatus?.stats?.uptime ? ` · ${pollingStatus.stats.uptime}` : pollingStatus?.lastPollTime ? ` · ${timeAgo(pollingStatus.lastPollTime)}` : ""}
              </span>
            </div>
          </MetricTooltip>

          <MetricTooltip title="Total Filings" description="Cumulative Form 4 filings parsed and stored.">
            <div className="flex items-center gap-1.5 cursor-help">
              <Database className="w-3 h-3 text-muted-foreground" />
              <span className="text-[10px] text-muted-foreground">{formatNumber(pollingStatus?.stats?.eftsPolls || pollingStatus?.totalFilingsProcessed || 0)} polls</span>
            </div>
          </MetricTooltip>

          <span className="text-[10px] text-muted-foreground hidden lg:inline">
            {new Date().toLocaleDateString("en-US", { weekday: "short", month: "short", day: "numeric", year: "numeric" })}
          </span>
        </div>
      </header>

      {/* Enrichment Progress Banner */}
      {pipelineStatus && pipelineStatus.enrichmentProgress < 100 && pipelineStatus.totalSignals > 0 && (
        <div className="px-4 py-2 bg-amber-500/10 border-b border-amber-500/20 shrink-0">
          <div className="flex items-center gap-3">
            <Database className="w-3.5 h-3.5 text-amber-400 shrink-0" />
            <div className="flex-1 min-w-0">
              <div className="flex items-center justify-between mb-1">
                <span className="text-[11px] text-amber-300 font-medium">
                  Data Enrichment: {pipelineStatus.enrichmentProgress}% complete ({formatNumber(pipelineStatus.enrichedSignals || 0)} / {formatNumber((pipelineStatus as any).enrichableSignals || pipelineStatus.totalSignals)} signals){(pipelineStatus as any).skippedSignals > 0 ? ` · ${formatNumber((pipelineStatus as any).skippedSignals)} skipped (no market data)` : ''}
                </span>
                <span className="text-[10px] text-amber-400/60">Analytics improve as enrichment progresses</span>
              </div>
              <div className="w-full h-1.5 bg-amber-900/30 rounded-full overflow-hidden">
                <div
                  className="h-full bg-amber-500 rounded-full transition-all duration-1000"
                  style={{ width: `${Math.max(pipelineStatus.enrichmentProgress, 1)}%` }}
                />
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Main Content */}
      <main className="flex-1 overflow-auto p-3" style={{ overscrollBehavior: "contain" }}>
        {activeTab === "signals" && <SignalsTab />}
        {activeTab === "factors" && <FactorResearchTab />}
        {activeTab === "portfolio" && <PortfolioTab />}
        {activeTab === "performance" && <PerformanceTab />}
        {activeTab === "execution" && <ExecutionTab />}
        {activeTab === "settings" && <SettingsTab />}

        {/* Footer */}
        <footer className="text-[9px] text-muted-foreground text-center py-2 border-t border-border mt-3">
          Data sourced from SEC EDGAR Form 4 filings · V3 Multi-Factor Model: 11 empirical factors weighted by predictive power
          · 304K+ transactions · {new Date().getFullYear()} · Not investment advice
        </footer>
      </main>
    </div>
  );
}
