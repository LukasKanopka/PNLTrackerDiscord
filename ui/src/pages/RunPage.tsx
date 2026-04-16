import { useCallback, useEffect, useMemo, useState } from 'react';
import { Link, useParams } from 'react-router-dom';
import { Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import { apiDelete, apiGet, apiPostForm } from '../api/client';
import type { RunBetsResponse, RunDetailResponse, RunIssuesResponse, RunReportResponse } from '../api/types';
import { Button, Card, DangerButton, Input, Pill, Select, Small } from '../components/Ui';

function fmtMoney(x: number | null | undefined) {
  if (x === null || x === undefined) return '—';
  return new Intl.NumberFormat(undefined, { style: 'currency', currency: 'USD', maximumFractionDigits: 2 }).format(x);
}

function toneForStatus(s: string | null | undefined) {
  const st = (s || '').toUpperCase();
  if (st === 'DONE') return 'good';
  if (st === 'ERROR') return 'bad';
  if (st === 'ANALYZING') return 'warn';
  return 'neutral';
}

type Tab = 'overview' | 'users' | 'bets' | 'debug';

function asRecord(x: unknown): Record<string, unknown> | null {
  if (!x || typeof x !== 'object') return null;
  return x as Record<string, unknown>;
}

function asNumber(x: unknown): number | null {
  if (typeof x === 'number' && Number.isFinite(x)) return x;
  return null;
}

function fmtAxisTime(ts: unknown) {
  try {
    const d = new Date(String(ts));
    // Show date when it changes; otherwise just time keeps the chart clean.
    return d.toLocaleString(undefined, { month: 'short', day: '2-digit', hour: '2-digit', minute: '2-digit' });
  } catch {
    return String(ts ?? '');
  }
}

export function RunPage() {
  const { runId } = useParams();
  const [tab, setTab] = useState<Tab>('overview');
  const [detail, setDetail] = useState<RunDetailResponse | null>(null);
  const [report, setReport] = useState<RunReportResponse | null>(null);
  const [issues, setIssues] = useState<RunIssuesResponse | null>(null);
  const [bets, setBets] = useState<RunBetsResponse | null>(null);
  const [unitNotional, setUnitNotional] = useState<string>('100');

  const [betsAuthor, setBetsAuthor] = useState('');
  const [betsPlatform, setBetsPlatform] = useState('');
  const [betsStatus, setBetsStatus] = useState('');
  const [betsSort, setBetsSort] = useState('ts_desc');
  const [limit, setLimit] = useState(50);
  const [offset, setOffset] = useState(0);

  const loadDetail = useCallback(async () => {
    if (!runId) return;
    const d = await apiGet<RunDetailResponse>(`/v1/runs/${runId}`);
    setDetail(d);
    const st = 'status' in d ? d.status : null;
    if (st === 'DONE') {
      setReport(await apiGet<RunReportResponse>(`/v1/runs/${runId}/report`));
      setIssues(await apiGet<RunIssuesResponse>(`/v1/runs/${runId}/issues`));
    } else {
      setReport(null);
      setIssues(null);
    }
  }, [runId]);

  const loadBets = useCallback(async () => {
    if (!runId) return;
    const qs = new URLSearchParams();
    qs.set('limit', String(limit));
    qs.set('offset', String(offset));
    if (betsAuthor) qs.set('author', betsAuthor);
    if (betsPlatform) qs.set('platform', betsPlatform);
    if (betsStatus) qs.set('status', betsStatus);
    if (betsSort) qs.set('sort', betsSort);
    setBets(await apiGet<RunBetsResponse>(`/v1/runs/${runId}/bets?${qs.toString()}`));
  }, [runId, limit, offset, betsAuthor, betsPlatform, betsStatus, betsSort]);

  useEffect(() => {
    loadDetail();
    const t = window.setInterval(loadDetail, 2500);
    return () => window.clearInterval(t);
  }, [runId, loadDetail]);

  useEffect(() => {
    loadBets();
  }, [loadBets]);

  const isError = detail && 'error' in detail;
  const status = detail && !('error' in detail) ? detail.status : null;

  async function triggerAnalyze() {
    if (!runId) return;
    const fd = new FormData();
    fd.append('verify_prices', 'true');
    await apiPostForm(`/v1/runs/${runId}/analyze_async`, fd);
    await loadDetail();
  }

  async function deleteRun() {
    if (!runId) return;
    const ok = window.confirm('Delete this run and all its DB data? This cannot be undone.');
    if (!ok) return;
    await apiDelete<{ ok?: boolean; error?: string }>(`/v1/runs/${runId}`);
    window.location.href = '/';
  }

  const metrics = useMemo(() => {
    if (!detail || 'error' in detail) return null;
    return asRecord(detail.metrics);
  }, [detail]);

  useEffect(() => {
    if (!detail || 'error' in detail) return;
    const snap = asRecord(detail.settings_snapshot);
    const u = snap ? asNumber(snap['unit_notional_usd']) : null;
    if (u !== null) setUnitNotional(String(u));
  }, [detail]);

  return (
    <div style={{ display: 'grid', gap: 14 }}>
      <Card
        title={`Run ${runId}`}
        right={
          <div className="row">
            {status && <Pill label={status} tone={toneForStatus(status)} />}
            <a className="btn btn-ghost" href={`/v1/runs/${runId}/upload`} target="_blank" rel="noreferrer">
              Download upload
            </a>
            <DangerButton onClick={deleteRun} disabled={status === 'ANALYZING'}>
              Delete
            </DangerButton>
            {status !== 'ANALYZING' && status !== 'DONE' && (
              <Button onClick={triggerAnalyze} disabled={status === 'ERROR'}>
                Analyze
              </Button>
            )}
          </div>
        }
      >
        {isError ? (
          <div className="muted">Error: {(detail as { error: string }).error}</div>
        ) : (
          <div className="grid3">
            <div>
              <Small>File</Small>
              <div className="mono">{(detail as RunDetailResponse & { source_filename?: string | null })?.source_filename ?? '—'}</div>
            </div>
            <div>
              <Small>Messages / Calls</Small>
              {detail && !('error' in detail) ? (
                <div>
                  {detail.message_count ?? '—'} / {detail.call_count ?? '—'}
                </div>
              ) : (
                <div>—</div>
              )}
            </div>
            <div>
              <Small>Timings</Small>
              {detail && !('error' in detail) ? (
                <div className="mono">
                  parse={detail.parse_ms ?? '—'}ms · extract={detail.extract_ms ?? '—'}ms · analyze={detail.analyze_ms ?? '—'}ms
                </div>
              ) : (
                <div className="mono">—</div>
              )}
            </div>
          </div>
        )}
        {detail && !('error' in detail) && detail.error_text && <div className="muted">Error: {detail.error_text}</div>}
      </Card>

      <div className="row" style={{ gap: 10, flexWrap: 'wrap' }}>
        <div className="segmented">
          <Button variant={tab === 'overview' ? 'primary' : 'ghost'} onClick={() => setTab('overview')}>
            Overview
          </Button>
          <Button variant={tab === 'users' ? 'primary' : 'ghost'} onClick={() => setTab('users')}>
            Users
          </Button>
          <Button variant={tab === 'bets' ? 'primary' : 'ghost'} onClick={() => setTab('bets')}>
            Bets
          </Button>
          <Button variant={tab === 'debug' ? 'primary' : 'ghost'} onClick={() => setTab('debug')}>
            Debug
          </Button>
        </div>
        <div style={{ flex: 1 }} />
        <Link className="btn btn-ghost" to="/">
          Back to runs
        </Link>
      </div>

      {tab === 'overview' && (
        <div className="grid2">
          <Card title="P&L Summary">
            {report && !('error' in report) ? (
              <div style={{ display: 'grid', gap: 10 }}>
                <div className="grid2">
                  <div>
                    <Small>Total net P&L</Small>
                    <div style={{ fontSize: 22, fontWeight: 700 }}>{fmtMoney(report.aggregate.total_net_pnl_usd)}</div>
                  </div>
                  <div>
                    <Small>Win rate / Resolved</Small>
                    <div style={{ fontSize: 22, fontWeight: 700 }}>
                      {report.aggregate.win_rate === null ? '—' : `${Math.round(report.aggregate.win_rate * 100)}%`} ·{' '}
                      {report.aggregate.resolved_bets}
                    </div>
                  </div>
                </div>
                <div className="grid2">
                  <div>
                    <Small>Max drawdown</Small>
                    <div style={{ fontSize: 18, fontWeight: 650 }}>{fmtMoney(report.aggregate.max_drawdown_usd)}</div>
                  </div>
                  <div>
                    <Small>Download</Small>
                    <a className="btn btn-ghost" href={`/v1/runs/${runId}/report`} target="_blank" rel="noreferrer">
                      JSON report
                    </a>
                  </div>
                </div>
              </div>
            ) : (
              <Small className="muted">{status === 'DONE' ? 'Report unavailable.' : 'Waiting for analysis…'}</Small>
            )}
          </Card>

          <Card
            title="Sizing (Amount per bet)"
            right={<Small>Updates contracts &amp; P&amp;L</Small>}
          >
            <div style={{ display: 'grid', gap: 10 }}>
              <Small className="muted">
                Unit notional is the fixed dollar amount wagered per bet for this run.
              </Small>
              <div className="grid2">
                <div>
                  <Small>Unit notional (USD)</Small>
                  <Input
                    inputMode="decimal"
                    value={unitNotional}
                    onChange={(e) => setUnitNotional(e.target.value)}
                    placeholder="100"
                  />
                </div>
                <div />
              </div>
              <div className="row" style={{ justifyContent: 'flex-end' }}>
                <Button
                  onClick={async () => {
                    if (!runId) return;
                    const fd = new FormData();
                    if (unitNotional.trim()) fd.append('unit_notional_usd', unitNotional.trim());
                    await apiPostForm(`/v1/runs/${runId}/rescale`, fd);
                    await loadDetail();
                  }}
                  disabled={status === 'ANALYZING'}
                >
                  Save &amp; Recompute P&amp;L
                </Button>
              </div>
            </div>
          </Card>

          <Card title="Equity Curve">
            {report && !('error' in report) ? (
              <div style={{ height: 240 }}>
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={report.equity_curve}>
                    <XAxis
                      dataKey="timestamp_utc"
                      tickFormatter={fmtAxisTime}
                      minTickGap={28}
                      interval="preserveStartEnd"
                      tick={{ fill: 'rgba(250,250,250,0.65)', fontSize: 11 }}
                      axisLine={{ stroke: 'rgba(255,255,255,0.10)' }}
                      tickLine={{ stroke: 'rgba(255,255,255,0.10)' }}
                    />
                    <YAxis tickFormatter={(v) => `${v}`} />
                    <Tooltip
                      labelFormatter={(l) => new Date(String(l)).toLocaleString()}
                      formatter={(v) => [fmtMoney(Number(v)), 'Cum P&L']}
                    />
                    <Line type="monotone" dataKey="cum_net_pnl_usd" stroke="#a78bfa" dot={false} />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            ) : (
              <Small className="muted">No equity curve yet.</Small>
            )}
          </Card>

          <Card title="Funnel Hit-Rate (Debug)">
            <div style={{ display: 'grid', gap: 10 }}>
              <Small className="muted">
                Parse → Candidates → Extracted Calls → Analysis statuses. Use this to spot missing bets.
              </Small>
              <div className="grid2">
                <div>
                  <Small>Parsed messages</Small>
                  <div style={{ fontSize: 20, fontWeight: 650 }}>
                    {asNumber(asRecord(metrics?.parse)?.parsed_message_count) ?? '—'}
                  </div>
                </div>
                <div>
                  <Small>Candidates</Small>
                  <div style={{ fontSize: 20, fontWeight: 650 }}>
                    {asNumber(asRecord(metrics?.candidates)?.candidate_count) ?? '—'}
                  </div>
                </div>
              </div>
              <div className="grid2">
                <div>
                  <Small>Extracted calls</Small>
                  <div style={{ fontSize: 20, fontWeight: 650 }}>
                    {asNumber(asRecord(metrics?.extraction)?.extracted_call_count) ?? '—'}
                  </div>
                </div>
                <div>
                  <Small>Msgs with URL but no candidate</Small>
                  <div style={{ fontSize: 20, fontWeight: 650 }}>
                    {asNumber(asRecord(metrics?.candidates)?.messages_with_market_url_but_no_candidate) ?? '—'}
                  </div>
                </div>
              </div>
            </div>
          </Card>
        </div>
      )}

      {tab === 'users' && (
        <Card title="Best Bettors (advanced metrics)">
          {report && !('error' in report) ? (
            <table className="table">
              <thead>
                <tr>
                  <th>User</th>
                  <th>Bets</th>
                  <th>Win%</th>
                  <th>Net P&L</th>
                  <th>Avg/bet</th>
                  <th>Median</th>
                  <th>Profit factor</th>
                  <th>Max DD</th>
                </tr>
              </thead>
              <tbody>
                {report.user_stats.map((u) => (
                  <tr key={u.author}>
                    <td className="mono">
                      <Link to={`/runs/${runId}/users/${encodeURIComponent(u.author)}`}>{u.author}</Link>
                    </td>
                    <td>{u.bets}</td>
                    <td>{u.win_rate === null ? '—' : `${Math.round(u.win_rate * 100)}%`}</td>
                    <td>{fmtMoney(u.net_pnl_usd)}</td>
                    <td>{u.avg_pnl_per_bet === null ? '—' : fmtMoney(u.avg_pnl_per_bet)}</td>
                    <td>{u.median_pnl_usd === null ? '—' : fmtMoney(u.median_pnl_usd)}</td>
                    <td>
                      {u.profit_factor === null
                        ? u.profit_factor_is_infinite
                          ? '∞'
                          : '—'
                        : u.profit_factor.toFixed(2)}
                    </td>
                    <td>{fmtMoney(u.max_drawdown_usd)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <Small className="muted">Waiting for analysis…</Small>
          )}
        </Card>
      )}

      {tab === 'bets' && (
        <Card
          title="Bets"
          right={
            <div className="row">
              <Small>Filters</Small>
            </div>
          }
        >
          <div style={{ display: 'grid', gap: 10 }}>
            <div className="grid3">
              <div>
                <Small>Author</Small>
                <Input value={betsAuthor} onChange={(e) => setBetsAuthor(e.target.value)} placeholder="e.g. champtgram" />
              </div>
              <div>
                <Small>Platform</Small>
                <Select value={betsPlatform} onChange={(e) => setBetsPlatform(e.target.value)}>
                  <option value="">(any)</option>
                  <option value="kalshi">kalshi</option>
                  <option value="polymarket">polymarket</option>
                </Select>
              </div>
              <div>
                <Small>Status</Small>
                <Select value={betsStatus} onChange={(e) => setBetsStatus(e.target.value)}>
                  <option value="">(any)</option>
                  <option value="OK">OK</option>
                  <option value="UNMATCHED">UNMATCHED</option>
                  <option value="AMBIGUOUS_MARKET">AMBIGUOUS_MARKET</option>
                  <option value="PENDING">PENDING</option>
                  <option value="ERROR_MISSING_ENTRY_PRICE">ERROR_MISSING_ENTRY_PRICE</option>
                  <option value="ERROR">ERROR</option>
                </Select>
              </div>
            </div>
            <div className="grid3">
              <div>
                <Small>Sort</Small>
                <Select value={betsSort} onChange={(e) => setBetsSort(e.target.value)}>
                  <option value="ts_desc">Newest</option>
                  <option value="ts_asc">Oldest</option>
                  <option value="net_pnl_desc">P&L desc</option>
                  <option value="net_pnl_asc">P&L asc</option>
                </Select>
              </div>
              <div>
                <Small>Limit</Small>
                <Select value={String(limit)} onChange={(e) => setLimit(Number(e.target.value))}>
                  <option value="25">25</option>
                  <option value="50">50</option>
                  <option value="100">100</option>
                </Select>
              </div>
              <div className="row" style={{ justifyContent: 'flex-end' }}>
                <Button variant="ghost" onClick={() => setOffset(Math.max(0, offset - limit))} disabled={offset === 0}>
                  Prev
                </Button>
                <Button variant="ghost" onClick={() => setOffset(offset + limit)}>
                  Next
                </Button>
              </div>
            </div>

            {bets && 'error' in bets ? (
              <Small className="muted">Error: {bets.error}</Small>
            ) : (
              bets &&
              !('error' in bets) && (
                <table className="table">
                  <thead>
                    <tr>
                      <th>User</th>
                      <th>Time</th>
                      <th>Platform</th>
                      <th>Side</th>
                      <th>Units</th>
                      <th>Notional</th>
                      <th>Status</th>
                      <th>Net P&L</th>
                      <th>Match</th>
                      <th>Price</th>
                    </tr>
                  </thead>
                  <tbody>
                    {bets.bets.map((b) => (
                      <tr key={b.call_id}>
                        <td className="mono">{b.call.author}</td>
                        <td className="mono">{new Date(b.call.timestamp_utc).toLocaleString()}</td>
                        <td className="mono">{b.call.platform}</td>
                        <td className="mono">{b.call.position_direction}</td>
                        <td className="mono">{b.call.bet_size_units}</td>
                        <td className="mono">
                          {b.result?.contracts !== null && b.result?.contracts !== undefined
                            ? fmtMoney(b.result.contracts)
                            : '—'}
                        </td>
                        <td>
                          <Pill label={b.result?.status ?? '—'} tone={toneForStatus(b.result?.status)} />
                        </td>
                        <td>{fmtMoney(b.result?.net_pnl_usd ?? null)}</td>
                        <td className="mono">{b.result?.matched_market_title ?? b.result?.matched_market_id ?? '—'}</td>
                        <td className="mono">
                          {b.result?.entry_price_used ?? '—'} ({b.result?.price_quality ?? '—'})
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )
            )}
          </div>
        </Card>
      )}

      {tab === 'debug' && (
        <div className="grid2">
          <Card title="Issue Queue">
            {issues && !('error' in issues) ? (
              <div style={{ display: 'grid', gap: 12 }}>
                <div className="row" style={{ flexWrap: 'wrap' }}>
                  {Object.entries(issues.counts).map(([k, v]) => (
                    <Pill key={k} label={`${k}: ${v}`} tone={k === 'UPSTREAM_ERROR' || k === 'ERROR' ? 'bad' : k === 'PENDING' ? 'warn' : 'neutral'} />
                  ))}
                </div>
                <table className="table">
                  <thead>
                    <tr>
                      <th>Type</th>
                      <th>User</th>
                      <th>Platform</th>
                      <th>Time</th>
                      <th>Intent</th>
                    </tr>
                  </thead>
                  <tbody>
                    {issues.issues.slice(0, 50).map((it) => (
                      <tr key={it.issue_id}>
                        <td className="mono">{it.issue_type}</td>
                        <td className="mono">{it.call?.author ?? '—'}</td>
                        <td className="mono">{it.call?.platform ?? '—'}</td>
                        <td className="mono">{it.call?.timestamp_utc ? new Date(it.call.timestamp_utc).toLocaleString() : '—'}</td>
                        <td style={{ maxWidth: 420 }}>{it.call?.market_intent ?? '—'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                <Small className="muted">Showing first 50 issues. Use the Bets tab to filter by status.</Small>
              </div>
            ) : (
              <Small className="muted">No issues yet (or analysis not done).</Small>
            )}
          </Card>

          <Card title="Why calls get missed">
            <div style={{ display: 'grid', gap: 10 }}>
              <Small className="muted">
                Candidates are intentionally high-recall; misses usually show up as “URL but no candidate”, ambiguous market matches, or missing entry prices.
              </Small>
              <div className="grid2">
                <div>
                  <Small>Candidate reasons</Small>
                  <pre className="mono" style={{ whiteSpace: 'pre-wrap', margin: 0 }}>
                    {JSON.stringify(asRecord(asRecord(metrics?.candidates)?.candidate_reason_counts) ?? {}, null, 2)}
                  </pre>
                </div>
                <div>
                  <Small>Analysis breakdown</Small>
                  <pre className="mono" style={{ whiteSpace: 'pre-wrap', margin: 0 }}>
                    {JSON.stringify(asRecord(asRecord(metrics?.analysis)?.status_counts) ?? {}, null, 2)}
                  </pre>
                </div>
              </div>
            </div>
          </Card>
        </div>
      )}
    </div>
  );
}
