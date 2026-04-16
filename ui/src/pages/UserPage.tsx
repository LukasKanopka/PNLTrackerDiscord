import { useEffect, useState } from 'react';
import { Link, useParams } from 'react-router-dom';
import { Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import { apiGet } from '../api/client';
import type { BetRow } from '../api/types';
import { Card, Pill, Small } from '../components/Ui';

type UserDetailResponse =
  | {
      run_id: string;
      status: string | null;
      author: string;
      user: {
        author: string;
        bets: number;
        wins: number;
        win_rate: number | null;
        net_pnl_usd: number;
        avg_pnl_per_bet: number | null;
        median_pnl_usd: number | null;
        profit_factor: number | null;
        profit_factor_is_infinite?: boolean;
        avg_roi: number | null;
        max_drawdown_usd: number;
      } | null;
      equity_curve: Array<{ timestamp_utc: string; net_pnl_usd: number; cum_net_pnl_usd: number }>;
      total: number;
      limit: number;
      offset: number;
      bets: BetRow[];
    }
  | { error: string };

function fmtMoney(x: number | null | undefined) {
  if (x === null || x === undefined) return '—';
  return new Intl.NumberFormat(undefined, { style: 'currency', currency: 'USD', maximumFractionDigits: 2 }).format(x);
}

function fmtAxisTime(ts: unknown) {
  try {
    const d = new Date(String(ts));
    return d.toLocaleString(undefined, { month: 'short', day: '2-digit', hour: '2-digit', minute: '2-digit' });
  } catch {
    return String(ts ?? '');
  }
}

export function UserPage() {
  const { runId, author } = useParams();
  const [data, setData] = useState<UserDetailResponse | null>(null);

  useEffect(() => {
    (async () => {
      if (!runId || !author) return;
      setData(await apiGet<UserDetailResponse>(`/v1/runs/${runId}/users/${encodeURIComponent(author)}`));
    })();
  }, [runId, author]);

  if (!runId || !author) return null;
  if (!data) {
    return (
      <Card title="Loading…">
        <Small className="muted">Fetching user details…</Small>
      </Card>
    );
  }
  if ('error' in data) {
    return (
      <Card title="Error">
        <Small className="muted">{data.error}</Small>
      </Card>
    );
  }

  const u = data.user;
  return (
    <div style={{ display: 'grid', gap: 14 }}>
      <Card
        title={`User: ${author}`}
        right={
          <div className="row">
            <Pill label={data.status ?? '—'} tone={(data.status || '').toUpperCase() === 'DONE' ? 'good' : 'neutral'} />
            <Link className="btn btn-ghost" to={`/runs/${runId}`}>
              Back
            </Link>
          </div>
        }
      >
        {u ? (
          <div className="grid3">
            <div>
              <Small>Net P&amp;L</Small>
              <div style={{ fontSize: 22, fontWeight: 700 }}>{fmtMoney(u.net_pnl_usd)}</div>
            </div>
            <div>
              <Small>Win rate / Bets</Small>
              <div style={{ fontSize: 22, fontWeight: 700 }}>
                {u.win_rate === null ? '—' : `${Math.round(u.win_rate * 100)}%`} · {u.bets}
              </div>
            </div>
            <div>
              <Small>Max drawdown</Small>
              <div style={{ fontSize: 18, fontWeight: 650 }}>{fmtMoney(u.max_drawdown_usd)}</div>
            </div>
          </div>
        ) : (
          <Small className="muted">No resolved bets for this user.</Small>
        )}
      </Card>

      <Card title="Equity Curve">
        <div style={{ height: 240 }}>
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={data.equity_curve}>
              <XAxis
                dataKey="timestamp_utc"
                tickFormatter={fmtAxisTime}
                minTickGap={28}
                interval="preserveStartEnd"
                tick={{ fill: 'rgba(250,250,250,0.65)', fontSize: 11 }}
                axisLine={{ stroke: 'rgba(255,255,255,0.10)' }}
                tickLine={{ stroke: 'rgba(255,255,255,0.10)' }}
              />
              <YAxis />
              <Tooltip
                labelFormatter={(l) => new Date(String(l)).toLocaleString()}
                formatter={(v) => [fmtMoney(Number(v)), 'Cum P&L']}
              />
              <Line type="monotone" dataKey="cum_net_pnl_usd" stroke="#34d399" dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </Card>

      <Card title="Bets">
        {Array.isArray(data.bets) ? (
          <table className="table">
            <thead>
              <tr>
                <th>Time</th>
                <th>Platform</th>
                <th>Side</th>
                <th>Units</th>
                <th>Notional</th>
                <th>Status</th>
                <th>Net P&amp;L</th>
                <th>Market</th>
              </tr>
            </thead>
            <tbody>
              {data.bets.map((b) => (
                <tr key={b.call_id}>
                  <td className="mono">{new Date(b.call.timestamp_utc).toLocaleString()}</td>
                  <td className="mono">{b.call.platform}</td>
                  <td className="mono">{b.call.position_direction}</td>
                  <td className="mono">{b.call.bet_size_units}</td>
                  <td className="mono">{b.result?.contracts != null ? fmtMoney(b.result.contracts) : '—'}</td>
                  <td className="mono">{b.result?.status ?? '—'}</td>
                  <td>{fmtMoney(b.result?.net_pnl_usd ?? null)}</td>
                  <td className="mono">{b.result?.matched_market_title ?? b.result?.matched_market_id ?? '—'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <Small className="muted">No bets found.</Small>
        )}
      </Card>
    </div>
  );
}
