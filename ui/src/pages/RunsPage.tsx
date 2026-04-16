import { useEffect, useMemo, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { apiGet, apiPostForm } from '../api/client';
import type { CreateRunResponse, RunsResponse, RunListItem } from '../api/types';
import { Dropzone } from '../components/Dropzone';
import { Button, Card, Pill, Select, Small } from '../components/Ui';

function fmtNum(x: number | null | undefined) {
  if (x === null || x === undefined) return '—';
  return new Intl.NumberFormat(undefined, { maximumFractionDigits: 2 }).format(x);
}

function toneForStatus(s: string | null | undefined) {
  const st = (s || '').toUpperCase();
  if (st === 'DONE') return 'good';
  if (st === 'ERROR') return 'bad';
  if (st === 'ANALYZING') return 'warn';
  return 'neutral';
}

export function RunsPage() {
  const nav = useNavigate();
  const [runs, setRuns] = useState<RunListItem[]>([]);
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const [file, setFile] = useState<File | null>(null);
  const [tz, setTz] = useState('America/New_York');
  const [verifyPrices, setVerifyPrices] = useState(true);
  const [autoAnalyze, setAutoAnalyze] = useState(true);

  async function refresh() {
    const res = await apiGet<RunsResponse>('/v1/runs');
    if ('error' in res) {
      setErr(res.error);
      setRuns([]);
      return;
    }
    setErr(null);
    setRuns(res.runs);
  }

  useEffect(() => {
    refresh();
    const t = window.setInterval(refresh, 4000);
    return () => window.clearInterval(t);
  }, []);

  const newestFirst = useMemo(() => runs.slice(), [runs]);

  async function submit() {
    if (!file) return;
    setBusy(true);
    setErr(null);
    try {
      const fd = new FormData();
      fd.append('file', file);
      fd.append('export_timezone', tz);
      fd.append('verify_prices', String(verifyPrices));
      fd.append('auto_analyze', String(autoAnalyze));
      const res = await apiPostForm<CreateRunResponse>('/v1/runs', fd);
      if ('error' in res) {
        setErr(res.error);
        return;
      }
      nav(`/runs/${res.run_id}`);
    } finally {
      setBusy(false);
    }
  }

  return (
    <div style={{ display: 'grid', gap: 14 }}>
      <div className="grid2">
        <Card title="New Upload" right={<Small>Stores raw file + run history</Small>}>
          <div style={{ display: 'grid', gap: 10 }}>
            <div>
              <Small>Discord export (.txt)</Small>
              <Dropzone value={file} onChange={setFile} accept=".txt,text/plain" />
            </div>
            <div className="grid2">
              <div>
                <Small>Export timezone</Small>
                <Select value={tz} onChange={(e) => setTz(e.target.value)}>
                  <option value="America/New_York">America/New_York</option>
                  <option value="America/Chicago">America/Chicago</option>
                  <option value="America/Denver">America/Denver</option>
                  <option value="America/Los_Angeles">America/Los_Angeles</option>
                  <option value="UTC">UTC</option>
                </Select>
              </div>
              <div>
                <Small>Verify prices</Small>
                <Select value={String(verifyPrices)} onChange={(e) => setVerifyPrices(e.target.value === 'true')}>
                  <option value="true">true</option>
                  <option value="false">false</option>
                </Select>
              </div>
            </div>
            <div className="grid2">
              <div>
                <Small>Auto analyze</Small>
                <Select value={String(autoAnalyze)} onChange={(e) => setAutoAnalyze(e.target.value === 'true')}>
                  <option value="true">true</option>
                  <option value="false">false</option>
                </Select>
              </div>
              <div className="row" style={{ justifyContent: 'flex-end' }}>
                <Button disabled={!file || busy} onClick={submit}>
                  {busy ? 'Uploading…' : 'Create Run'}
                </Button>
              </div>
            </div>
            {err && <div className="muted">Error: {err}</div>}
          </div>
        </Card>

        <Card title="What gets tracked">
          <div style={{ display: 'grid', gap: 10 }}>
            <Small>
              Funnel metrics: parse → candidate scan → extraction → match/resolution/pricing → P&amp;L, plus issue queues for
              unmatched/ambiguous/pending/missing-price.
            </Small>
            <Small>Tip: keep `DATABASE_URL` set and Postgres running for full history + queries.</Small>
          </div>
        </Card>
      </div>

      <Card title={`Runs (${newestFirst.length})`} right={<Button variant="ghost" onClick={refresh}>Refresh</Button>}>
        <table className="table">
          <thead>
            <tr>
              <th>Status</th>
              <th>Created</th>
              <th>File</th>
              <th>Msgs</th>
              <th>Calls</th>
              <th>Parse</th>
              <th>Extract</th>
              <th>Analyze</th>
            </tr>
          </thead>
          <tbody>
            {newestFirst.map((r) => (
              <tr key={r.run_id} style={{ cursor: 'pointer' }} onClick={() => nav(`/runs/${r.run_id}`)}>
                <td>
                  <Pill label={r.status ?? '—'} tone={toneForStatus(r.status)} />
                </td>
                <td className="mono">{r.created_at ? new Date(r.created_at).toLocaleString() : '—'}</td>
                <td className="mono">{r.source_filename ?? '—'}</td>
                <td>{fmtNum(r.message_count)}</td>
                <td>{fmtNum(r.call_count)}</td>
                <td>{r.parse_ms ? `${r.parse_ms}ms` : '—'}</td>
                <td>{r.extract_ms ? `${r.extract_ms}ms` : '—'}</td>
                <td>{r.analyze_ms ? `${r.analyze_ms}ms` : '—'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </Card>
    </div>
  );
}
