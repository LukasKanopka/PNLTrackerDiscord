export type RunListItem = {
  run_id: string;
  created_at: string | null;
  source_filename: string | null;
  export_timezone: string;
  verify_prices: boolean;
  upload_id?: string | null;
  status?: string | null;
  error_text?: string | null;
  parse_ms?: number | null;
  extract_ms?: number | null;
  analyze_ms?: number | null;
  message_count: number;
  call_count: number;
};

export type RunsResponse = { runs: RunListItem[] } | { error: string };

export type CreateRunResponse =
  | {
      run_id: string;
      upload_id: string;
      status: string;
      message_count: number;
      call_count: number;
      duration_ms: number;
    }
  | { error: string };

export type RunDetailResponse =
  | {
      run_id: string;
      created_at: string | null;
      source_filename: string | null;
      export_timezone: string;
      verify_prices: boolean;
      upload_id: string | null;
      status: string | null;
      error_text: string | null;
      parse_ms: number | null;
      extract_ms: number | null;
      analyze_ms: number | null;
      metrics: unknown;
      settings_snapshot: unknown;
      message_count: number | null;
      call_count: number | null;
    }
  | { error: string };

export type RunReportResponse =
  | {
      run_id: string;
      status: string | null;
      aggregate: {
        resolved_bets: number;
        win_rate: number | null;
        total_net_pnl_usd: number;
        total_net_units: number | null;
        max_drawdown_usd: number;
      };
      leaderboard: Array<{
        author: string;
        bets: number;
        wins: number;
        win_rate: number | null;
        net_pnl_usd: number;
        net_units: number | null;
      }>;
      user_stats: Array<{
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
      }>;
      equity_curve: Array<{ timestamp_utc: string; net_pnl_usd: number; cum_net_pnl_usd: number }>;
    }
  | { error: string };

export type BetRow = {
  call_id: number;
  call: {
    author: string;
    timestamp_utc: string;
    platform: string;
    market_intent: string;
    position_direction: string;
    quoted_price: number | null;
    bet_size_units: number;
    source_message_index: number | null;
    action: string | null;
    market_ref: unknown;
    extraction_confidence: number | null;
    evidence: string[] | null;
  };
  result:
    | {
        status: string;
        matched_market_id: string | null;
        matched_market_title: string | null;
        match_confidence: number | null;
        match_method: string | null;
        resolved_outcome: string | null;
        entry_price_used: number | null;
        price_source: string | null;
        price_quality: string | null;
        price_ts_utc: string | null;
        contracts: number | null;
        fees_usd: number | null;
        net_pnl_usd: number | null;
        roi: number | null;
        debug_json: unknown;
      }
    | null;
};

export type RunBetsResponse =
  | { run_id: string; status: string | null; total: number; limit: number; offset: number; bets: BetRow[] }
  | { error: string };

export type RunIssuesResponse =
  | {
      run_id: string;
      status: string | null;
      counts: Record<string, number>;
      issues: Array<{
        issue_id: number;
        issue_type: string;
        call_id: number | null;
        details_json: unknown;
        call:
          | {
              author: string;
              timestamp_utc: string;
              platform: string;
              market_intent: string;
              position_direction: string;
            }
          | null;
      }>;
    }
  | { error: string };
