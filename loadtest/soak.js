// Soak profile: moderate, steady load for a long time to expose slow failures: goroutine leaks,
// heap growth, connection pool drift, outbox backlog creep (docs/07-testing-and-observability.md, section 1).
//
//   k6 run -e RPS=300 -e DURATION=8h loadtest/soak.js     # full soak
//   k6 run loadtest/soak.js                               # 20 min default (minimum 2m)
//
// Sales are kept in Postgres, so a sold-out drop stays sold out. For a fresh run, start the server
// with a new DROP_ID (for example `make run DROP=soak-1`).
//
// Leak check: a monitor samples the server's goroutines and heap every 30 s, starting after a 1 min
// warm-up. Every sample is compared with the first one. Both are taken under the same load, so open
// keep-alive connections (one goroutine each) do not count as growth; a real leak keeps growing.
import { Trend } from 'k6/metrics';
import { browse, buyer, baseThresholds, runtimeStats } from './lib.js';

const RPS = Number(__ENV.RPS || 300);
const DURATION = __ENV.DURATION || '20m';
const BUYER_SHARE = 1 / 51;
const WARMUP_S = 60;

function seconds(d) {
  const m = /^(\d+)(s|m|h)$/.exec(d);
  if (!m) {
    throw new Error(`DURATION must look like 90s, 20m or 8h (got ${d})`);
  }
  return Number(m[1]) * { s: 1, m: 60, h: 3600 }[m[2]];
}

const TOTAL_S = seconds(DURATION);
if (TOTAL_S < 2 * WARMUP_S) {
  throw new Error('DURATION must be at least 2m (1 min warm-up + time to sample)');
}

const goroutineGrowth = new Trend('goroutine_growth'); // goroutines now minus the first sample
const heapGrowthMB = new Trend('heap_growth_mb');      // heap now minus the first sample, in MB

export const options = {
  scenarios: {
    browse: {
      executor: 'constant-arrival-rate', exec: 'browseScenario',
      rate: Math.max(1, Math.round(RPS * (1 - BUYER_SHARE))), timeUnit: '1s', duration: DURATION,
      preAllocatedVUs: 50, maxVUs: 1000,
    },
    buyers: {
      executor: 'constant-arrival-rate', exec: 'buyerScenario',
      rate: Math.max(1, Math.round(RPS * BUYER_SHARE)), timeUnit: '1s', duration: DURATION,
      preAllocatedVUs: 50, maxVUs: 1000,
    },
    monitor: {
      executor: 'constant-arrival-rate', exec: 'monitorScenario',
      rate: 2, timeUnit: '1m', startTime: `${WARMUP_S}s`, duration: `${TOTAL_S - WARMUP_S}s`,
      preAllocatedVUs: 1, maxVUs: 1, // one VU, so the first sample stays in this VU's memory
    },
  },
  thresholds: {
    ...baseThresholds,
    // Under the same load, goroutines and heap must stay flat. Small ups and downs are normal.
    goroutine_growth: ['max<100'],
    heap_growth_mb: ['max<64'],
  },
};

export function browseScenario() {
  browse();
}

export function buyerScenario() {
  buyer();
}

let first = null;

export function monitorScenario() {
  const now = runtimeStats();
  if (!now) {
    return;
  }
  if (first === null) {
    first = now;
    console.log(`baseline under load: goroutines ${now.goroutines}, heap ${(now.heapAlloc / 1e6).toFixed(1)}MB`);
    return;
  }
  goroutineGrowth.add(now.goroutines - first.goroutines);
  heapGrowthMB.add((now.heapAlloc - first.heapAlloc) / 1e6);
}
