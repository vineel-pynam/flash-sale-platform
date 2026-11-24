// Spike profile: the brief's scenario. Traffic jumps from normal to peak within 30 s, holds,
// then falls off sharply. Reads and buyer journeys are mixed at ~50:1.
//
// Rates are scaled by PEAK_RPS so the same script runs on a laptop and, distributed across k6
// runners in three regions, at the full 750K rps rehearsal (1.5x the 500K peak; docs/07-testing-and-observability.md, section 1).
//
//   k6 run loadtest/spike.js                                  # laptop default: 2,000 rps peak
//   k6 run -e PEAK_RPS=20000 -e HOLD=5m loadtest/spike.js
import { browse, buyer, baseThresholds } from './lib.js';

const PEAK = Number(__ENV.PEAK_RPS || 2000);
const NORMAL = Math.max(1, Math.round(PEAK / 250)); // the brief: 2K -> 500K is a 250x spike
const HOLD = __ENV.HOLD || '2m';
const BUYER_SHARE = 1 / 51; // 50:1 read:write

const rate = (total) => Math.max(1, Math.round(total));

function stages(total) {
  return [
    { target: rate(NORMAL * total), duration: '30s' }, // normal traffic
    { target: rate(PEAK * total), duration: '30s' },   // the spike: 250x within 30 s
    { target: rate(PEAK * total), duration: HOLD },    // the drop
    { target: rate(NORMAL * total), duration: '30s' }, // sold out: falls off sharply
    { target: rate(NORMAL * total), duration: '30s' },
  ];
}

export const options = {
  discardResponseBodies: false,
  scenarios: {
    browse: {
      executor: 'ramping-arrival-rate',
      exec: 'browseScenario',
      startRate: rate(NORMAL * (1 - BUYER_SHARE)),
      timeUnit: '1s',
      preAllocatedVUs: 200,
      maxVUs: 5000,
      stages: stages(1 - BUYER_SHARE),
    },
    buyers: {
      executor: 'ramping-arrival-rate',
      exec: 'buyerScenario',
      startRate: rate(NORMAL * BUYER_SHARE),
      timeUnit: '1s',
      preAllocatedVUs: 200,
      maxVUs: 5000, // buyer journeys last several seconds (payment + polling)
      stages: stages(BUYER_SHARE),
    },
  },
  thresholds: {
    ...baseThresholds,
    'http_req_duration{name:availability}': ['p(99)<200'],
  },
};

export function browseScenario() {
  browse();
}

export function buyerScenario() {
  buyer();
}
