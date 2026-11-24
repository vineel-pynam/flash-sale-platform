// Stress profile: raise buyer traffic step by step until the reservation SLO breaks, to find the
// real capacity knee of one instance (docs/07-testing-and-observability.md, section 1). The run aborts automatically when p99
// on the reserve path stays above 500 ms; the arrival rate at that moment is the knee.
//
//   k6 run -e START_RPS=100 -e MAX_RPS=20000 loadtest/stress.js
//
// Compare the knee with the per-pod target in docs/02-scale-and-numbers.md, point 8 (2,000 rps) to confirm or
// correct assumption A8.
import { buyer } from './lib.js';

const START = Number(__ENV.START_RPS || 100);
const MAX = Number(__ENV.MAX_RPS || 20000);
const STEP = __ENV.STEP || '2m';

// +10% every step, as in the plan.
function stages() {
  const out = [];
  for (let r = START; r <= MAX; r = Math.ceil(r * 1.1)) {
    out.push({ target: r, duration: STEP });
  }
  return out;
}

export const options = {
  scenarios: {
    buyers: {
      executor: 'ramping-arrival-rate',
      startRate: START,
      timeUnit: '1s',
      preAllocatedVUs: 500,
      maxVUs: 20000,
      stages: stages(),
    },
  },
  thresholds: {
    'http_req_duration{name:reserve}': [{ threshold: 'p(99)<500', abortOnFail: true, delayAbortEval: '30s' }],
    reservation_availability: [{ threshold: 'rate>0.999', abortOnFail: true, delayAbortEval: '30s' }],
  },
};

export default function () {
  buyer();
}
