// Shared helpers for the k6 profiles: admission tokens, the buyer journey, the browse request and
// the custom metrics the thresholds are written against.
import http from 'k6/http';
import crypto from 'k6/crypto';
import encoding from 'k6/encoding';
import exec from 'k6/execution';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

export const BASE_URL = __ENV.BASE_URL || 'http://127.0.0.1:8080';
export const DROP_ID = __ENV.DROP_ID || 'drop-1';
const KEY = __ENV.ADMISSION_TOKEN_KEY;
const RUN_ID = __ENV.RUN_ID || `${Date.now()}`;

// --- metrics -------------------------------------------------------------------------------

// SLI: an admitted reserve request is good if it returns a valid outcome (201 held, 200 already
// held, 410 sold out). 5xx, timeouts and connection errors are bad (docs/07-testing-and-observability.md, section 5).
export const reservationAvailability = new Rate('reservation_availability');
export const reserveLatency = new Trend('reserve_latency', true);
export const reserveHeld = new Counter('reserve_held');
export const reserveSoldOut = new Counter('reserve_sold_out');
export const reserveShed = new Counter('reserve_shed'); // 503 + Retry-After from load shedding
// A holder's Pay request is accepted if it gets 202 within 3 tries (same Idempotency-Key, following
// Retry-After), like the browser does (docs/06-scalability-and-fault-tolerance.md, section 4).
export const paymentAccepted = new Rate('payment_accepted');
export const paymentShed = new Counter('payment_shed'); // 503/409 + Retry-After, retried
export const paymentResolved = new Rate('payment_resolved_10s');
export const paymentPaid = new Counter('payment_paid');
export const paymentFailed = new Counter('payment_failed');

// --- admission tokens --------------------------------------------------------------------------

// Mirrors internal/admission: "<base64url(json)>.<base64url(hmac_sha256(key, payload))>".
// In a real drop the waiting room issues these; the load test plays that role.
export function admissionToken(userId) {
  if (!KEY) {
    exec.test.abort('ADMISSION_TOKEN_KEY is not set (source .env first)');
  }
  const claims = JSON.stringify({ u: userId, d: DROP_ID, exp: Math.floor(Date.now() / 1000) + 600 });
  const payload = encoding.b64encode(claims, 'rawurl');
  const sig = encoding.b64encode(crypto.hmac('sha256', KEY, payload, 'binary'), 'rawurl');
  return `${payload}.${sig}`;
}

export function uniqueUser() {
  return `lt-${RUN_ID}-${exec.scenario.name}-${exec.scenario.iterationInTest}`;
}

function idemKey() {
  return `${RUN_ID}-${exec.vu.idInTest}-${exec.vu.iterationInInstance}-${Math.random().toString(36).slice(2)}`;
}

// --- requests ------------------------------------------------------------------------------

// Browsing: the availability read that makes up ~98% of drop traffic (50:1 read:write). In
// production the CDN answers almost all of these; here they hit the service's 500 ms cache.
export function browse() {
  const res = http.get(`${BASE_URL}/v1/drops/${DROP_ID}/availability`, { tags: { name: 'availability' } });
  check(res, { 'availability 200': (r) => r.status === 200 });
}

// The buyer journey for one admitted user: reserve, pay, wait for the result.
export function buyer() {
  const user = uniqueUser();
  const token = admissionToken(user);

  // Reserve, retrying like the client SDK: same Idempotency-Key, honour Retry-After, max 3 tries.
  const key = idemKey();
  let res;
  for (let attempt = 0; attempt < 3; attempt++) {
    res = http.post(`${BASE_URL}/v1/drops/${DROP_ID}/reservations`, null, {
      headers: { 'X-Admission-Token': token, 'Idempotency-Key': key },
      tags: { name: 'reserve' },
      timeout: '2s',
    });
    reserveLatency.add(res.timings.duration);
    if (res.status === 503 || res.status === 409) {
      reserveShed.add(1);
      sleep(Math.random() * Number(res.headers['Retry-After'] || 1)); // full jitter
      continue;
    }
    break;
  }

  const good = res.status === 201 || res.status === 200 || res.status === 410;
  reservationAvailability.add(good);
  check(res, { 'reserve: valid outcome': () => good });
  if (res.status === 410) {
    reserveSoldOut.add(1);
    return;
  }
  if (res.status !== 201) {
    return;
  }
  reserveHeld.add(1);
  const reservationId = res.json('reservation_id');

  // A real user takes a few seconds to enter payment details.
  sleep(1 + Math.random() * 2);

  // Pay, retrying like the client SDK: same Idempotency-Key, honour Retry-After, max 3 tries.
  const payKey = idemKey();
  let pay;
  for (let attempt = 0; attempt < 3; attempt++) {
    pay = http.post(`${BASE_URL}/v1/reservations/${reservationId}/payment`,
      JSON.stringify({ payment_token: 'tok_ok' }), {
        headers: { 'X-Admission-Token': token, 'Idempotency-Key': payKey, 'Content-Type': 'application/json' },
        tags: { name: 'payment' },
        timeout: '3s',
      });
    if (pay.status === 503 || pay.status === 409 || pay.status === 0) {
      paymentShed.add(1);
      sleep(Math.random() * Number(pay.headers['Retry-After'] || 1)); // full jitter
      continue;
    }
    break;
  }
  paymentAccepted.add(pay.status === 202);
  check(pay, { 'payment accepted (202)': (r) => r.status === 202 });
  if (pay.status !== 202) {
    return;
  }

  // Poll for the result every 500 ms for up to 10 s (the client would use server-sent events).
  const started = Date.now();
  while (Date.now() - started < 10000) {
    sleep(0.5);
    const st = http.get(`${BASE_URL}/v1/reservations/${reservationId}`, {
      headers: { 'X-Admission-Token': token },
      tags: { name: 'status' },
    });
    const state = st.status === 200 ? st.json('state') : '';
    if (state === 'PAID' || state === 'FAILED') {
      paymentResolved.add(true);
      (state === 'PAID' ? paymentPaid : paymentFailed).add(1);
      return;
    }
  }
  paymentResolved.add(false);
}

// Thresholds shared by every profile. The run fails if any is breached.
export const baseThresholds = {
  // Reservation path SLO: p99 < 500 ms for admitted users.
  'http_req_duration{name:reserve}': ['p(99)<500'],
  // Reservation availability SLO, measured per request.
  reservation_availability: ['rate>0.9995'],
  // Holders' Pay requests are accepted (202) after client retries.
  payment_accepted: ['rate>0.999'],
  // 99% of payments resolved within 10 s.
  payment_resolved_10s: ['rate>0.99'],
  // Browsing must stay healthy while buyers compete.
  'http_req_failed{name:availability}': ['rate<0.001'],
};

// Reads the server's goroutine count and heap, for leak checks in soak runs.
export function runtimeStats() {
  const res = http.get(`${BASE_URL}/debug/vars`, { tags: { name: 'debug' } });
  if (res.status !== 200) {
    return null;
  }
  return { goroutines: res.json('goroutines'), heapAlloc: res.json('memstats.HeapAlloc') };
}
