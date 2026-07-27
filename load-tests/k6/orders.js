import http from 'k6/http';
import exec from 'k6/execution';
import { check, sleep } from 'k6';
import { Counter, Rate } from 'k6/metrics';

const profileName = __ENV.PROFILE || 'smoke';
const baseUrl = (__ENV.BASE_URL || '').replace(/\/$/, '');
const runId = __ENV.RUN_ID || `${Date.now()}`;

if (!baseUrl) {
  throw new Error('BASE_URL is required.');
}

const commonThresholds = {
  checks: ['rate>0.99'],
  http_req_failed: ['rate<0.01'],
  order_flow_success: ['rate>0.99'],
  'http_req_duration{endpoint:create-order}': ['p(95)<500', 'p(99)<1000'],
  'http_req_duration{endpoint:get-order}': ['p(95)<300', 'p(99)<750'],
};

const profiles = {
  smoke: {
    scenarios: {
      orders: {
        executor: 'constant-vus',
        vus: 1,
        duration: '10s',
        gracefulStop: '10s',
      },
    },
    thresholds: {
      checks: ['rate==1'],
      http_req_failed: ['rate==0'],
      order_flow_success: ['rate==1'],
      'http_req_duration{endpoint:create-order}': ['p(95)<750'],
      'http_req_duration{endpoint:get-order}': ['p(95)<500'],
    },
  },
  baseline: {
    scenarios: {
      orders: {
        executor: 'ramping-vus',
        startVUs: 0,
        stages: [
          { duration: '10s', target: 5 },
          { duration: '20s', target: 5 },
          { duration: '10s', target: 10 },
          { duration: '20s', target: 10 },
          { duration: '10s', target: 0 },
        ],
        gracefulRampDown: '10s',
        gracefulStop: '15s',
      },
    },
    thresholds: commonThresholds,
  },
  autoscale: {
    scenarios: {
      orders: {
        executor: 'ramping-vus',
        startVUs: 0,
        stages: [
          { duration: '15s', target: 75 },
          { duration: '60s', target: 75 },
          { duration: '15s', target: 0 },
        ],
        gracefulRampDown: '15s',
        gracefulStop: '20s',
      },
    },
    thresholds: {
      ...commonThresholds,
      'http_req_duration{endpoint:create-order}': ['p(95)<750', 'p(99)<1500'],
      'http_req_duration{endpoint:get-order}': ['p(95)<500', 'p(99)<1000'],
    },
  },
  resilience: {
    scenarios: {
      orders: {
        executor: 'constant-vus',
        vus: 5,
        duration: '75s',
        gracefulStop: '20s',
      },
    },
    thresholds: {
      checks: ['rate==1'],
      http_req_failed: ['rate==0'],
      order_flow_success: ['rate==1'],
      'http_req_duration{endpoint:create-order}': ['p(95)<750', 'p(99)<1500'],
      'http_req_duration{endpoint:get-order}': ['p(95)<500', 'p(99)<1000'],
    },
  },
  stress: {
    scenarios: {
      orders: {
        executor: 'ramping-vus',
        startVUs: 0,
        stages: [
          { duration: '15s', target: 10 },
          { duration: '30s', target: 20 },
          { duration: '30s', target: 30 },
          { duration: '15s', target: 0 },
        ],
        gracefulRampDown: '15s',
        gracefulStop: '20s',
      },
    },
    thresholds: {
      ...commonThresholds,
      checks: ['rate>0.98'],
      http_req_failed: ['rate<0.02'],
      order_flow_success: ['rate>0.98'],
      'http_req_duration{endpoint:create-order}': ['p(95)<750', 'p(99)<1500'],
      'http_req_duration{endpoint:get-order}': ['p(95)<500', 'p(99)<1000'],
    },
  },
  soak: {
    scenarios: {
      orders: {
        executor: 'constant-vus',
        vus: 5,
        duration: '5m',
        gracefulStop: '20s',
      },
    },
    thresholds: commonThresholds,
  },
  cache: {
    scenarios: {
      orders: {
        executor: 'constant-vus',
        vus: 10,
        duration: '30s',
        gracefulStop: '10s',
      },
    },
    thresholds: {
      checks: ['rate>0.99'],
      http_req_failed: ['rate<0.01'],
      cache_hit_rate: ['rate>0.90'],
      'http_req_duration{endpoint:get-order-cached}': ['p(95)<100', 'p(99)<250'],
    },
  },
};

if (!profiles[profileName]) {
  throw new Error(
    `Unsupported PROFILE "${profileName}". Use smoke, baseline, autoscale, resilience, stress, soak, or cache.`,
  );
}

export const options = {
  ...profiles[profileName],
  discardResponseBodies: false,
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
  tags: {
    workload: 'orders',
    profile: profileName,
  },
};

const createdOrders = new Counter('orders_created');
const instanceRequests = new Counter('api_instance_requests');
const successfulFlows = new Rate('order_flow_success');
const cacheHitRate = new Rate('cache_hit_rate');

const CACHE_SEED_POOL_SIZE = 15;

export function setup() {
  if (profileName !== 'cache') {
    return {};
  }

  const orderIds = [];
  for (let index = 0; index < CACHE_SEED_POOL_SIZE; index += 1) {
    const payload = JSON.stringify({
      customerId: `cache-seed-customer-${index}`,
      amount: 19.9,
      currency: 'BRL',
    });
    const response = http.post(`${baseUrl}/orders`, payload, {
      headers: {
        'Content-Type': 'application/json',
        'X-Correlation-ID': `k6-cache-seed-${runId}-${index}`,
      },
      timeout: '5s',
    });
    if (response.status === 201) {
      const id = response.json('id');
      if (id) {
        orderIds.push(id);
      }
    }
  }

  // Give the Worker time to process the seed orders (and invalidate any
  // not-yet-populated cache entries) before the cached-read workload starts.
  sleep(2);
  return { orderIds };
}

export default function (data) {
  if (profileName === 'cache') {
    cacheWorkload(data);
    return;
  }

  ordersWorkload();
}

function cacheWorkload(data) {
  const orderIds = (data && data.orderIds) || [];
  if (orderIds.length === 0) {
    sleep(1);
    return;
  }

  const orderId = orderIds[Math.floor(Math.random() * orderIds.length)];
  const response = http.get(`${baseUrl}/orders/${orderId}`, {
    tags: {
      endpoint: 'get-order-cached',
      name: 'GET /orders/:id (cached)',
    },
    timeout: '5s',
  });

  const succeeded = check(response, {
    'cached order read returns 200': (res) => res.status === 200,
  });

  cacheHitRate.add(response.headers['X-Cache'] === 'HIT');
  successfulFlows.add(succeeded);
  sleep(0.2 + Math.random() * 0.2);
}

function ordersWorkload() {
  const iterationId = `${runId}-${exec.vu.idInTest}-${exec.scenario.iterationInTest}`;
  const correlationId = `k6-${iterationId}`;
  const payload = JSON.stringify({
    customerId: `performance-customer-${iterationId}`,
    amount: 49.9,
    currency: 'BRL',
  });

  const createResponse = http.post(`${baseUrl}/orders`, payload, {
    headers: {
      'Content-Type': 'application/json',
      'X-Correlation-ID': correlationId,
    },
    tags: {
      endpoint: 'create-order',
      name: 'POST /orders',
    },
    timeout: '5s',
  });

  const responseCorrelationId = createResponse.headers['X-Correlation-Id'];
  const instanceId = createResponse.headers['X-Instance-Id'] || 'missing';
  let orderId = '';

  if (createResponse.status === 201) {
    try {
      orderId = createResponse.json('id') || '';
    } catch {
      orderId = '';
    }
  }

  const createSucceeded = check(createResponse, {
    'order creation returns 201': (response) => response.status === 201,
    'order creation returns an identifier': () => orderId.length > 0,
    'correlation header is preserved': () => responseCorrelationId === correlationId,
    'instance header identifies a pod': () => instanceId !== 'missing',
  });

  if (!createSucceeded) {
    successfulFlows.add(false);
    sleep(1);
    return;
  }

  createdOrders.add(1);
  instanceRequests.add(1, { instance_id: instanceId });

  const getResponse = http.get(`${baseUrl}/orders/${orderId}`, {
    tags: {
      endpoint: 'get-order',
      name: 'GET /orders/:id',
    },
    timeout: '5s',
  });

  const readSucceeded = check(getResponse, {
    'created order can be read': (response) => response.status === 200,
    'read order identifier matches': (response) => response.json('id') === orderId,
  });

  successfulFlows.add(readSucceeded);
  sleep(0.8 + Math.random() * 0.4);
}
