const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? "http://127.0.0.1:8000";

async function request(path, options = {}) {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    headers: { "Content-Type": "application/json", ...(options.headers ?? {}) },
    ...options,
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(payload.detail ?? "Request failed");
  }
  return payload;
}

export function getStatus() {
  return request("/settings/status");
}

export function saveFmp(apiKey) {
  return request("/settings/fmp", {
    method: "POST",
    body: JSON.stringify({ api_key: apiKey }),
  });
}

export function saveIg(values) {
  return request("/settings/ig", {
    method: "POST",
    body: JSON.stringify({
      api_key: values.apiKey,
      username: values.username,
      password: values.password,
      environment: "demo",
    }),
  });
}

export function getMarkets() {
  return request("/markets");
}

export function getMarketPlugins() {
  return request("/market-plugins");
}

export function installMarketPlugin(pluginId) {
  return request(`/market-plugins/${pluginId}/install`, {
    method: "POST",
  });
}

export function saveMarket(market) {
  return request("/markets", {
    method: "POST",
    body: JSON.stringify(market),
  });
}

export function createResearchRun(payload) {
  return request("/research/runs", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function getResearchRuns() {
  return request("/research/runs");
}

export function getResearchCandidates() {
  return request("/research/candidates");
}

export function saveResearchSchedule(payload) {
  return request("/research/schedules", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}
