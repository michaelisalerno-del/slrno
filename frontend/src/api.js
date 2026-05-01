const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? "http://127.0.0.1:8000";

async function request(path, options = {}) {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    headers: { "Content-Type": "application/json", ...(options.headers ?? {}) },
    ...options,
  });
  const contentType = response.headers.get("content-type") ?? "";
  const isJson = contentType.includes("application/json");
  const payload = isJson ? await response.json().catch(() => ({})) : {};
  const fallbackText = isJson ? "" : await response.text().catch(() => "");
  if (!response.ok) {
    const fallback = fallbackText.replace(/<[^>]*>/g, " ").replace(/\s+/g, " ").trim();
    const detail = payload.detail ?? fallback.slice(0, 180);
    throw new Error(detail || `Request failed (${response.status})`);
  }
  if (!isJson) {
    throw new Error(`API returned non-JSON for ${path}`);
  }
  return payload;
}

export function getStatus() {
  return request("/settings/status");
}

export function getCockpitSummary() {
  return request("/cockpit/summary");
}

export function getResearchSummary(limit = 80) {
  return request(`/research/summary?limit=${limit}`);
}

export function getBacktestsSummary(includeArchived = false) {
  return request(`/backtests/summary?include_archived=${includeArchived}`);
}

export function getPaperSummary() {
  return request("/paper/summary");
}

export function getBrokerSummary() {
  return request("/broker/summary");
}

export function previewBrokerOrder(payload) {
  return request("/broker/order-preview", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function getRiskSummary() {
  return request("/risk/summary");
}

export function getSettingsSummary() {
  return request("/settings/summary");
}

export function saveEodhd(apiToken) {
  return request("/settings/eodhd", {
    method: "POST",
    body: JSON.stringify({ api_token: apiToken }),
  });
}

export function saveIg(values) {
  return request("/settings/ig", {
    method: "POST",
    body: JSON.stringify({
      api_key: values.apiKey,
      username: values.username,
      password: values.password,
      account_id: values.accountId,
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

export function getMarketDataCacheStatus() {
  return request("/market-data/cache");
}

export function pruneMarketDataCache() {
  return request("/market-data/cache/prune", {
    method: "POST",
    body: JSON.stringify({}),
  });
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

export function getResearchEngines() {
  return request("/research/engines");
}

export function getIgSpreadBetEngines() {
  return request("/ig/spread-bet/engines");
}

export function syncIgCosts(payload = {}) {
  return request("/ig/markets/sync-costs", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function getIgCostProfile(marketId) {
  return request(`/ig/markets/${marketId}/cost-profile`);
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

export function getResearchRun(runId) {
  return request(`/research/runs/${runId}`);
}

export function deleteResearchRun(runId) {
  return request(`/research/runs/${runId}`, {
    method: "DELETE",
  });
}

export function archiveResearchRun(runId) {
  return request(`/research/runs/${runId}/archive`, {
    method: "POST",
  });
}

export function researchRunExportUrl(runId, includeBars = true) {
  return `${API_BASE_URL}/research/runs/${runId}/export?include_bars=${includeBars}`;
}

export function getResearchTrials(runId) {
  return request(`/research/runs/${runId}/trials`);
}

export function getResearchPareto(runId) {
  return request(`/research/runs/${runId}/pareto`);
}

export function getResearchCandidates(limit = 80) {
  return request(`/research/candidates?limit=${limit}`);
}

export function getResearchCandidate(candidateId) {
  return request(`/research/candidates/${candidateId}`);
}

export function getResearchCritique() {
  return request("/research/critique");
}

export function saveResearchSchedule(payload) {
  return request("/research/schedules", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}
