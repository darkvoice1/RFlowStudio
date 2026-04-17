const DEFAULT_API_BASE_URL = '/api/v1';

function joinUrl(baseUrl, path) {
  const normalizedBase = baseUrl.replace(/\/+$/, '');
  const normalizedPath = path.replace(/^\/+/, '');
  return `${normalizedBase}/${normalizedPath}`;
}

function getApiBaseUrl() {
  const rawBaseUrl = import.meta.env.VITE_API_BASE_URL;
  if (typeof rawBaseUrl !== 'string' || !rawBaseUrl.trim()) {
    return DEFAULT_API_BASE_URL;
  }

  return rawBaseUrl.trim();
}

async function parseJsonResponse(response) {
  const contentType = response.headers.get('content-type') ?? '';
  if (!contentType.includes('application/json')) {
    return null;
  }

  return response.json();
}

export async function apiRequest(path, init = {}) {
  const response = await fetch(joinUrl(getApiBaseUrl(), path), init);
  const payload = await parseJsonResponse(response);

  if (!response.ok) {
    const detail =
      payload && typeof payload.detail === 'string'
        ? payload.detail
        : '请求失败，请稍后再试。';
    throw new Error(detail);
  }

  return payload;
}

export function buildApiUrl(path) {
  return joinUrl(getApiBaseUrl(), path);
}

export function formatDateTime(value) {
  if (!value) {
    return '未知时间';
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }

  return new Intl.DateTimeFormat('zh-CN', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  }).format(date);
}

export function formatFileSize(sizeBytes) {
  if (typeof sizeBytes !== 'number' || Number.isNaN(sizeBytes)) {
    return '未知大小';
  }

  if (sizeBytes < 1024) {
    return `${sizeBytes} B`;
  }

  const units = ['KB', 'MB', 'GB'];
  let value = sizeBytes / 1024;
  let unitIndex = 0;

  while (value >= 1024 && unitIndex < units.length - 1) {
    value /= 1024;
    unitIndex += 1;
  }

  return `${value.toFixed(value >= 10 ? 0 : 1)} ${units[unitIndex]}`;
}
