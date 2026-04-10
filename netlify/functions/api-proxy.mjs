// Universal API proxy — injects API key server-side and forwards user JWT.
// Deploy identical copy to every consumer site.
// Browser calls: /.netlify/functions/api-proxy?path=/api/cs/routes&rep=Name
// Proxy forwards to gateway with X-API-Key + Authorization headers.

const GATEWAY = 'https://informativ-sales-api.netlify.app';

export async function handler(event) {
  // --- CORS preflight ---
  if (event.httpMethod === 'OPTIONS') {
    return {
      statusCode: 204,
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization',
        'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
        'Access-Control-Max-Age': '86400',
      },
      body: '',
    };
  }

  const params = event.queryStringParameters || {};
  const apiPath = params.path;
  if (!apiPath || !apiPath.startsWith('/api/')) {
    return { statusCode: 400, body: JSON.stringify({ error: 'Missing or invalid ?path= parameter' }) };
  }

  // Build gateway URL: path + remaining query params
  const forwardParams = { ...params };
  delete forwardParams.path;
  const qs = new URLSearchParams(forwardParams).toString();
  const url = GATEWAY + apiPath + (qs ? '?' + qs : '');

  const apiKey = process.env.GATEWAY_API_KEY;
  if (!apiKey) {
    return { statusCode: 500, body: JSON.stringify({ error: 'GATEWAY_API_KEY not configured' }) };
  }

  try {
    const fetchHeaders = {
      'Content-Type': 'application/json',
      'X-API-Key': apiKey,
    };

    // Forward user JWT if present (Netlify Identity session)
    const authHeader = (event.headers || {}).authorization || '';
    if (authHeader.startsWith('Bearer ')) {
      fetchHeaders['Authorization'] = authHeader;
    }

    const fetchOpts = {
      method: event.httpMethod,
      headers: fetchHeaders,
    };
    if (event.body && event.httpMethod !== 'GET') {
      fetchOpts.body = event.body;
    }

    const resp = await fetch(url, fetchOpts);
    const body = await resp.text();

    // Forward relevant headers
    const responseHeaders = {
      'Content-Type': resp.headers.get('content-type') || 'application/json',
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Headers': 'Content-Type, Authorization',
    };
    const etag = resp.headers.get('etag');
    if (etag) responseHeaders['ETag'] = etag;
    const cacheControl = resp.headers.get('cache-control');
    if (cacheControl) responseHeaders['Cache-Control'] = cacheControl;

    return {
      statusCode: resp.status,
      headers: responseHeaders,
      body,
    };
  } catch (err) {
    return {
      statusCode: 502,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ error: 'Gateway unreachable', detail: err.message }),
    };
  }
}
