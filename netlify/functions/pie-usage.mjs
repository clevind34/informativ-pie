/**
 * PIE Dashboard Usage Tracker — Netlify Serverless Function
 *
 * Logs usage events from the Prospecting Intelligence Engine dashboard.
 * GitHub-persisted to pie-usage-log.json in the chuck-sales-assistant repo.
 *
 * Events tracked:
 * - page_load: Rep opened the dashboard
 * - tab_switch: Rep navigated to a tab
 * - detail_view: Rep opened a prospect/cross-sell detail panel
 * - pricing_generated: Rep generated pricing for an account
 * - disposition_submitted: Rep submitted feedback
 * - price_sheet_generated: Rep generated an external price sheet
 * - chuck_launched: Rep opened Chuck AI panel
 *
 * POST body: { rep, event_type, metadata, timestamp }
 * GET: Returns full usage log (for Usage Analytics tab)
 * GET ?summary=true: Returns aggregated per-rep summary
 */

const GITHUB_OWNER = 'clevind34';
const GITHUB_REPO = 'chuck-sales-assistant';
const FILE_PATH = 'pie-usage-log.json';
const BRANCH = 'main';
const MAX_EVENTS = 10000; // Rolling window — trim oldest beyond this

export async function handler(event) {
    const headers = {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'POST, GET, OPTIONS'
    };

    if (event.httpMethod === 'OPTIONS') {
        return { statusCode: 204, headers };
    }

    const token = process.env.GITHUB_TOKEN;

    if (event.httpMethod === 'GET') {
        try {
            if (!token) {
                return { statusCode: 200, headers, body: JSON.stringify({ events: [], warning: 'GITHUB_TOKEN not configured' }) };
            }
            const data = await fetchFileFromGitHub(token);
            const log = data.content;
            const queryParams = event.queryStringParameters || {};

            if (queryParams.summary === 'true') {
                // Return aggregated per-rep summary
                const summary = buildSummary(log.events || []);
                return { statusCode: 200, headers, body: JSON.stringify(summary) };
            }

            // Optional: filter by rep
            let events = log.events || [];
            if (queryParams.rep) {
                events = events.filter(e => e.rep === queryParams.rep);
            }
            // Optional: filter by days back
            if (queryParams.days) {
                const cutoff = new Date();
                cutoff.setDate(cutoff.getDate() - parseInt(queryParams.days));
                const cutoffISO = cutoff.toISOString();
                events = events.filter(e => e.timestamp >= cutoffISO);
            }

            return { statusCode: 200, headers, body: JSON.stringify({ events, total: events.length, last_updated: log.last_updated }) };
        } catch (err) {
            // File doesn't exist yet
            return { statusCode: 200, headers, body: JSON.stringify({ events: [], total: 0 }) };
        }
    }

    if (event.httpMethod === 'POST') {
        try {
            const body = JSON.parse(event.body || '{}');
            const { rep, event_type, metadata } = body;

            if (!rep || !event_type) {
                return { statusCode: 400, headers, body: JSON.stringify({ error: 'rep and event_type required' }) };
            }

            if (!token) {
                return { statusCode: 200, headers, body: JSON.stringify({ warning: 'GITHUB_TOKEN not configured, event not persisted' }) };
            }

            // Fetch current log
            let log = { events: [], schema_version: '1.0' };
            let sha = null;
            try {
                const data = await fetchFileFromGitHub(token);
                log = data.content;
                sha = data.sha;
            } catch (e) {
                // File doesn't exist — will create
            }

            if (!log.events) log.events = [];

            // Add new event
            const newEvent = {
                rep,
                event_type,
                metadata: metadata || {},
                timestamp: body.timestamp || new Date().toISOString()
            };
            log.events.push(newEvent);

            // Trim to max rolling window
            if (log.events.length > MAX_EVENTS) {
                log.events = log.events.slice(log.events.length - MAX_EVENTS);
            }

            log.last_updated = new Date().toISOString();
            log.schema_version = '1.0';

            // Commit back to GitHub
            await commitFileToGitHub(token, log, sha);

            return { statusCode: 200, headers, body: JSON.stringify({ success: true, total_events: log.events.length }) };
        } catch (err) {
            return { statusCode: 500, headers, body: JSON.stringify({ error: err.message }) };
        }
    }

    return { statusCode: 405, headers, body: JSON.stringify({ error: 'Method not allowed' }) };
}

function buildSummary(events) {
    const reps = {};
    const eventTypes = {};
    const dailyActivity = {};

    events.forEach(e => {
        // Per-rep summary
        if (!reps[e.rep]) {
            reps[e.rep] = { total_events: 0, last_active: null, event_types: {}, sessions: 0, first_seen: e.timestamp };
        }
        const r = reps[e.rep];
        r.total_events++;
        r.event_types[e.event_type] = (r.event_types[e.event_type] || 0) + 1;
        if (!r.last_active || e.timestamp > r.last_active) r.last_active = e.timestamp;
        if (e.event_type === 'page_load') r.sessions++;

        // Global event type counts
        eventTypes[e.event_type] = (eventTypes[e.event_type] || 0) + 1;

        // Daily activity
        const day = e.timestamp ? e.timestamp.split('T')[0] : 'unknown';
        if (!dailyActivity[day]) dailyActivity[day] = { events: 0, unique_reps: new Set() };
        dailyActivity[day].events++;
        dailyActivity[day].unique_reps.add(e.rep);
    });

    // Convert Sets to counts for JSON serialization
    const daily = {};
    Object.entries(dailyActivity).forEach(([day, d]) => {
        daily[day] = { events: d.events, unique_reps: d.unique_reps.size };
    });

    return {
        total_events: events.length,
        unique_reps: Object.keys(reps).length,
        reps,
        event_types: eventTypes,
        daily_activity: daily,
        generated_at: new Date().toISOString()
    };
}

async function fetchFileFromGitHub(token) {
    const url = `https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/contents/${FILE_PATH}?ref=${BRANCH}`;
    const resp = await fetch(url, {
        headers: { 'Authorization': `token ${token}`, 'Accept': 'application/vnd.github.v3+json' }
    });
    if (!resp.ok) throw new Error(`GitHub fetch failed: ${resp.status}`);
    const data = await resp.json();
    const content = JSON.parse(Buffer.from(data.content, 'base64').toString('utf-8'));
    return { content, sha: data.sha };
}

async function commitFileToGitHub(token, content, sha) {
    const url = `https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/contents/${FILE_PATH}`;
    const body = {
        message: `PIE usage log update — ${new Date().toISOString().split('T')[0]}`,
        content: Buffer.from(JSON.stringify(content, null, 2)).toString('base64'),
        branch: BRANCH
    };
    if (sha) body.sha = sha;
    const resp = await fetch(url, {
        method: 'PUT',
        headers: { 'Authorization': `token ${token}`, 'Content-Type': 'application/json', 'Accept': 'application/vnd.github.v3+json' },
        body: JSON.stringify(body)
    });
    if (!resp.ok) {
        const errText = await resp.text();
        throw new Error(`GitHub commit failed: ${resp.status} — ${errText}`);
    }
}
