/**
 * PIE Contact Edits — Netlify Serverless Function
 *
 * GitHub-persisted contact edits for PIE Dashboard prospects.
 * Reps can update name, title, email, phone on prospects.
 * Edits survive page refreshes and sync across all users.
 *
 * Flow:
 * 1. GET — Returns all saved contact edits
 * 2. POST — Accepts a single contact edit, merges with existing
 *
 * Data structure:
 * {
 *   contacts: { [prospect_id]: { contact_name, title, email, phone, edited_by, edited_at } },
 *   schema_version: "1.0",
 *   last_modified: ISO timestamp
 * }
 *
 * Hosted on: customer-intelligence-dashboard.netlify.app
 * Called cross-origin from: prospecting-intelligence.netlify.app
 */

const GITHUB_OWNER = 'clevind34';
const GITHUB_REPO = 'informativ-pie';
const FILE_PATH = 'pie-contact-edits.json';
const BRANCH = 'main';

export async function handler(event) {
    const headers = {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS'
    };

    // CORS preflight
    if (event.httpMethod === 'OPTIONS') {
        return { statusCode: 204, headers, body: '' };
    }

    const token = process.env.GITHUB_TOKEN;
    if (!token) {
        return {
            statusCode: 200,
            headers,
            body: JSON.stringify({ success: false, error: 'No GitHub token configured', contacts: {} })
        };
    }

    try {
        if (event.httpMethod === 'GET') {
            return await handleGet(token, headers);
        } else if (event.httpMethod === 'POST') {
            return await handlePost(event, token, headers);
        }

        return { statusCode: 405, headers, body: JSON.stringify({ error: 'Method not allowed' }) };
    } catch (err) {
        console.error('pie-contacts error:', err);
        return {
            statusCode: 200,
            headers,
            body: JSON.stringify({ success: false, error: err.message, contacts: {} })
        };
    }
}


// ============================================================
// GET — Return all saved contact edits
// ============================================================

async function handleGet(token, headers) {
    const { content } = await fetchFileFromGitHub(token);

    return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
            success: true,
            contacts: content.contacts || {},
            last_modified: content.last_modified || null,
            count: Object.keys(content.contacts || {}).length
        })
    };
}


// ============================================================
// POST — Save a contact edit
// ============================================================

async function handlePost(event, token, headers) {
    let body;
    try {
        body = JSON.parse(event.body);
    } catch {
        return {
            statusCode: 400,
            headers,
            body: JSON.stringify({ success: false, error: 'Invalid JSON body' })
        };
    }

    const { prospect_id, field, value, edited_by } = body;

    if (!prospect_id || !field) {
        return {
            statusCode: 400,
            headers,
            body: JSON.stringify({ success: false, error: 'Missing prospect_id or field' })
        };
    }

    // Allowed fields
    const ALLOWED_FIELDS = ['contact_name', 'title', 'email', 'phone'];
    if (!ALLOWED_FIELDS.includes(field)) {
        return {
            statusCode: 400,
            headers,
            body: JSON.stringify({ success: false, error: `Field not editable: ${field}` })
        };
    }

    // Fetch current state
    const { content, sha } = await fetchFileFromGitHub(token);

    // Merge edit
    if (!content.contacts) content.contacts = {};
    if (!content.contacts[prospect_id]) {
        content.contacts[prospect_id] = { created_at: new Date().toISOString() };
    }

    content.contacts[prospect_id][field] = value;
    content.contacts[prospect_id].edited_by = edited_by || 'unknown';
    content.contacts[prospect_id].edited_at = new Date().toISOString();
    content.last_modified = new Date().toISOString();

    // Commit
    const msg = `PIE contact edit: ${field} for ${prospect_id.substring(0, 8)}`;
    await commitFileToGitHub(token, content, sha, msg);

    return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
            success: true,
            prospect_id,
            field,
            value,
            last_modified: content.last_modified
        })
    };
}


// ============================================================
// GitHub API Helpers
// ============================================================

async function fetchFileFromGitHub(token) {
    const url = `https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/contents/${FILE_PATH}?ref=${BRANCH}`;

    const response = await fetch(url, {
        headers: {
            'Authorization': `token ${token}`,
            'Accept': 'application/vnd.github.v3+json',
            'User-Agent': 'PIE-Contact-Editor'
        }
    });

    if (response.status === 404) {
        return {
            content: {
                contacts: {},
                schema_version: '1.0',
                created_at: new Date().toISOString(),
                last_modified: new Date().toISOString()
            },
            sha: null
        };
    }

    if (!response.ok) {
        throw new Error(`GitHub fetch failed: ${response.status}`);
    }

    const data = await response.json();
    const contentStr = Buffer.from(data.content, 'base64').toString('utf-8');
    const content = JSON.parse(contentStr);

    return { content, sha: data.sha };
}

async function commitFileToGitHub(token, updatedContent, currentSha, commitMessage) {
    const url = `https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/contents/${FILE_PATH}`;

    const contentStr = JSON.stringify(updatedContent, null, 2);
    const contentBase64 = Buffer.from(contentStr).toString('base64');

    const payload = {
        message: commitMessage,
        content: contentBase64,
        branch: BRANCH
    };

    if (currentSha) {
        payload.sha = currentSha;
    }

    const response = await fetch(url, {
        method: 'PUT',
        headers: {
            'Authorization': `token ${token}`,
            'Accept': 'application/vnd.github.v3+json',
            'User-Agent': 'PIE-Contact-Editor',
            'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload)
    });

    if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        throw new Error(`GitHub commit failed: ${response.status} — ${errorData.message || response.statusText}`);
    }

    return await response.json();
}
