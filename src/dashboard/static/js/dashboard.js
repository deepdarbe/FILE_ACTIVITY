/* FILE_ACTIVITY dashboard inline script — extracted from
 * index.html in #194 D3 (audit-2026-04-28.md). Loaded by a
 * sync <script src> tag at the same DOM position the inline
 * block previously occupied, so script-execution ordering vs
 * the surrounding HTML body is byte-identical. CI parses this
 * file with `node --check` via scripts/check_inline_js.py.
 */
// ═══════════════════════════════════════════════════
// GLOBAL STATE
// ═══════════════════════════════════════════════════
let sources = [];
let chartInstances = {};
let treemapData = null;
let treemapZoomPath = null;
let scanPollingInterval = null;
const API = '/api';

// ═══════════════════════════════════════════════════
// SECURITY — XSS escape helper (security audit 2026-04-28, H-1)
//
// Every untrusted leaf value spliced into an `innerHTML = `...${x}...``
// template literal MUST go through escapeHtml(). Filenames, owners,
// source.unc_path, etc. flow from the filesystem and could contain
// `<script>` or `<img onerror=...>` — we cannot trust them. Numeric ids,
// counts and known-internal tokens are exempt (use String(n) if needed).
//
// Function declaration (not expression) so it is hoisted within this
// script block — call sites further down the file are safe.
// ═══════════════════════════════════════════════════
function escapeHtml(s) {
    if (s == null) return '';
    return String(s).replace(/[&<>"']/g, c => ({
        '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
    }[c]));
}

// ═══════════════════════════════════════════════════
// UTILITY
// ═══════════════════════════════════════════════════
function downloadFile(url) {
    const a = document.createElement('a');
    a.href = url;
    a.download = '';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
}

async function api(path, opts = {}) {
    // Issue #79: track in-flight calls so the global progress widget can
    // expose "Loading X/Y endpoints..." as a breadcrumb.
    const callId = ++_apiCallCounter;
    currentApiCalls.add(callId);
    _apiCallsSeen = Math.max(_apiCallsSeen, currentApiCalls.size);
    _renderApiCallBreadcrumb();
    try {
        const r = await fetch(API + path, { headers: {'Content-Type':'application/json'}, ...opts });
        if (!r.ok) { const e = await r.json().catch(()=>({})); throw new Error(e.detail || `HTTP ${r.status}`); }
        return r.json();
    } catch(e) { if (!opts.silent) notify(e.message, 'error'); throw e; }
    finally {
        currentApiCalls.delete(callId);
        _renderApiCallBreadcrumb();
    }
}

function notify(msg, type='info') {
    const el = document.createElement('div');
    el.className = `notification ${type}`;
    // Security audit H-1: msg flows from server errors / filenames / etc.
    // Escape to prevent stored XSS through notification toasts.
    el.innerHTML = `<span>${{success:'✅',error:'❌',info:'ℹ️',warning:'⚠️'}[type]||'ℹ️'}</span> ${escapeHtml(msg)}`;
    el.onclick = () => el.remove();
    document.getElementById('notifications').appendChild(el);
    setTimeout(() => el.remove(), 5000);
}

// Bug 3 fix (Issue #82): wrapper that adds loading spinner, disabled state,
// timeout via AbortController, and uniform error toast for export/download buttons.
async function withButtonLoading(button, fn, timeoutMs = 60000) {
    if (!button || button.disabled) return;
    const original = button.innerHTML;
    button.disabled = true;
    button.innerHTML = '<span class="spinner"></span> Hazirlaniyor...';
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), timeoutMs);
    try {
        await fn(ctrl.signal);
    } catch (e) {
        if (e && e.name === 'AbortError') {
            notify('Islem zaman asimina ugradi (' + (timeoutMs/1000) + 'sn). Sunucu yogun olabilir.', 'error');
        } else {
            notify('Hata: ' + (e && e.message ? e.message : e), 'error');
        }
    } finally {
        clearTimeout(timer);
        button.disabled = false;
        button.innerHTML = original;
    }
}

// Issue #122: surface a hint when an XLSX export was either split across
// multiple sheets (>1M rows hit Excel's hard limit) or had to fall back to
// streaming CSV. Callers that fetch an export endpoint manually (Response
// object in hand) pass it through this helper before triggering the
// download, so the user sees a toast explaining what just happened.
function notifyExportFormat(response) {
    if (!response || !response.headers || typeof notify !== 'function') return;
    try {
        const ctype = (response.headers.get('Content-Type') || '').toLowerCase();
        const fallback = (response.headers.get('X-Format-Fallback') || '').toLowerCase();
        const sheetCount = parseInt(response.headers.get('X-Sheet-Count') || '0', 10);
        const cdisp = response.headers.get('Content-Disposition') || '';

        if (fallback === 'csv') {
            notify("Cok fazla satir, CSV formatina dusuldu", 'warn');
            return;
        }
        const isXlsx = ctype.indexOf('spreadsheetml') >= 0
            || ctype.indexOf('vnd.openxmlformats') >= 0;
        if (isXlsx && (sheetCount > 1 || /_part1\.xlsx/i.test(cdisp))) {
            const n = sheetCount > 1 ? sheetCount : 'birden fazla';
            notify(
                "Cok buyuk rapor: " + n + " sheet halinde indirildi, "
                + "Excel'de Index sheet'inden gezebilirsiniz",
                'info'
            );
        }
    } catch (_) { /* best-effort UI hint — never throw out of a download path */ }
}

// ═══════════════════════════════════════════════════
// Issue #79: Loading progress + ETA for slow dashboard pages
// ═══════════════════════════════════════════════════
const currentApiCalls = new Set();
let _apiCallCounter = 0;
let _apiCallsSeen = 0;
let _gpSlowTimer = null;
let _gpEtaTimer = null;
let _gpLastPage = null;
let _gpLastFn = null;

function _gpEl(id) { return document.getElementById(id); }

function _renderApiCallBreadcrumb() {
    const widget = _gpEl('global-progress');
    if (!widget || !widget.classList.contains('active')) return;
    const total = _apiCallsSeen || currentApiCalls.size;
    const inflight = currentApiCalls.size;
    const done = Math.max(0, total - inflight);
    const txt = total > 0 ? `${done}/${total} endpoint` : '';
    const callsEl = _gpEl('gp-calls');
    if (callsEl) callsEl.textContent = txt;
    widget.setAttribute('aria-label', txt
        ? `Yukleniyor: ${txt}`
        : 'Sayfa yukleniyor');
}

function getEtaForPage(pageName) {
    /* Read localStorage 'loadDurations.{pageName}' (last 10 ms values).
       <3 samples => null (caller shows indeterminate). Else => p50 in ms. */
    try {
        const raw = localStorage.getItem('loadDurations.' + pageName);
        if (!raw) return null;
        const arr = JSON.parse(raw);
        if (!Array.isArray(arr) || arr.length < 3) return null;
        const sorted = arr.slice().sort((a, b) => a - b);
        const mid = Math.floor(sorted.length / 2);
        const p50 = sorted.length % 2
            ? sorted[mid]
            : Math.round((sorted[mid - 1] + sorted[mid]) / 2);
        return p50;
    } catch (_) { return null; }
}

function _appendLoadDuration(pageName, ms) {
    try {
        const key = 'loadDurations.' + pageName;
        const raw = localStorage.getItem(key);
        const arr = raw ? (JSON.parse(raw) || []) : [];
        arr.push(Math.max(0, Math.round(ms)));
        // cap last 10
        const capped = arr.slice(-10);
        localStorage.setItem(key, JSON.stringify(capped));
    } catch (_) { /* quota / private mode - ignore */ }
}

function _gpHide() {
    const widget = _gpEl('global-progress');
    if (!widget) return;
    widget.classList.remove('active', 'indeterminate', 'error');
    const meta = _gpEl('gp-meta');
    if (meta) meta.style.display = 'none';
    const fill = _gpEl('gp-fill');
    if (fill) fill.style.width = '0%';
    const retry = _gpEl('gp-retry');
    if (retry) retry.style.display = 'none';
    if (_gpSlowTimer) { clearTimeout(_gpSlowTimer); _gpSlowTimer = null; }
    if (_gpEtaTimer) { clearInterval(_gpEtaTimer); _gpEtaTimer = null; }
}

function _gpShow(pageName, etaMs, started) {
    const widget = _gpEl('global-progress');
    if (!widget) return;
    widget.classList.add('active');
    widget.classList.remove('error');
    const meta = _gpEl('gp-meta');
    const status = _gpEl('gp-status');
    const etaEl = _gpEl('gp-eta');
    const retry = _gpEl('gp-retry');
    const fill = _gpEl('gp-fill');
    if (meta) meta.style.display = 'flex';
    if (retry) retry.style.display = 'none';
    if (status) {
        status.classList.remove('gp-error', 'gp-slow');
        status.textContent = 'yukleniyor...';
    }
    if (etaMs == null) {
        widget.classList.add('indeterminate');
        if (fill) fill.style.width = '0%';
        if (etaEl) etaEl.textContent = '';
    } else {
        widget.classList.remove('indeterminate');
        if (etaEl) etaEl.textContent = `Tahmini ~${Math.max(1, Math.round(etaMs / 1000))} sn`;
        // Drive percentage progress against the p50 estimate. Clamp at 95%
        // until fn resolves so we don't lie about completion.
        if (_gpEtaTimer) clearInterval(_gpEtaTimer);
        _gpEtaTimer = setInterval(() => {
            const elapsed = performance.now() - started;
            const pct = Math.min(95, (elapsed / etaMs) * 100);
            if (fill) fill.style.width = pct.toFixed(1) + '%';
            widget.setAttribute('aria-valuenow', String(Math.round(pct)));
        }, 200);
    }
}

function _gpMarkSlow(pageName, started) {
    const status = _gpEl('gp-status');
    if (status) { status.classList.add('gp-slow'); status.textContent = 'Yavas yanit (>30s)'; }
    const elapsed = Math.round((performance.now() - started) / 1000);
    notify(`Yavas yanit, ~${elapsed}s sonra tamamlanir`, 'warning');
}

async function loadWithProgress(pageName, fn) {
    /* Issue #79: wrap a page loader with a global progress bar + ETA.
       - Indeterminate on first call (<3 samples), percentage from p50 after.
       - On success: hide bar + record duration (cap last 10).
       - On error: keep bar visible in "error" state with Tekrar Dene retry.
       - >30s: emit "Yavas yanit" toast once.
       Issue #125: after 5s without resolve, drop a skeleton placeholder
       into the page's main container so the user sees structure instead
       of a blank rectangle. Skeleton MUST be cancellable on quick loads. */
    if (typeof fn !== 'function') return;
    _gpLastPage = pageName;
    _gpLastFn = fn;
    // Reset breadcrumb counters per page-load
    _apiCallsSeen = 0;
    currentApiCalls.clear();

    // Skeleton loading class on the page container (KPI shimmer for overview/insights etc.)
    const pageEl = document.getElementById('page-' + pageName);
    if (pageEl) pageEl.classList.add('loading');

    const eta = getEtaForPage(pageName);
    const started = performance.now();
    _gpShow(pageName, eta, started);
    _renderApiCallBreadcrumb();

    if (_gpSlowTimer) clearTimeout(_gpSlowTimer);
    _gpSlowTimer = setTimeout(() => _gpMarkSlow(pageName, started), 30000);

    // Issue #125: schedule a 5s skeleton fallback. If fn() resolves
    // earlier we cancel via clearTimeout — never flashes for fast loads.
    let _skelTimer = setTimeout(() => {
        try {
            const target = _findSkeletonContainer(pageName);
            if (target) renderPageSkeleton(target.id, target.type);
        } catch (_) { /* skeleton is purely visual — never escalate */ }
    }, 5000);

    try {
        const result = await fn();
        const duration = performance.now() - started;
        _appendLoadDuration(pageName, duration);
        if (pageEl) pageEl.classList.remove('loading');
        if (_skelTimer) { clearTimeout(_skelTimer); _skelTimer = null; }
        // Final flash to 100%
        const fill = _gpEl('gp-fill');
        const widget = _gpEl('global-progress');
        if (widget) widget.classList.remove('indeterminate');
        if (fill) fill.style.width = '100%';
        setTimeout(_gpHide, 220);
        return result;
    } catch (e) {
        if (pageEl) pageEl.classList.remove('loading');
        if (_skelTimer) { clearTimeout(_skelTimer); _skelTimer = null; }
        const widget = _gpEl('global-progress');
        const status = _gpEl('gp-status');
        const retry = _gpEl('gp-retry');
        const etaEl = _gpEl('gp-eta');
        if (widget) { widget.classList.add('error'); widget.classList.remove('indeterminate'); }
        if (_gpEtaTimer) { clearInterval(_gpEtaTimer); _gpEtaTimer = null; }
        if (_gpSlowTimer) { clearTimeout(_gpSlowTimer); _gpSlowTimer = null; }
        if (status) {
            status.classList.add('gp-error');
            const msg = (e && e.message ? e.message : String(e || 'bilinmeyen hata')).slice(0, 120);
            status.textContent = 'Yukleme basarisiz: ' + msg;
        }
        if (etaEl) etaEl.textContent = '';
        if (retry) retry.style.display = 'inline-block';
        throw e;
    }
}

function retryLastLoad() {
    if (_gpLastPage && _gpLastFn) {
        loadWithProgress(_gpLastPage, _gpLastFn).catch(() => { /* surfaced in widget */ });
    } else {
        _gpHide();
    }
}

// ═══════════════════════════════════════════════════
// Issue #125 — "Su an ne oluyor" status banner + page skeleton
// ═══════════════════════════════════════════════════

let _opsPollTimer = null;
let _opsLastSig = '';
const OPS_POLL_INTERVAL_MS = 5000;
// Per-source live scan op snapshots, keyed by source_id. Populated by
// ``pollOperations`` and consumed by ``pollScanProgress`` so the inline
// "MFT okunuyor (N kayit)" header and the bottom status bar can fall back
// to the ops registry's live count while the scan is still in the
// enumeration phase (when ``progress.file_count`` is still 0).
//   { [source_id]: { live_count: int, started_at: float, label: string } }
const _opsLiveBySource = {};

// Parse a scan op label like "MFT okuma: 1,651,187 kayit" or
// "Tarama: \\fs01\dept (123,456)" and return the embedded integer count,
// or null if no count is present. Tolerates the Turkish locale's "."
// thousand separator as well as "," (the backend currently uses ",").
function _parseOpsLiveCount(label) {
    if (!label || typeof label !== 'string') return null;
    const m = label.match(/([\d.,]+)\s*kayit/i) || label.match(/\(([\d.,]+)\)/);
    if (!m) return null;
    const digits = m[1].replace(/[.,\s]/g, '');
    if (!digits) return null;
    const n = parseInt(digits, 10);
    return Number.isFinite(n) && n > 0 ? n : null;
}

function _opsIcon(type) {
    return '🔄';
}

function _opsLabelMin(eta_seconds) {
    if (eta_seconds == null) return '';
    const mins = Math.max(1, Math.round(eta_seconds / 60));
    return ` · ~${mins} dk kaldi`;
}

function renderOpsBanner(operations) {
    const banner = document.getElementById('ops-banner');
    const text = document.getElementById('ops-banner-text');
    const dropdown = document.getElementById('ops-banner-dropdown');
    if (!banner || !text || !dropdown) return;

    const list = Array.isArray(operations) ? operations : [];
    if (list.length === 0) {
        banner.classList.remove('active', 'expanded');
        text.textContent = '';
        dropdown.innerHTML = '';
        return;
    }
    banner.classList.add('active');

    if (list.length === 1) {
        const op = list[0];
        text.textContent = `${_opsIcon(op.type)} ${op.label}${_opsLabelMin(op.eta_seconds)}`;
    } else {
        text.textContent = `${_opsIcon('multi')} ${list.length} islem devam ediyor (tikla)`;
    }

    // Always rebuild the dropdown so a click while polling shows fresh data.
    dropdown.innerHTML = list.map(op => {
        const meta = [];
        if (op.progress_pct != null) meta.push(`%${op.progress_pct}`);
        if (op.eta_seconds != null) meta.push(`~${Math.max(1, Math.round(op.eta_seconds/60))} dk kaldi`);
        if (op.metadata && op.metadata.source_id) meta.push(`source #${op.metadata.source_id}`);
        const metaTxt = meta.join(' · ');
        return `<div class="ops-banner-row">
            <div class="ops-row-label">${_opsIcon(op.type)} ${_escapeHtml(op.label || op.type)}</div>
            ${metaTxt ? `<div class="ops-row-meta">${_escapeHtml(metaTxt)}</div>` : ''}
        </div>`;
    }).join('');
}

function opsBannerToggle() {
    const banner = document.getElementById('ops-banner');
    if (!banner) return;
    // Single-op pills don't expand — the label already says everything.
    const dropdown = document.getElementById('ops-banner-dropdown');
    if (!dropdown || dropdown.children.length <= 1) return;
    banner.classList.toggle('expanded');
}

async function pollOperations() {
    try {
        const r = await fetch('/api/system/status', { cache: 'no-store' });
        if (!r.ok) return;
        const j = await r.json();
        const ops = (j && Array.isArray(j.operations)) ? j.operations : [];
        // Refresh per-source live snapshots BEFORE the no-op short-circuit
        // below — pollScanProgress reads from this map every 3s and the
        // banner-signature check would otherwise starve it whenever the
        // label digits change but the eta does not.
        const seenSources = new Set();
        for (const op of ops) {
            const sid = op && op.metadata && op.metadata.source_id;
            if (sid == null || op.type !== 'scan') continue;
            seenSources.add(sid);
            const live = _parseOpsLiveCount(op.label);
            _opsLiveBySource[sid] = {
                live_count: live,
                started_at: typeof op.started_at === 'number' ? op.started_at : null,
                label: op.label || '',
            };
        }
        // Drop entries for sources that no longer have an active scan op.
        for (const sid of Object.keys(_opsLiveBySource)) {
            if (!seenSources.has(Number(sid)) && !seenSources.has(sid)) {
                delete _opsLiveBySource[sid];
            }
        }
        // Avoid re-rendering when the snapshot is unchanged — keeps the
        // banner steady and avoids flicker. Cheap signature: type+id only.
        const sig = ops.map(o => `${o.op_id || ''}|${o.label || ''}|${o.eta_seconds || ''}`).join(';');
        if (sig === _opsLastSig) return;
        _opsLastSig = sig;
        renderOpsBanner(ops);
    } catch (_) {
        /* offline / 5xx — banner stays at last known state, no notify spam */
    }
}

function startOperationsPolling() {
    if (_opsPollTimer) return;
    pollOperations();
    _opsPollTimer = setInterval(pollOperations, OPS_POLL_INTERVAL_MS);
}

// ───────────────── Page skeleton ─────────────────

function renderPageSkeleton(containerId, type = 'table') {
    /* Inject placeholder shimmer markup into ``containerId``.
       - 'table': 10 row placeholders (column count comes from DOM if any).
       - 'chart': single 400x300 pulsing rectangle.
       - 'cards': 4 KPI card shimmers.
       Returns the host element so the caller can stash a reference. */
    const host = document.getElementById(containerId);
    if (!host) return null;
    let html = '';
    if (type === 'cards') {
        html = '<div class="skeleton-host"><div class="skeleton-cards">' +
            '<div class="skeleton-card"></div>'.repeat(4) +
            '</div></div>';
    } else if (type === 'chart') {
        html = '<div class="skeleton-host"><div class="skeleton-chart"></div></div>';
    } else {
        html = '<div class="skeleton-host">' +
            '<div class="skeleton-row"></div>'.repeat(10) +
            '</div>';
    }
    host.setAttribute('data-skeleton', '1');
    host.innerHTML = html;
    return host;
}

function _findSkeletonContainer(pageName) {
    /* Heuristic: find the main "data" container of a given page. We
       prefer common id conventions (#<name>-table, #<name>-chart,
       #page-<name> .table-host) and fall back to the page node itself. */
    const candidates = [
        `${pageName}-table`,
        `${pageName}-list`,
        `${pageName}-grid`,
        `${pageName}-container`,
        `${pageName}-content`,
    ];
    for (const id of candidates) {
        if (document.getElementById(id)) return { id, type: 'table' };
    }
    // chart-only pages
    const chartCandidates = [`${pageName}-chart`, `${pageName}-canvas`];
    for (const id of chartCandidates) {
        if (document.getElementById(id)) return { id, type: 'chart' };
    }
    // Fallback: page root
    const pageEl = document.getElementById('page-' + pageName);
    if (pageEl && !pageEl.id.startsWith('skeleton-')) {
        return { id: pageEl.id, type: 'cards' };
    }
    return null;
}

// Bug 3 fix: shared helper that fetches a URL as blob with abort signal,
// extracts filename from Content-Disposition, and triggers a download anchor.
async function fetchAndDownload(url, signal, fallbackName) {
    const resp = await fetch(url, { signal });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const blob = await resp.blob();
    const cd = resp.headers.get('Content-Disposition') || '';
    const m = cd.match(/filename="?([^";]+)"?/);
    const filename = m ? m[1] : fallbackName;
    const objUrl = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = objUrl; a.download = filename;
    document.body.appendChild(a); a.click();
    a.remove(); URL.revokeObjectURL(objUrl);
}

function formatSize(bytes) {
    if (!bytes || bytes === 0) return '0 B';
    const u = ['B','KB','MB','GB','TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(1024));
    return (bytes / Math.pow(1024, i)).toFixed(i > 0 ? 1 : 0) + ' ' + u[i];
}

function formatNum(n) { return n ? n.toLocaleString('tr-TR') : '0'; }

// Issue #194 update #6: defensive innerHTML setter. Customer prod
// (2026-04-29 00:55) hit "TypeError: Cannot set properties of null
// (setting 'innerHTML')" in loadDuplicates / loadGrowth / loadNaming
// when their browser served stale cached HTML against fresh JS — the
// IDs the JS targets weren't yet in the DOM the browser had cached.
// Rather than letting the loader crash and bubble up to a red toast,
// log a warning and continue. Used in loaders that touch summary
// cards which may not be present in older HTML versions.
function _setHtmlSafe(id, html) {
    const el = document.getElementById(id);
    if (el) {
        el.innerHTML = html;
    } else if (window && window.console) {
        console.warn('_setHtmlSafe: element not found:', id);
    }
}

function destroyChart(id) { if (chartInstances[id]) { chartInstances[id].destroy(); delete chartInstances[id]; } }

const COLORS = ['#3b82f6','#10b981','#f59e0b','#ef4444','#8b5cf6','#06b6d4','#ec4899','#f97316','#14b8a6','#a855f7','#6366f1','#84cc16','#e11d48','#0ea5e9','#d946ef','#facc15','#22d3ee','#fb923c','#4ade80','#c084fc'];

// Chart.js defaults
Chart.defaults.color = '#94a3b8';
Chart.defaults.borderColor = '#1e293b';
Chart.defaults.font.family = "'Segoe UI', sans-serif";

// ═══════════════════════════════════════════════════
// NAVIGATION
// ═══════════════════════════════════════════════════
function showPage(name) {
    // Issue #181 Track B2: tear down any running partial-data poll when
    // navigating away from a page that was mid-scan.
    if (typeof _psv2StopPoll === 'function' && _psv2PollPage && _psv2PollPage !== name) {
        _psv2StopPoll();
    }
    document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
    document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
    const page = document.getElementById('page-' + name);
    if (page) page.classList.add('active');
    // Find and activate nav item
    document.querySelectorAll('.nav-item').forEach(n => { if (n.getAttribute('onclick')?.includes(name)) n.classList.add('active'); });
    // Auto-load data
    const loaders = { overview: loadOverview, sources: loadSources, frequency: loadFrequency, types: loadTypes, sizes: loadSizes, users: loadUsers, anomalies: loadAnomalies, archive: loadArchive, 'archive-history': loadArchiveHistory, duplicates: loadDuplicates, growth: loadGrowth, naming: loadNaming, policies: loadPolicies, schedules: loadSchedules, treemap: loadTreemap, insights: loadInsights, sqlquery: sqlqInit, 'legal-holds': loadLegalHolds, standards: loadStandards, 'orphan-sids': loadOrphanSids, ransomware: loadRansomwareAlerts, acl: loadAclAnalyzer, 'extension-anomalies': loadExtensionAnomalies, 'image-duplicates': loadImageDuplicates, pii: loadPii, retention: loadRetention, syslog: loadSyslog, mcp: loadMcp, backups: loadBackups, operations: loadOperations, approvals: loadApprovalsAll, chargeback: loadChargeback, quarantine: loadQuarantine, forecast: loadForecast };
    // Issue #79: route through loadWithProgress so slow pages get a top
    // progress bar, ETA from p50 history, and a retry path on failure.
    if (loaders[name]) loadWithProgress(name, loaders[name]).catch(() => { /* widget surfaces error */ });
}

// ═══════════════════════════════════════════════════
// MODALS
// ═══════════════════════════════════════════════════
function openModal(id) { document.getElementById(id).classList.add('active'); }
function closeModal(id) { document.getElementById(id).classList.remove('active'); }
function openAddSourceModal() { openModal('modal-source'); }
function openAddPolicyModal() { openModal('modal-policy'); }
function openAddScheduleModal() { populateSourceSelect('sch-source'); openModal('modal-schedule'); }

// Close modals on overlay click
document.querySelectorAll('.modal-overlay').forEach(m => m.addEventListener('click', e => { if (e.target === m) m.classList.remove('active'); }));

// ═══════════════════════════════════════════════════
// SOURCES
// ═══════════════════════════════════════════════════
// Issue #177 — per-source trend cache for delta line in source cards.
// Keyed by source_id, populated lazily in loadSources().
const _sourceTrends = {};

async function loadSources() {
    sources = await api('/sources');
    populateAllSourceSelects();
    renderSources();
    // Issue #177 — fetch trend data for each source in parallel so the
    // delta line (Önceki tarama / Δ dosya) can be rendered. Errors are
    // swallowed — a missing trend just hides the delta line gracefully.
    if (sources.length > 0) {
        Promise.all(sources.map(s =>
            fetch(`/api/trend/${s.id}`, { cache: 'no-store' })
                .then(r => r.ok ? r.json() : null)
                .catch(() => null)
                .then(data => {
                    if (data) _sourceTrends[s.id] = data;
                })
        )).then(() => renderSources()).catch(() => {});
    }
}

function populateAllSourceSelects() {
    ['overview-source','freq-source','types-source','sizes-source','treemap-source','insights-source','ah-source','dup-source','growth-source','naming-source','orphan-sids-source','acl-source','ext-anomaly-source','pii-source','cb-source','fc-source'].forEach(id => populateSourceSelect(id));
    // Auto-select first source if only one
    if (sources.length === 1) {
        ['overview-source','freq-source','types-source','sizes-source','treemap-source','insights-source','ah-source','dup-source','growth-source','naming-source','orphan-sids-source','acl-source','ext-anomaly-source','pii-source','cb-source','fc-source'].forEach(id => {
            const sel = document.getElementById(id);
            if (sel && !sel.value) sel.value = sources[0].id;
        });
    }
}

function populateSourceSelect(id) {
    const sel = document.getElementById(id);
    if (!sel) return;
    const cur = sel.value;
    // Issue #81: PII findings and similar pages support "all sources"
    // (empty value); use a different placeholder there.
    const placeholder = (id === 'pii-source')
        ? '<option value="">Tum kaynaklar</option>'
        : '<option value="">Kaynak secin...</option>';
    sel.innerHTML = placeholder + sources.map(s => `<option value="${s.id}">${escapeHtml(s.name)}</option>`).join('');
    if (cur) sel.value = cur;
    else if (sources.length === 1 && id !== 'pii-source') sel.value = sources[0].id;
}

function renderSources() {
    const c = document.getElementById('source-list');
    if (!sources.length) { c.innerHTML = '<div class="empty-state"><div class="icon">📁</div><h3>Kaynak Bulunamadi</h3><p>"+ Kaynak Ekle" ile dosya paylasim kaynagi ekleyin.</p></div>'; return; }
    c.innerHTML = sources.map(s => {
        // Issue #177 — build delta line from cached trend data
        const trend = _sourceTrends[s.id];
        let deltaHtml = '';
        if (trend && trend.scans && trend.scans.length >= 2) {
            const sorted = trend.scans.slice().sort((a, b) => (a.started_at || '') < (b.started_at || '') ? -1 : 1);
            const prev = sorted[sorted.length - 2];
            const latest = sorted[sorted.length - 1];
            if (prev && prev.started_at && prev.total_files != null) {
                const prevDate = escapeHtml(prev.started_at.substring(0, 16).replace('T', ' '));
                const prevFiles = formatNum(prev.total_files || 0);
                let deltaStr = '';
                if (trend.growth && trend.growth.file_diff != null) {
                    const d = trend.growth.file_diff;
                    const sign = d >= 0 ? '+' : '';
                    deltaStr = ` <span style="color:${d >= 0 ? 'var(--success)' : 'var(--danger)'}">Δ ${sign}${formatNum(d)} dosya</span>`;
                }
                deltaHtml = `<div class="info-row" style="font-size:11px;color:var(--text-muted)"><span class="label">Önceki Tarama</span><span class="value">${prevDate} | ${prevFiles} dosya${deltaStr}</span></div>`;
            }
        }
        return `
        <div class="source-card">
            <div class="source-card-header">
                <div class="name">📁 ${escapeHtml(s.name)}</div>
                <span class="badge ${s.enabled ? 'badge-success' : 'badge-warning'}">${s.enabled ? 'Aktif' : 'Pasif'}</span>
            </div>
            <div class="source-card-body">
                <div class="info-row"><span class="label">Yol</span><span class="value">${escapeHtml(s.unc_path)}</span></div>
                <div class="info-row"><span class="label">Arsiv</span><span class="value">${escapeHtml(s.archive_dest || '-')}</span></div>
                <div class="info-row"><span class="label">Son Tarama</span><span class="value">${escapeHtml(s.last_scanned_at ? s.last_scanned_at.substring(0,19) : 'Hic')}</span></div>
                ${deltaHtml}
                <div class="info-row"><span class="label">Izleme</span><span class="value" id="watcher-status-${s.id}"><span class="watcher-dot inactive"></span> <span style="font-size:11px;color:var(--text-muted)">Durduruldu</span></span></div>
            </div>
            <div class="source-card-actions">
                <button class="btn btn-sm btn-primary" onclick="startScan(${s.id})">🔍 Tara</button>
                <button class="btn btn-sm btn-outline" onclick="stopScan(${s.id})">⏹ Taramayi Durdur</button>
                <button class="btn btn-sm btn-outline" onclick="testSource(${s.id})">🔌 Test</button>
                <button class="btn btn-sm btn-outline" onclick="exportReport(${s.id}, event)">📄 Rapor</button>
                <button class="btn btn-sm btn-success" onclick="startWatcher(${s.id})">▶ Izle</button>
                <button class="btn btn-sm btn-outline" onclick="stopWatcher(${s.id})">⏹ Izlemeyi Durdur</button>
                <button class="btn-export" onclick="exportXLS(${s.id}, event)">XLS</button>
                <button class="btn-export" style="background:var(--danger)" onclick="exportPDF(${s.id}, event)">PDF</button>
                <button class="btn btn-sm btn-danger" onclick="deleteSource(${s.id})">🗑️ Sil</button>
            </div>
        </div>
    `;
    }).join('');
    // Update watcher status for all sources
    sources.forEach(s => updateWatcherStatus(s.id));
}

async function addSource() {
    const name = document.getElementById('src-name').value.trim();
    const path = document.getElementById('src-path').value.trim();
    const archive = document.getElementById('src-archive').value.trim();
    if (!name || !path) { notify('Ad ve yol zorunlu!', 'warning'); return; }
    await api('/sources', { method: 'POST', body: JSON.stringify({ name, unc_path: path, archive_dest: archive || null }) });
    closeModal('modal-source');
    notify(`Kaynak eklendi: ${name}`, 'success');
    document.getElementById('src-name').value = '';
    document.getElementById('src-path').value = '';
    document.getElementById('src-archive').value = '';
    loadSources();
}

async function deleteSource(id) {
    if (!confirm('Bu kaynagi silmek istediginize emin misiniz?')) return;
    await api(`/sources/${id}`, { method: 'DELETE' });
    notify('Kaynak silindi', 'success');
    loadSources();
}

async function testSource(id) {
    notify('Baglanti test ediliyor...', 'info');
    const r = await api(`/sources/${id}/test`, { method: 'POST' });
    notify(r.message, r.success ? 'success' : 'error');
}

async function exportReport(id, event) {
    if (!id) { notify('Onceeerce kaynak secin', 'warning'); return; }
    const btn = event?.currentTarget;
    await withButtonLoading(btn, async (signal) => {
        await fetchAndDownload(`${API}/reports/export/${id}`, signal, `report_${id}.pdf`);
    });
}

// ═══════════════════════════════════════════════════
// SCAN WITH LIVE PROGRESS
// ═══════════════════════════════════════════════════
let activeScanSourceId = null;

async function startScan(sourceId) {
    activeScanSourceId = sourceId;
    const prog = document.getElementById('scan-progress');
    // Issue #177 — reset state classes and show scanning state
    prog.classList.remove('scan-completed', 'scan-failed', 'scan-cancelled', 'scan-idle');
    prog.classList.add('active', 'scan-scanning');

    // Reset progress
    document.getElementById('scan-progress-bar').style.width = '2%';
    document.getElementById('sp-files').textContent = '0';
    document.getElementById('sp-size').textContent = '0 B';
    document.getElementById('sp-speed').textContent = '0';
    document.getElementById('sp-errors').textContent = '0';
    document.getElementById('sp-dir').textContent = 'Başlatılıyor...';
    document.getElementById('scan-elapsed').textContent = '0s';
    const titleSpan = document.getElementById('scan-title-text');
    if (titleSpan) titleSpan.textContent = 'Tarama Devam Ediyor';
    const subtitle = document.getElementById('scan-progress-subtitle');
    if (subtitle) subtitle.textContent = '';
    // Reset edge-trigger so first completion fires toast
    pollScanProgress._lastSeenStatus = 'scanning';

    // Fire scan (returns immediately - runs in background)
    try {
        const result = await api(`/scan/${sourceId}`, { method: 'POST' });
        if (result.status === 'already_running') {
            notify('Bu kaynak zaten taraniyor!', 'warning');
        } else {
            notify('Tarama arka planda başlatıldı! Diğer sayfalara geçebilirsiniz.', 'success');
        }
    } catch(e) {
        notify(`Tarama başlatılamadı: ${e.message}`, 'error');
        prog.classList.remove('active');
        activeScanSourceId = null;
        return;
    }

    // Start polling progress (her saniye)
    if (scanPollingInterval) clearInterval(scanPollingInterval);
    scanPollingInterval = setInterval(() => pollScanProgress(sourceId), 3000);
}

// Issue #131 — kullanici tetikli tarama iptali. /api/scan/{id}/stop
// endpoint'i cancel_event'i ate eder, scan_run'i 'cancelled' olarak isaretler.
async function stopScan(sourceId) {
    if (!confirm('Aktif taramayi durdurmak istediginize emin misiniz? Kismi sonuclar kaydedilir.')) return;
    try {
        const result = await api(`/scan/${sourceId}/stop`, { method: 'POST' });
        if (result.cancelled) {
            const forced = result.forced ? ' (zorla)' : '';
            notify(`Tarama durduruldu${forced}: ${result.partial_files || 0} dosya kaydedildi`, 'info');
        } else {
            notify('Aktif tarama yok.', 'warning');
        }
    } catch(e) {
        notify(`Tarama durdurulamadi: ${e.message}`, 'error');
    }
}

async function pollScanProgress(sourceId) {
    try {
        const p = await api(`/scan/progress/${sourceId}`, { silent: true });

        // Issue #137 — prefer the live MFT-collection counter over the
        // (still-zero) DB file_count during the early "MFT okuma" phase
        // so the Sources page card and DOSYA KPI track the ops banner.
        // Falls back to file_count once the scanner starts iterating
        // batches and live_count plateaus / disappears.
        const displayCount = (p.live_count != null && p.live_count > (p.file_count || 0))
            ? p.live_count
            : (p.file_count || 0);

        // Issue #177 — edge-triggered toast: fire only on state transition.
        // lastSeenStatus is set once per pollScanProgress call-site so
        // repeated polls at the same state don't re-fire the toast.
        const previousStatus = pollScanProgress._lastSeenStatus;
        const currentStatus = p.status;

        // ── Helper: transition scan-progress to a terminal state ──────────
        function _applyTerminalState(stateClass, titleText, subtitleText, filesText, sizeText, speedText, dirText) {
            const prog = document.getElementById('scan-progress');
            if (!prog) return;
            prog.classList.remove('scan-scanning', 'scan-completed', 'scan-failed', 'scan-cancelled', 'scan-idle');
            prog.classList.add(stateClass);

            const titleSpan = document.getElementById('scan-title-text');
            if (titleSpan) titleSpan.textContent = titleText;

            const subtitle = document.getElementById('scan-progress-subtitle');
            if (subtitle) subtitle.textContent = subtitleText || '';

            document.getElementById('sp-files').textContent = filesText || '0';
            document.getElementById('sp-size').textContent = sizeText || '0 B';
            document.getElementById('sp-speed').textContent = speedText || '—';
            document.getElementById('sp-dir').textContent = dirText || '';

            // Persist last-known source so restart button works
            if (activeScanSourceId) {
                const btn = document.getElementById('scan-restart-btn');
                if (btn) btn.setAttribute('onclick', `startScan(${activeScanSourceId})`);
            }
        }

        // Tarama bitti mi?
        if (p.finished) {
            clearInterval(scanPollingInterval);
            scanPollingInterval = null;

            document.getElementById('scan-progress-bar').style.width = '100%';

            const finalFiles = formatNum(p.total_files || displayCount);
            const finalSize = p.total_size_formatted || formatSize(p.total_size);
            const finalSpeed = p.files_per_second ? (p.files_per_second + '/s') : '—';
            const finalErrors = String(p.errors || '0');
            const elapsed = p.elapsed || '';
            const finStatus = p.status || 'completed';

            // Build subtitle: "28-04-2026 18:31 | 33sn | 0 hata"
            const now = new Date();
            const pad = n => String(n).padStart(2, '0');
            const ts = `${pad(now.getDate())}-${pad(now.getMonth()+1)}-${now.getFullYear()} ${pad(now.getHours())}:${pad(now.getMinutes())}`;
            const subtitle = [ts, elapsed, `${p.errors || 0} hata`].filter(Boolean).join(' | ');

            document.getElementById('sp-errors').textContent = finalErrors;

            if (finStatus === 'cancelled') {
                _applyTerminalState('scan-cancelled',
                    'İptal edildi',
                    subtitle,
                    finalFiles, finalSize, finalSpeed, '—');
            } else if (finStatus === 'failed') {
                const errMsg = p.error || p.error_message || '';
                _applyTerminalState('scan-failed',
                    'Tarama başarısız',
                    errMsg ? `${subtitle} — ${errMsg}` : subtitle,
                    finalFiles, finalSize, finalSpeed, '—');
            } else {
                _applyTerminalState('scan-completed',
                    `Son tarama: ${ts}`,
                    `${elapsed ? elapsed + ' | ' : ''}${finalFiles} dosya | ${p.errors || 0} hata`,
                    finalFiles, finalSize, finalSpeed, 'Tamamlandı');
            }

            // Issue #177 — edge-triggered toast on state transition
            if (previousStatus !== finStatus) {
                pollScanProgress._lastSeenStatus = finStatus;
                if (finStatus === 'cancelled') {
                    notify(`Tarama iptal edildi: ${finalFiles} dosya kaydedildi`, 'warning');
                } else if (finStatus === 'failed') {
                    const errMsg = p.error || p.error_message || '';
                    notify(`Tarama başarısız${errMsg ? ': ' + errMsg : ''}`, 'error');
                } else {
                    // completed (or any other finished state)
                    if (p.total_files > 0 || displayCount > 0) {
                        notify(`Tarama tamamlandı: ${finalFiles} dosya, ${finalSize}`, 'success');
                    } else {
                        notify('Tarama tamamlandı ancak dosya bulunamadı.', 'warning');
                    }
                }
            }

            setTimeout(() => { loadSources(); }, 1000);
            // Global mini progress'i gizle (artik banner kalici)
            const miniProg = document.getElementById('global-scan-progress');
            if (miniProg) miniProg.style.display = 'none';
            return;
        }

        if (p.status === 'scanning' || p.status === 'generating_report' || p.status === 'connecting' || p.status === 'resuming') {
            pollScanProgress._lastSeenStatus = p.status;

            // Ensure scanning state class is set
            const prog = document.getElementById('scan-progress');
            if (prog && !prog.classList.contains('scan-scanning')) {
                prog.classList.remove('scan-completed', 'scan-failed', 'scan-cancelled', 'scan-idle');
                prog.classList.add('scan-scanning');
            }

            // displayCount comes from the function-scope definition at the
            // top of pollScanProgress (#138 — prefers p.live_count over
            // p.file_count during MFT collection phase).
            const _opsLive = _opsLiveBySource[sourceId];
            // Bottom-status elapsed (s) — fall back to (now - started_at)
            // from the ops registry when the scanner hasn't published
            // ``elapsed`` yet (still in connecting/early-enumeration).
            let elapsedStr = p.elapsed || '';
            if (!elapsedStr && _opsLive && _opsLive.started_at) {
                const secs = Math.max(0, Math.round(Date.now()/1000 - _opsLive.started_at));
                elapsedStr = secs + 's';
            }
            // Speed string — show "—" instead of "0/s" while the scanner
            // hasn't measured a rate yet so the user doesn't read it as
            // "broken / stuck at zero".
            const fps = Number(p.files_per_second);
            const speedStr = (Number.isFinite(fps) && fps > 0) ? (fps + '/s') : '—';

            document.getElementById('sp-files').textContent = formatNum(displayCount);
            document.getElementById('sp-size').textContent = p.total_size_formatted || formatSize(p.total_size);
            document.getElementById('sp-speed').textContent = (Number.isFinite(fps) && fps > 0) ? fps : '—';
            document.getElementById('sp-errors').textContent = p.errors || '0';
            document.getElementById('scan-elapsed').textContent = elapsedStr;
            const dir = p.current_dir || '';
            document.getElementById('sp-dir').textContent = dir.length > 60 ? '...' + dir.slice(-57) : dir;
            // Issue #135 — phase label (MFT okunuyor / DB'ye yaziliyor /
            // Analiz calisiyor / Tamamlandi). Falls back to the previous
            // "Tarama Devam Ediyor" header when phase is missing.
            const titleSpan = document.getElementById('scan-title-text');
            if (titleSpan) {
                let phaseLabel = 'Tarama Devam Ediyor';
                if (p.phase === 'enumeration') {
                    phaseLabel = 'MFT okunuyor (' + formatNum(displayCount) + ' kayıt)';
                } else if (p.phase === 'insert') {
                    phaseLabel = "DB'ye yazılıyor (" + formatNum(displayCount) + ' dosya)';
                } else if (p.phase === 'analysis') {
                    phaseLabel = 'Analiz çalışıyor';
                } else if (p.phase === 'completed') {
                    phaseLabel = 'Tamamlandı';
                }
                titleSpan.textContent = phaseLabel;
            }
            // Animated progress: prefer server-side phase_pct when available
            // so the bar reflects the lifecycle (enumeration -> insert ->
            // analysis -> completed) instead of climbing only with file
            // count. Old indeterminate fallback kept for backwards compat.
            const bar = document.getElementById('scan-progress-bar');
            let w;
            if (typeof p.phase_pct === 'number' && p.phase_pct > 0) {
                w = Math.max(5, Math.min(95, p.phase_pct));
            } else {
                w = Math.min(90, 5 + (displayCount / 100));
            }
            bar.style.width = w + '%';

            // Global mini progress (sidebar'da her sayfada gorunur)
            const gsp = document.getElementById('global-scan-progress');
            if (gsp) {
                gsp.style.display = '';
                document.getElementById('gsp-label').textContent = p.status === 'generating_report' ? 'Rapor...' : 'Tarama...';
                document.getElementById('gsp-count').textContent = formatNum(displayCount) + ' dosya';
                document.getElementById('gsp-bar').style.width = w + '%';
                document.getElementById('gsp-detail').textContent =
                    `${p.total_size_formatted || formatSize(p.total_size)} | ${speedStr} | ${elapsedStr || '—'}`;
            }

            // Issue #123: throttle mid-scan auto-refresh to >= 15 minutes.
            //
            // Old behaviour: ``file_count % 5000 < 500`` fires for ~10% of
            // polls (every 3s during a scan) — on a 2.5M-row scan that hit
            // /api/reports/{frequency,types,sizes} every ~30s, hammering DB
            // and CPU. With backend caching keyed on scan_id, *any* repeat
            // hit is a no-op; but mid-scan the scan_id is still ``running``
            // so each call invalidates the cache as soon as new files land.
            // Combine: rate-limit to once per 15 min per page, AND skip if
            // the last call was less than 15 min ago for this scan_id.
            window._reportRefreshLast = window._reportRefreshLast || {};
            const activePage = document.querySelector('.page.active')?.id?.replace('page-','');
            if (displayCount > 0 && ['frequency','types','sizes','treemap','overview'].includes(activePage)) {
                const now = Date.now();
                const last = window._reportRefreshLast[activePage] || 0;
                const FIFTEEN_MIN = 15 * 60 * 1000;
                if (now - last >= FIFTEEN_MIN) {
                    window._reportRefreshLast[activePage] = now;
                    const loaders = { overview: loadOverview, frequency: loadFrequency, types: loadTypes, sizes: loadSizes, treemap: loadTreemap };
                    if (loaders[activePage]) loaders[activePage]();
                }
            }
        }
    } catch(e) { /* silent */ }
}
pollScanProgress._lastSeenStatus = null;

// ═══════════════════════════════════════════════════
// FOLDER BROWSER
// ═══════════════════════════════════════════════════
function browsePath() {
    const wrap = document.getElementById('folder-browser-wrap');
    if (wrap.style.display === 'none') {
        wrap.style.display = 'block';
        // Start with common paths
        const list = document.getElementById('folder-browser-list');
        list.innerHTML = `
            <div class="folder-item" onclick="selectBrowsePath('C:\\\\')"><span class="icon">💾</span><span class="name">C:\\</span></div>
            <div class="folder-item" onclick="selectBrowsePath('D:\\\\')"><span class="icon">💾</span><span class="name">D:\\</span></div>
            <div class="folder-item" onclick="selectBrowsePath('E:\\\\')"><span class="icon">💾</span><span class="name">E:\\</span></div>
            <div class="folder-item" onclick="selectBrowsePath('\\\\\\\\')"><span class="icon">🌐</span><span class="name">UNC Yolu Girin...</span></div>
        `;
    } else {
        wrap.style.display = 'none';
    }
}

function selectBrowsePath(path) {
    document.getElementById('src-path').value = path;
    document.getElementById('folder-browser-wrap').style.display = 'none';
}

// ═══════════════════════════════════════════════════
// FOLDER BROWSER MODAL (Issue #82 Bug 4 / #105)
// ─── Generic folder picker backed by /api/system/list-dir.
// Opens #modal-folder-browser, lets the user click into directories,
// returns the chosen path to the original input (default: src-path).
// Localhost-only on the backend; remote callers see a friendly message.
// ═══════════════════════════════════════════════════
let folderBrowserState = {
    targetInputId: 'src-path',
    currentPath: '',
};

function loadFolderBrowser(targetInputId) {
    folderBrowserState.targetInputId = targetInputId || 'src-path';
    folderBrowserState.currentPath = '';
    // Seed the path input with whatever's already in the target field, if
    // any — saves the user navigating from "/" when they're refining a
    // path they typed earlier.
    const target = document.getElementById(folderBrowserState.targetInputId);
    const seed = (target && target.value) ? target.value.trim() : '';
    document.getElementById('fb-path-input').value = seed;
    folderBrowserHideError();
    openModal('modal-folder-browser');
    folderBrowserLoad(seed);
}

function folderBrowserHideError() {
    const err = document.getElementById('fb-error');
    err.style.display = 'none';
    err.textContent = '';
}

function folderBrowserShowError(msg) {
    const err = document.getElementById('fb-error');
    err.textContent = msg;
    err.style.display = 'block';
    document.getElementById('fb-list').innerHTML =
        '<div style="padding:20px;text-align:center;color:var(--text-muted)">—</div>';
}

async function folderBrowserLoad(path) {
    folderBrowserHideError();
    const list = document.getElementById('fb-list');
    list.innerHTML = '<div style="padding:20px;text-align:center;color:var(--text-muted)">Yukleniyor...</div>';
    let resp;
    try {
        resp = await fetch(`${API}/system/list-dir?path=${encodeURIComponent(path || '')}`);
    } catch (e) {
        folderBrowserShowError('Ag hatasi: ' + (e && e.message ? e.message : e));
        return;
    }
    if (resp.status === 403) {
        folderBrowserShowError(
            'Klasor tarayici sadece sunucudan calisirken kullanilabilir. Yolu manuel girin.'
        );
        return;
    }
    if (resp.status === 404) {
        folderBrowserShowError('Yol bulunamadi');
        return;
    }
    if (!resp.ok) {
        folderBrowserShowError('Hata: HTTP ' + resp.status);
        return;
    }
    const data = await resp.json();
    folderBrowserState.currentPath = data.path || '';
    document.getElementById('fb-path-input').value = data.path || '';
    folderBrowserRender(data);
}

function folderBrowserRender(data) {
    const list = document.getElementById('fb-list');
    const rows = [];
    // Up-one-level row when we're not at the logical roots.
    if (data.parent !== null && data.parent !== undefined) {
        const parentArg = JSON.stringify(data.parent);
        rows.push(
            `<div class="folder-item" onclick='folderBrowserLoad(${parentArg})'>`
            + `<span class="icon">⬆</span><span class="name">.. (Ust dizin)</span></div>`
        );
    }
    if (!data.entries || data.entries.length === 0) {
        rows.push(
            '<div style="padding:20px;text-align:center;color:var(--text-muted)">(Bos dizin)</div>'
        );
    } else {
        for (const e of data.entries) {
            const name = (e.name || '').replace(/[<>&"']/g, c => ({
                '<': '&lt;', '>': '&gt;', '&': '&amp;', '"': '&quot;', "'": '&#39;'
            }[c]));
            if (e.type === 'dir') {
                // Compose absolute path. Empty data.path means "we're at
                // the logical roots view" so the entry name IS the path.
                const childPath = data.path
                    ? joinPathForBrowser(data.path, e.name)
                    : e.name;
                const arg = JSON.stringify(childPath);
                rows.push(
                    `<div class="folder-item" onclick='folderBrowserLoad(${arg})'>`
                    + `<span class="icon">📁</span><span class="name">${name}</span></div>`
                );
            } else {
                const sizeStr = (e.size != null) ? formatBrowserSize(e.size) : '';
                rows.push(
                    `<div class="folder-item" style="cursor:default;opacity:0.65">`
                    + `<span class="icon">📄</span><span class="name">${name}</span>`
                    + `<span class="size">${sizeStr}</span></div>`
                );
            }
        }
    }
    if (data.truncated) {
        rows.push(
            `<div style="padding:10px;text-align:center;color:var(--warning,#f59e0b);font-size:12px">`
            + `Liste ${data.max_entries} girise kirpildi.</div>`
        );
    }
    list.innerHTML = rows.join('');
}

function joinPathForBrowser(parent, child) {
    // Pick the right separator. Backslash if the parent already uses it
    // (Windows / UNC), forward slash otherwise. We avoid os-style joins
    // here — the server will normalise on the next list-dir round trip.
    if (parent.indexOf('\\') >= 0) {
        return parent.endsWith('\\') ? parent + child : parent + '\\' + child;
    }
    return parent.endsWith('/') ? parent + child : parent + '/' + child;
}

function formatBrowserSize(bytes) {
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
    if (bytes < 1024 * 1024 * 1024) return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
    return (bytes / (1024 * 1024 * 1024)).toFixed(2) + ' GB';
}

function folderBrowserGo() {
    const v = document.getElementById('fb-path-input').value.trim();
    folderBrowserLoad(v);
}

function folderBrowserUp() {
    const cur = folderBrowserState.currentPath;
    if (!cur) return;  // already at logical roots
    // Strip trailing separator(s), then drop the last segment.
    let p = cur.replace(/[\\/]+$/, '');
    const lastBack = p.lastIndexOf('\\');
    const lastFwd = p.lastIndexOf('/');
    const cut = Math.max(lastBack, lastFwd);
    if (cut <= 0) {
        folderBrowserLoad('');
    } else {
        folderBrowserLoad(p.substring(0, cut));
    }
}

function folderBrowserConfirm() {
    // Prefer the (possibly hand-edited) path input over currentPath so a
    // power user can paste a UNC and just hit Sec without browsing into it.
    const chosen = document.getElementById('fb-path-input').value.trim()
        || folderBrowserState.currentPath;
    if (!chosen) {
        folderBrowserShowError('Lutfen bir yol secin veya girin.');
        return;
    }
    const target = document.getElementById(folderBrowserState.targetInputId);
    if (target) target.value = chosen;
    closeModal('modal-folder-browser');
}

// ═══════════════════════════════════════════════════
// ═══════════════════════════════════════════════════
// UPDATE (GitHub release kontrol + uygulama)
// ═══════════════════════════════════════════════════
function showUpdateDialog() {
    const d = window._updateInfo || {};
    const msg = `Yeni sürüm mevcut: v${d.remote}\nMevcut sürüm: v${d.local}\n\nGuncelle butonuna bastiginizda:\n  - Dashboard kapanir\n  - update.cmd arka planda calisir (yeni pencerede)\n  - master'in en son hali indirilip kurulur (~1-2 dakika)\n  - Verileriniz korunur (data/, config/, logs/)\n\n30-60 saniye sonra dashboard'u yeniden acabilirsiniz.\n\nDevam edilsin mi?`;
    if (!confirm(msg)) return;
    fetch('/api/system/update', { method: 'POST' })
        .then(r => r.json().then(d => ({ ok: r.ok, data: d })))
        .then(({ ok, data }) => {
            if (ok) {
                alert(data.message || 'Guncelleme baslatildi.');
            } else {
                alert('Guncelleme baslatilamadi: ' + (data.detail || 'bilinmeyen hata'));
            }
        })
        .catch(e => alert('Ag hatasi: ' + e));
}

function showWalDialog() {
    const w = window._walWarning || {};
    const msg = `Veritabani Bakimi Onerilir\n\nSQLite WAL dosyasi: ${w.wal_size_formatted || '(bilinmiyor)'}\nSiddet: ${w.severity || 'warning'}\n\nBuyuk WAL dosyalari sorgulari yavaslatir ve disk alani yer. "Optimize Et" butonu:\n  - WAL'i sifirdan tekrar olusturur (checkpoint TRUNCATE)\n  - Tablolari VACUUM + ANALYZE ile siker\n  - Sorgu planlarini gunceller\n\nIslem sirasinda dashboard birkac saniye askida kalabilir.\n\nKaynaklar sayfasina gitmek ister misiniz?`;
    if (!confirm(msg)) return;
    // Kaynaklar sayfasina goturelim
    if (typeof showPage === 'function') {
        showPage('sources');
    } else {
        // showPage yoksa scroll
        const el = document.querySelector('[onclick*="sources"]');
        if (el) el.click();
    }
}

// ═══════════════════════════════════════════════════
// OVERVIEW
// ═══════════════════════════════════════════════════
async function loadDbStats() {
    const el = document.getElementById('db-stats-info');
    el.textContent = 'Yukleniyor...';
    try {
        const s = await api('/db/stats');
        el.innerHTML = `
            <div style="display:flex;gap:16px;flex-wrap:wrap">
                <span>Toplam disk: <b>${formatSize(s.total_disk||s.db_size||0)}</b></span>
                <span>DB: <b>${formatSize(s.db_size||0)}</b></span>
                ${s.wal_size > 0 ? `<span style="color:${s.wal_size > 100000000 ? 'var(--danger)' : 'var(--warning)'}">WAL: <b>${formatSize(s.wal_size)}</b>${s.wal_size > 100000000 ? ' ⚠ Optimize edin!' : ''}</span>` : ''}
                <span>Dosya kayit: <b>${formatNum(s.scanned_files_count||0)}</b></span>
                <span>Tarama: <b>${s.scan_runs_count||0}</b></span>
                <span>Arsiv: <b>${formatNum(s.archived_files_count||0)}</b></span>
                <span>Erisim log: <b>${formatNum(s.user_access_logs_count||0)}</b></span>
                <span>Ilk: <b>${(s.oldest_scan||'-').substring(0,10)}</b></span>
                <span>Son: <b>${(s.newest_scan||'-').substring(0,10)}</b></span>
            </div>`;
    } catch(e) { el.textContent = 'Hata: ' + e.message; }
}
async function cleanupOldScans() {
    const keep = prompt('Son kac taramayi korumak istiyorsunuz? (varsayilan: 5)', '5');
    if (!keep) return;
    try {
        const r = await api('/db/cleanup?keep_last=' + parseInt(keep), {method:'POST'});
        if (r.error) { notify(r.error, 'error'); return; }
        notify(`Temizlik tamamlandi: ${r.deleted_runs} tarama, ${formatNum(r.deleted_files)} dosya kaydi silindi`, 'success');
        loadDbStats();
    } catch(e) { notify('Temizlik hatasi: ' + e.message, 'error'); }
}
async function optimizeDb() {
    try {
        const r = await api('/db/optimize', {method:'POST'});
        if (r.error) { notify(r.error, 'error'); return; }
        const saved = r.saved > 0 ? ` (${formatSize(r.saved)} kazanildi)` : '';
        notify(`Veritabani optimize edildi: ${formatSize(r.size_before)} -> ${formatSize(r.size_after)}${saved}`, 'success');
        loadDbStats();
    } catch(e) { notify('Optimize hatasi: ' + e.message, 'error'); }
}

// Issue #139 — partial-data banner state. Single setInterval handle so
// switching sources or navigating away cancels the auto-refresh. The
// banner re-fetches /api/overview every 30 sec while is_partial is true;
// when the response flips to full data the banner hides + interval clears.
let _partialBannerTimer = null;

function updatePartialBanner(sourceId, ov) {
    const banner = document.getElementById('ov-partial-banner');
    const textEl = document.getElementById('ov-partial-banner-text');
    if (!banner || !textEl) return;
    const isPartial = !!(ov && ov.is_partial);
    if (!isPartial) {
        banner.style.display = 'none';
        if (_partialBannerTimer) {
            clearInterval(_partialBannerTimer);
            _partialBannerTimer = null;
        }
        return;
    }
    // Compute a friendly "N dk once" label from partial_updated_at.
    let mins = null;
    const ts = ov.partial_updated_at;
    if (ts) {
        const parsed = Date.parse(ts.replace(' ', 'T'));
        if (!isNaN(parsed)) {
            mins = Math.max(0, Math.floor((Date.now() - parsed) / 60000));
        }
    }
    const minsLabel = (mins === null) ? 'bilinmiyor' :
        (mins < 1 ? 'az once' : `${mins} dk once`);
    textEl.textContent =
        `⚠ Kismi veri — Tarama devam ediyor, son guncelleme ${minsLabel}.`;
    banner.style.display = 'block';
    // Schedule a 30-second auto refresh so the banner stays current AND
    // flips to "complete" automatically once the scan finishes.
    if (!_partialBannerTimer) {
        _partialBannerTimer = setInterval(() => {
            const currentSid = document.getElementById('overview-source')?.value;
            if (!currentSid || String(currentSid) !== String(sourceId)) {
                clearInterval(_partialBannerTimer);
                _partialBannerTimer = null;
                return;
            }
            api(`/overview/${currentSid}`, {silent:true})
                .then(ov2 => updatePartialBanner(currentSid, ov2))
                .catch(() => {});
        }, 30000);
    }
}

// ═══════════════════════════════════════════════════
// PARTIAL SUMMARY v2 — Live data during scan (#181 Track B2)
// Shared helpers used by every dashboard page's loadXxx() function.
// ═══════════════════════════════════════════════════
let _psv2PollTimer = null;       // single global poll timer
let _psv2PollPage  = null;       // page name currently being polled
let _psv2PollSid   = null;       // source ID string currently being polled
const _psv2PrevWasPartial = {};  // pageName → bool (was last render from partial?)

async function _fetchPartialSummaryV2(sid) {
    // Returns the v2 JSON dict when a scan is in progress, null otherwise.
    // Never throws — a missing endpoint is treated as "no partial data".
    try {
        const r = await fetch(API + '/sources/' + sid + '/partial-summary', { cache: 'no-store' });
        if (!r.ok) return null;
        return await r.json();
    } catch (e) { return null; }
}

function _psv2ShowBanner(bannerId, state, progress) {
    const el = document.getElementById(bannerId);
    if (!el) return;
    const filesStr = (progress && progress.files_so_far) ? formatNum(progress.files_so_far) + ' dosya' : '';
    const rateStr  = (progress && progress.rate_per_sec)  ? ', ' + formatNum(progress.rate_per_sec) + '/sn'  : '';
    const stateMap = { mft_phase: 'MFT okuma', db_writing: 'DB yazma', enrich: 'Zenginlestirme' };
    const stateStr = stateMap[state] || state || 'tarama';
    el.textContent = '\u26A0 Kismi veri \u2014 Tarama devam ediyor (' + stateStr + (filesStr ? ', ' + filesStr : '') + rateStr + ')';
    el.style.display = 'block';
}

function _psv2HideBanner(bannerId) {
    const el = document.getElementById(bannerId);
    if (el) el.style.display = 'none';
}

function _psv2StartPoll(sid, pageName, loaderFn) {
    // Don't restart if already polling the same source + page.
    if (_psv2PollTimer && _psv2PollPage === pageName && _psv2PollSid === String(sid)) return;
    _psv2StopPoll();
    _psv2PollPage = pageName;
    _psv2PollSid  = String(sid);
    _psv2PollTimer = setInterval(async () => {
        const curPage = document.querySelector('.page.active');
        if (!curPage || curPage.id !== 'page-' + pageName) { _psv2StopPoll(); return; }
        try { await loaderFn(); } catch (e) { /* ignore transient poll errors */ }
    }, 5000);
}

function _psv2StopPoll() {
    if (_psv2PollTimer) { clearInterval(_psv2PollTimer); _psv2PollTimer = null; }
    _psv2PollPage = null;
    _psv2PollSid  = null;
}


// ═══════════════════════════════════════════════════
// OVERVIEW (Genel Bakis)
// ═══════════════════════════════════════════════════
// Issue (post-#192): a previous merge accidentally removed this
// function's declaration line, leaving the body floating at the top
// level. Result: SyntaxError on `await loadSources()`, the entire
// inline script fails to parse, every sidebar menu stops working
// (customer report 2026-04-28: "menüler arası gezinti yok").
async function loadOverview() {
    if (!sources.length) await loadSources();
    const sid = document.getElementById('overview-source').value;

    if (!sid) {
        ['ov-files','ov-size','ov-stale-pct','ov-owners','ov-risky','ov-dups'].forEach(id => {
            const el = document.getElementById(id); if(el) el.textContent = '-';
        });
        document.getElementById('ov-risk-text').textContent = '--';
        updatePartialBanner(null, null);
        return;
    }

    try {
        // HIZLI YOL: risk-score ve trend her ikisi de cached (scan_runs.summary_json).
        // Ag sorgusu olmadan milisaniyede gelirler. Sayfa bunlarla hemen dolar.
        const [riskData, trendData] = await Promise.all([
            api(`/risk-score/${sid}`, {silent:true}).catch(()=>({risk_score:0, kpis:{}})),
            api(`/trend/${sid}`, {silent:true}).catch(()=>({scans:[], growth:null})),
        ]);

        // Issue #139 — partial-data banner. /api/overview falls back to
        // scan_runs.partial_summary_json during a live scan; render the
        // banner whenever that endpoint reports is_partial=true.
        api(`/overview/${sid}`, {silent:true})
            .then(ov => updatePartialBanner(sid, ov))
            .catch(() => updatePartialBanner(sid, null));

        // YAVAS ENDPOINT'LER: frequency + insights arka planda, UI beklemez.
        // Her ikisi de cache miss'te agir oldugu icin await edilmezler; geldiklerinde
        // ilgili karti/panelini guncellerler. Boylece Overview dakikalarca takilmaz.
        const freqPromise = api(`/reports/frequency/${sid}`, {silent:true})
            .catch(()=>({frequency:[]}));
        const insightsPromise = api(`/insights/${sid}`, {silent:true})
            .catch(()=>({insights:[], score:0}));

        // KPI Cards
        const rs = riskData.risk_score || 0;
        const kpi = riskData.kpis || {};

        // Risk gauge
        const circumference = 213.6;
        const offset = circumference - (circumference * rs / 100);
        const gauge = document.getElementById('ov-risk-gauge');
        gauge.style.strokeDashoffset = offset;
        gauge.style.stroke = rs >= 70 ? 'var(--danger)' : rs >= 40 ? 'var(--warning)' : 'var(--success)';
        document.getElementById('ov-risk-text').textContent = rs;
        const lvl = riskData.risk_level || 'good';
        const lvlMap = {critical:'Kritik', warning:'Uyari', good:'Iyi'};
        document.getElementById('ov-risk-level').textContent = lvlMap[lvl] || lvl;

        document.getElementById('ov-stale-pct').textContent = (kpi.stale_pct || 0) + '%';
        document.getElementById('ov-stale-sub').textContent = formatNum(kpi.stale_count || 0) + ' dosya, ' + formatSize(kpi.stale_size || 0);

        document.getElementById('ov-files').textContent = formatNum(riskData.total_files || 0);
        const growth = trendData.growth;
        document.getElementById('ov-files-sub').textContent = growth ? ((growth.file_diff >= 0 ? '+' : '') + formatNum(growth.file_diff) + ' son tarama') : '';

        document.getElementById('ov-size').textContent = formatSize(riskData.total_size || 0);
        document.getElementById('ov-size-sub').textContent = growth ? ((growth.size_diff >= 0 ? '+' : '') + formatSize(growth.size_diff) + ' son tarama') : '';

        document.getElementById('ov-owners').textContent = kpi.owner_count || 0;
        document.getElementById('ov-owners-sub').textContent = kpi.owner_count <= 1 ? 'read_owner aktif edin' : 'benzersiz sahip';

        document.getElementById('ov-risky').textContent = formatNum(kpi.risky_files || 0);

        document.getElementById('ov-dups').textContent = formatNum(kpi.dup_groups || 0);

        // Age distribution chart (horizontal bar) — frequency endpoint'i yavas
        // olabilir, arka planda doldur. Once bos yer tut, cevap gelince ciz.
        destroyChart('ov-age-chart');
        freqPromise.then(freqData => {
            const freq = freqData.frequency || [];
            if (freq.length) {
                const ageColors = freq.map(f => {
                    const d = f.days || 0;
                    if (d >= 365) return '#ef4444';
                    if (d >= 180) return '#f59e0b';
                    if (d >= 90) return '#eab308';
                    return '#10b981';
                });
                chartInstances['ov-age-chart'] = new Chart(document.getElementById('ov-age-chart'), {
                    type: 'bar', data: {
                        labels: freq.map(f => f.label),
                        datasets: [{
                            label: 'Dosya Sayisi', data: freq.map(f => f.file_count),
                            backgroundColor: ageColors.map(c=>c+'cc'), borderColor: ageColors, borderWidth: 1, borderRadius: 4
                        }]
                    }, options: { indexAxis: 'y', responsive: true, plugins: { legend: { display: false } },
                        scales: { x: { beginAtZero: true, grid: { color: '#1e293b' } }, y: { grid: { display: false } } } }
                });
            }
        });

        // Growth trend chart (line)
        const scans = trendData.scans || [];
        destroyChart('ov-trend-chart');
        if (scans.length) {
            chartInstances['ov-trend-chart'] = new Chart(document.getElementById('ov-trend-chart'), {
                type: 'line', data: {
                    labels: scans.map(s => (s.started_at || '').substring(5, 16)),
                    datasets: [
                        { label: 'Dosya Sayisi', data: scans.map(s => s.total_files || 0), borderColor: '#3b82f6', backgroundColor: 'rgba(59,130,246,0.1)', fill: true, tension: 0.3, yAxisID: 'y' },
                        { label: 'Boyut (MB)', data: scans.map(s => ((s.total_size || 0) / 1048576).toFixed(0)), borderColor: '#10b981', backgroundColor: 'rgba(16,185,129,0.1)', fill: true, tension: 0.3, yAxisID: 'y1' }
                    ]
                }, options: { responsive: true, interaction: { mode: 'index', intersect: false },
                    plugins: { legend: { labels: { boxWidth: 10, font: { size: 11 } } } },
                    scales: { y: { beginAtZero: true, position: 'left', grid: { color: '#1e293b' } }, y1: { beginAtZero: true, position: 'right', grid: { drawOnChartArea: false } }, x: { grid: { display: false } } } }
            });
        }

        // AI Recommendations (top 5) — insights cache miss'te agir olabilir.
        // Arka planda ac, bekleme. Spinner yerine once cached olmayacagini soyleyen
        // mesaj goster, gelince degistir.
        const sevColors = { critical: 'var(--danger)', warning: 'var(--warning)', info: 'var(--info)', success: 'var(--success)' };
        const recEl = document.getElementById('ov-recommendations');
        if (recEl) {
            recEl.innerHTML = '<div style="text-align:center;padding:20px;color:var(--text-muted);font-size:12px">AI onerileri arka planda hazirlaniyor, hazir olunca burada gorunecek...</div>';
        }
        insightsPromise.then(insightsData => {
            const insights = (insightsData.insights || []).slice(0, 5);
            if (!recEl) return;
            if (insights.length) {
                recEl.innerHTML = insights.map(i => `
                    <div style="display:flex;align-items:center;gap:12px;padding:10px 14px;background:var(--bg-secondary);border-radius:8px;border-left:3px solid ${sevColors[i.severity]||'var(--border)'}">
                        <span style="display:inline-block;padding:2px 8px;border-radius:10px;font-size:10px;font-weight:700;background:${sevColors[i.severity]||'var(--border)'}22;color:${sevColors[i.severity]||'var(--text-muted)'};flex-shrink:0">${escapeHtml((i.severity||'info').toUpperCase())}</span>
                        <div style="flex:1;min-width:0">
                            <div style="font-size:13px;font-weight:600;color:var(--text-primary)">${escapeHtml(i.title)}</div>
                            <div style="font-size:11px;color:var(--text-secondary);margin-top:2px">${escapeHtml(i.description)}</div>
                        </div>
                        ${i.impact_size ? `<span style="font-size:12px;color:var(--text-muted);flex-shrink:0">${formatSize(i.impact_size)}</span>` : ''}
                        <button class="btn btn-sm btn-accent" onclick="showInsightFilesByCategory('${escapeHtml(i.category||'')}', '${escapeHtml(i.insight_type||'')}', ${sid})" style="flex-shrink:0">Incele</button>
                        ${i.action ? `<button class="btn btn-sm btn-outline" onclick="applyInsight('${escapeHtml(i.category)}', ${sid})" style="flex-shrink:0">Uygula</button>` : ''}
                    </div>
                `).join('');
            } else {
                recEl.innerHTML = '<div style="text-align:center;padding:20px;color:var(--text-muted);font-size:12px">Tarama yapildiktan sonra oneriler goruntulenecek</div>';
            }
        });
    } catch(e) { console.error(e); }
}

// ═══════════════════════════════════════════════════
// APPLY INSIGHT (Uygula butonu)
// ═══════════════════════════════════════════════════
async function applyInsight(category, sourceId) {
    // Map category to insight_type
    const typeMap = {
        'stale': 'stale_1year',
        'storage': 'large_files',
        'duplicates': 'duplicates',
        'security': 'temp_files',
        'growth': 'stale_3year',
        'recommendation': 'stale_1year',
        'audit': 'temp_files'
    };
    const insightType = typeMap[category] || 'stale_1year';

    try {
        // Step 1: Preview
        const preview = await api('/archive/by-insight', {
            method: 'POST',
            body: JSON.stringify({ type: insightType, source_id: sourceId, confirm: false })
        });

        if (!preview.file_count || preview.file_count === 0) {
            notify('Bu kategoride arsivlenecek dosya bulunamadi.', 'info');
            return;
        }

        // Step 2: Show enhanced confirmation modal
        const allFiles = preview.sample || [];
        const sampleHtml = allFiles.map((f, i) =>
            `<tr><td style="font-size:11px">${i+1}</td><td style="max-width:250px;word-break:break-all;font-size:11px">${escapeHtml(f.file_name || '')}</td><td style="max-width:300px;word-break:break-all;font-size:11px" title="${escapeHtml(f.file_path || '')}">${escapeHtml(f.file_path || '')}</td><td>${formatSize(f.file_size)}</td><td style="font-size:11px">${escapeHtml(f.owner || '-')}</td></tr>`
        ).join('');

        // Duplike kategorisi icin ozel yonlendirme
        const dupRedirect = category === 'duplicates' ? `<div style="margin-bottom:12px;padding:10px;background:rgba(0,212,255,0.1);border:1px solid var(--accent);border-radius:6px;font-size:12px">
            <strong>Ipucu:</strong> Duplike dosyalar icin detayli gruplu rapor ve secici arsivleme icin
            <a href="#" onclick="closeModal('modal-insight-confirm');showPage('duplicates')" style="color:var(--accent);text-decoration:underline">Kopya Dosyalar</a> sayfasini kullanabilirsiniz.
        </div>` : '';

        const modalContent = `
            <div style="padding:20px">
                <h3 style="margin:0 0 16px 0">Arsivleme Onaylama</h3>
                <div class="cards" style="margin-bottom:16px">
                    <div class="card accent"><div class="card-label">Dosya Sayisi</div><div class="card-value">${formatNum(preview.file_count)}</div></div>
                    <div class="card warning"><div class="card-label">Toplam Boyut</div><div class="card-value">${preview.total_size_formatted}</div></div>
                    <div class="card purple"><div class="card-label">Kategori</div><div class="card-value" style="font-size:14px">${escapeHtml(category)}</div></div>
                </div>
                ${dupRedirect}
                <div style="font-size:13px;color:var(--text-secondary);margin-bottom:12px">Tip: <strong>${escapeHtml(insightType)}</strong> | Tum dosyalar asagida listelenmistir (ilk ${allFiles.length} dosya)${preview.file_count > allFiles.length ? ` - toplam ${preview.file_count} dosya` : ''}</div>
                ${allFiles.length ? `
                <div style="max-height:350px;overflow-y:auto;margin-bottom:16px;border:1px solid var(--border);border-radius:var(--radius)">
                    <table style="width:100%;font-size:12px">
                        <thead style="position:sticky;top:0;background:var(--bg-secondary)"><tr><th>#</th><th>Dosya Adi</th><th>Yol</th><th>Boyut</th><th>Sahip</th></tr></thead>
                        <tbody>${sampleHtml}</tbody>
                    </table>
                    ${preview.file_count > allFiles.length ? `<div style="text-align:center;padding:8px;color:var(--text-muted);font-size:11px">ve ${preview.file_count - allFiles.length} dosya daha arsivlenecek...</div>` : ''}
                </div>` : ''}
                <div style="display:flex;gap:12px;justify-content:flex-end">
                    <button class="btn btn-outline" onclick="closeModal('modal-insight-confirm')">Iptal</button>
                    <button class="btn btn-primary" onclick="confirmInsightArchive('${escapeHtml(insightType)}', ${sourceId})" style="background:var(--danger)">Onayla ve Arsivle (${formatNum(preview.file_count)} dosya)</button>
                </div>
            </div>
        `;

        // Create modal dynamically
        let modal = document.getElementById('modal-insight-confirm');
        if (!modal) {
            modal = document.createElement('div');
            modal.id = 'modal-insight-confirm';
            modal.className = 'modal-overlay';
            modal.innerHTML = '<div class="modal" style="max-width:600px"></div>';
            modal.onclick = (e) => { if (e.target === modal) closeModal('modal-insight-confirm'); };
            document.body.appendChild(modal);
        }
        modal.querySelector('.modal').innerHTML = modalContent;
        modal.classList.add('active');

    } catch(e) {
        console.error(e);
        notify('Insight arsivleme hatasi: ' + (e.message || e), 'error');
    }
}

async function confirmInsightArchive(insightType, sourceId) {
    closeModal('modal-insight-confirm');
    notify('Arsivleme baslatildi...', 'info');
    try {
        const result = await api('/archive/by-insight', {
            method: 'POST',
            body: JSON.stringify({ type: insightType, source_id: sourceId, confirm: true })
        });
        if (result.archived > 0) {
            notify(`Arsivleme tamamlandi: ${result.archived} dosya, ${result.total_size_formatted}`, 'success');
        } else {
            notify('Arsivlenecek dosya bulunamadi veya hata olustu.', 'warning');
        }
        loadOverview();
        loadArchive();
    } catch(e) {
        notify('Arsivleme hatasi: ' + (e.message || e), 'error');
    }
}

// ═══════════════════════════════════════════════════
// FREQUENCY
// ═══════════════════════════════════════════════════
async function loadFrequency() {
    const sid = document.getElementById('freq-source').value;
    if (!sid) return;

    // Issue #181 Track B2: try partial summary first while scan is running.
    const ps = await _fetchPartialSummaryV2(sid);
    const wasPartial = !!_psv2PrevWasPartial['frequency'];
    if (ps && ps.scan_state && ps.scan_state !== 'completed') {
        _psv2PrevWasPartial['frequency'] = true;
        _psv2ShowBanner('freq-partial-banner', ps.scan_state, ps.progress);
        const ab = (ps.summary && ps.summary.age_buckets) || {};
        const bucketOrder = ['<30d', '30-60d', '60-90d', '90-180d', '180-365d', '>365d'];
        const bucketDays  = [0, 30, 60, 90, 180, 365];
        const labels = bucketOrder.map(k => ab.hasOwnProperty(k) ? k : null).filter(Boolean);
        if (labels.length) {
            const counts = labels.map(k => ab[k] || 0);
            const barColors = bucketDays.map(d => d >= 365 ? '#ef4444' : d >= 180 ? '#f59e0b' : d >= 90 ? '#eab308' : '#10b981');
            document.getElementById('freq-cards').innerHTML = labels.map((k, i) => {
                const d = bucketDays[i] || 0;
                const c = counts[i];
                const color = d >= 365 ? 'var(--danger)' : d >= 180 ? 'var(--warning)' : d >= 90 ? '#eab308' : 'var(--success)';
                return `<div class="card" style="border-top:3px solid ${color}"><div class="card-label">${escapeHtml(k)}</div><div class="card-value" style="font-size:22px">${formatNum(c)}</div><div class="card-sub">Kismi veri</div></div>`;
            }).join('');
            destroyChart('freq-chart');
            chartInstances['freq-chart'] = new Chart(document.getElementById('freq-chart'), {
                type: 'bar', data: { labels, datasets: [{ label: 'Dosya Sayisi', data: counts, backgroundColor: barColors.map(c=>c+'99'), borderColor: barColors, borderWidth: 1, borderRadius: 6, yAxisID: 'y' }] },
                options: { responsive: true, plugins: { legend: { display: true } }, scales: { y: { beginAtZero: true, grid: { color: '#1e293b' } }, x: { grid: { display: false } } } }
            });
            document.getElementById('freq-table-wrap').style.display = 'none';
        } else {
            document.getElementById('freq-cards').innerHTML = '<div style="padding:20px;color:var(--text-muted)">Kismi veri yok \u2014 Tarama tamamlandiktan sonra goruntulenecek</div>';
        }
        _psv2StartPoll(sid, 'frequency', loadFrequency);
        return;
    }

    if (wasPartial && ps && ps.scan_state === 'completed') {
        notify('Tarama tamamlandi, Erisim Sikligi guncellendi', 'success');
    }
    _psv2PrevWasPartial['frequency'] = false;
    _psv2HideBanner('freq-partial-banner');
    _psv2StopPoll();

    const data = await api(`/reports/frequency/${sid}`);
    const freq = data.frequency || [];

    // Stale badge helper
    function staleBadge(days) {
        if (days >= 365) return '<span style="display:inline-block;padding:2px 8px;border-radius:10px;font-size:10px;font-weight:700;background:rgba(239,68,68,0.15);color:#ef4444;margin-left:6px">ARSIVLE</span>';
        if (days >= 180) return '<span style="display:inline-block;padding:2px 8px;border-radius:10px;font-size:10px;font-weight:700;background:rgba(245,158,11,0.15);color:#f59e0b;margin-left:6px">ESKI</span>';
        if (days >= 90) return '<span style="display:inline-block;padding:2px 8px;border-radius:10px;font-size:10px;font-weight:700;background:rgba(234,179,8,0.15);color:#eab308;margin-left:6px">DIKKAT</span>';
        return '<span style="display:inline-block;padding:2px 8px;border-radius:10px;font-size:10px;font-weight:700;background:rgba(16,185,129,0.15);color:#10b981;margin-left:6px">AKTIF</span>';
    }

    // Calculate stale savings
    let staleSavings = 0;
    freq.forEach(f => { if ((f.days || 0) >= 365) staleSavings += (f.total_size || 0); });

    // Cards (clickable for drill-down)
    const total = freq.reduce((s, f) => s + f.file_count, 0);
    let cardsHtml = freq.map((f, i) => {
        const minD = f.days || 0;
        const maxD = freq[i+1] ? freq[i+1].days : null;
        const maxParam = maxD ? `&max_days=${maxD}` : '';
        const badgeColor = minD >= 365 ? 'var(--danger)' : minD >= 180 ? 'var(--warning)' : minD >= 90 ? '#eab308' : 'var(--success)';
        return `
        <div class="card clickable-row" style="border-top:3px solid ${badgeColor}"
             onclick="showDrilldown('${escapeHtml(f.label)}', '/drilldown/frequency/${sid}?min_days=${minD}${maxParam}',
             {source_id:${sid}, filter_type:'frequency', min_days:${minD}${maxD?`, max_days:${maxD}`:''}})">
            <div class="card-label">${escapeHtml(f.label)} ${staleBadge(minD)}</div>
            <div class="card-value" style="font-size:22px">${formatNum(f.file_count)}</div>
            <div class="card-sub">${escapeHtml(f.total_size_formatted)} | ${total ? ((f.file_count/total*100).toFixed(1))+'%' : '0%'}</div>
        </div>`;
    }).join('');

    // Savings banner
    if (staleSavings > 0) {
        cardsHtml = `<div style="grid-column:1/-1;background:linear-gradient(135deg,rgba(16,185,129,0.1),rgba(59,130,246,0.1));border:1px solid var(--success);border-radius:var(--radius);padding:16px 20px;display:flex;align-items:center;justify-content:space-between;margin-bottom:4px">
            <div><span style="font-size:14px;font-weight:700;color:var(--success)">Toplam Tasarruf Potansiyeli</span>
            <span style="font-size:12px;color:var(--text-secondary);margin-left:12px">Bu dosyalar arsivlenirse <strong style="color:var(--success)">${formatSize(staleSavings)}</strong> kazanilir</span></div>
            <button class="btn btn-sm btn-success" onclick="showDrilldown('365+ Gun Arsivle', '/drilldown/frequency/${sid}?min_days=365', {source_id:${parseInt(sid)}, filter_type:'frequency', min_days:365})">Arsivle</button>
        </div>` + cardsHtml;
    }
    document.getElementById('freq-cards').innerHTML = cardsHtml;

    // Chart
    destroyChart('freq-chart');
    if (freq.length) {
        const barColors = freq.map(f => {
            const d = f.days || 0;
            return d >= 365 ? '#ef4444' : d >= 180 ? '#f59e0b' : d >= 90 ? '#eab308' : '#10b981';
        });
        chartInstances['freq-chart'] = new Chart(document.getElementById('freq-chart'), {
            type: 'bar', data: {
                labels: freq.map(f => f.label),
                datasets: [
                    { label: 'Dosya Sayisi', data: freq.map(f => f.file_count), backgroundColor: barColors.map(c=>c+'99'), borderColor: barColors, borderWidth: 1, borderRadius: 6, yAxisID: 'y' },
                ]
            }, options: { responsive: true, plugins: { legend: { display: true } }, scales: { y: { beginAtZero: true, grid: { color: '#1e293b' } }, x: { grid: { display: false } } } }
        });
    }

    // Table
    document.getElementById('freq-table-wrap').style.display = 'block';
    document.getElementById('freq-table').innerHTML = `
        <thead><tr><th>Kriter</th><th>Durum</th><th>Dosya Sayisi</th><th>Toplam Boyut</th><th>Yuzde</th></tr></thead>
        <tbody>${freq.map((f, i) => {
            const minD = f.days || 0;
            const maxD = freq[i+1] ? freq[i+1].days : null;
            const maxParam = maxD ? `&max_days=${maxD}` : '';
            return `<tr class="clickable-row" onclick="showDrilldown('${escapeHtml(f.label)}', '/drilldown/frequency/${sid}?min_days=${minD}${maxParam}', {source_id:${sid}, filter_type:'frequency', min_days:${minD}${maxD?`, max_days:${maxD}`:''}})"><td>${escapeHtml(f.label)}</td><td>${staleBadge(minD)}</td><td>${formatNum(f.file_count)}</td><td>${escapeHtml(f.total_size_formatted)}</td><td>${total ? (f.file_count/total*100).toFixed(1)+'%' : '-'}</td></tr>`;
        }).join('')}</tbody>
    `;

    // Issue #84 — wire view toggle (visual = histogram above; grid = file list).
    window.__freqLastSourceId = sid;
    _attachFrequencyViewToggle();
}

// Issue #84 — Gorsel ↔ Profesyonel mod toggle for Erisim Sikligi.
function _renderFrequencyVisual() {
    const v = document.getElementById('freq-visual-host');
    const g = document.getElementById('freq-grid-host');
    if (v) v.style.display = '';
    if (g) g.style.display = 'none';
}

async function _renderFrequencyGrid() {
    const v = document.getElementById('freq-visual-host');
    const host = document.getElementById('freq-grid-host');
    if (v) v.style.display = 'none';
    host.style.display = '';
    const sid = window.__freqLastSourceId
        || document.getElementById('freq-source')?.value;
    if (!sid) {
        host.innerHTML = '<div style="padding:20px;color:var(--text-muted)">Once bir kaynak secin</div>';
        return;
    }
    if (typeof renderEntityList !== 'function') {
        host.innerHTML = '<div style="padding:20px;color:var(--text-muted)">entity-list yuklenmedi</div>';
        return;
    }
    host.innerHTML = '<div style="padding:30px;text-align:center;color:var(--text-muted);font-size:12px">Dosyalar yukleniyor...</div>';
    try {
        // Pull a generous slice of files (any age >= 0 days) so the grid covers
        // every band the chart shows. The drilldown endpoint already paginates
        // server-side; entity-list re-paginates client-side.
        const data = await api(`/drilldown/frequency/${sid}?min_days=0&page=1&limit=500`,
            { silent: true });
        const files = data.files || [];
        const rows = files.map(f => ({
            id: f.id,
            file_path: f.file_path,
            file_name: f.file_name || '',
            file_size: f.file_size || 0,
            file_size_formatted: f.file_size_formatted || formatSize(f.file_size || 0),
            owner: f.owner || '',
            last_access_time: (f.last_access_time || '').substring(0, 19),
            last_modify_time: (f.last_modify_time || '').substring(0, 19),
        }));
        renderEntityList(host, {
            rows: rows,
            rowKey: 'id',
            pageSize: 50,
            searchKeys: ['file_path', 'file_name', 'owner'],
            columns: [
                {key: 'file_path', label: 'Dosya Yolu'},
                {key: 'file_name', label: 'Dosya Adi'},
                {key: 'file_size', label: 'Boyut',
                 render: (v, row) => row.file_size_formatted || formatSize(v || 0)},
                {key: 'last_access_time', label: 'Son Erisim'},
                {key: 'last_modify_time', label: 'Son Degisiklik'},
                {key: 'owner', label: 'Sahip'},
            ],
            emptyMessage: 'Dosya bulunamadi',
        });
    } catch (e) {
        host.innerHTML = '<div style="padding:20px;color:var(--danger);font-size:12px">Liste yuklenemedi: ' +
            (e && e.message ? e.message : e) + '</div>';
    }
}

function _attachFrequencyViewToggle() {
    const page = document.getElementById('page-frequency');
    if (!page || typeof attachViewToggle !== 'function') return;
    attachViewToggle(page, {
        pageKey: 'frequency',
        renderVisual: _renderFrequencyVisual,
        renderGrid: _renderFrequencyGrid,
        defaultMode: 'visual',
    });
}

// ═══════════════════════════════════════════════════
// TYPES
// ═══════════════════════════════════════════════════
async function loadTypes() {
    const sid = document.getElementById('types-source').value;
    if (!sid) return;

    // Issue #181 Track B2: try partial summary first while scan is running.
    const ps = await _fetchPartialSummaryV2(sid);
    const wasPartial = !!_psv2PrevWasPartial['types'];
    if (ps && ps.scan_state && ps.scan_state !== 'completed') {
        _psv2PrevWasPartial['types'] = true;
        _psv2ShowBanner('types-partial-banner', ps.scan_state, ps.progress);
        const byExt = (ps.summary && ps.summary.by_extension) || [];
        const top = byExt.slice(0, 15);
        destroyChart('types-pie-chart');
        if (top.length) {
            chartInstances['types-pie-chart'] = new Chart(document.getElementById('types-pie-chart'), {
                type: 'doughnut', data: { labels: top.map(t=>'.'+escapeHtml(t.ext||'')), datasets: [{ data: top.map(t=>t.size_bytes||0), backgroundColor: COLORS, borderWidth: 0 }] },
                options: { responsive: true, cutout: '60%', plugins: { legend: { position: 'right', labels: { boxWidth: 10, padding: 6, font: { size: 11 } } } } }
            });
        }
        destroyChart('types-count-chart');
        if (top.length) {
            chartInstances['types-count-chart'] = new Chart(document.getElementById('types-count-chart'), {
                type: 'bar', data: { labels: top.map(t=>'.'+escapeHtml(t.ext||'')), datasets: [{ label: 'Dosya Sayisi', data: top.map(t=>t.count||0), backgroundColor: COLORS.map(c=>c+'99'), borderColor: COLORS, borderWidth: 1, borderRadius: 4 }] },
                options: { indexAxis: 'y', responsive: true, plugins: { legend: { display: false } }, scales: { x: { beginAtZero: true, grid: { color: '#1e293b' } }, y: { grid: { display: false } } } }
            });
        }
        document.getElementById('types-table').innerHTML = `
            <thead><tr><th>Uzanti</th><th>Sayi</th><th>Toplam Boyut</th></tr></thead>
            <tbody>${byExt.slice(0,30).map(t => `<tr><td><strong>.${escapeHtml(t.ext||'')}</strong></td><td>${formatNum(t.count||0)}</td><td>${formatSize(t.size_bytes||0)}</td></tr>`).join('')}</tbody>
        `;
        _psv2StartPoll(sid, 'types', loadTypes);
        return;
    }

    if (wasPartial && ps && ps.scan_state === 'completed') {
        notify('Tarama tamamlandi, Dosya Turleri guncellendi', 'success');
    }
    _psv2PrevWasPartial['types'] = false;
    _psv2HideBanner('types-partial-banner');
    _psv2StopPoll();

    const data = await api(`/reports/types/${sid}`);
    const types = data.types || [];
    const top = types.slice(0, 15);

    destroyChart('types-pie-chart');
    if (top.length) {
        chartInstances['types-pie-chart'] = new Chart(document.getElementById('types-pie-chart'), {
            type: 'doughnut', data: { labels: top.map(t=>'.'+t.extension), datasets: [{ data: top.map(t=>t.total_size||0), backgroundColor: COLORS, borderWidth: 0 }] },
            options: { responsive: true, cutout: '60%', plugins: { legend: { position: 'right', labels: { boxWidth: 10, padding: 6, font: { size: 11 } } } } }
        });
    }
    destroyChart('types-count-chart');
    if (top.length) {
        chartInstances['types-count-chart'] = new Chart(document.getElementById('types-count-chart'), {
            type: 'bar', data: { labels: top.map(t=>'.'+t.extension), datasets: [{ label: 'Dosya Sayisi', data: top.map(t=>t.file_count), backgroundColor: COLORS.map(c=>c+'99'), borderColor: COLORS, borderWidth: 1, borderRadius: 4 }] },
            options: { indexAxis: 'y', responsive: true, plugins: { legend: { display: false } }, scales: { x: { beginAtZero: true, grid: { color: '#1e293b' } }, y: { grid: { display: false } } } }
        });
    }

    // Risk badge helper for types
    const riskyExts = ['exe','bat','ps1','vbs','cmd','msi','scr','com','js','wsf'];
    const largeMediaExts = ['mp4','avi','mkv','iso','zip','rar','7z','tar','gz','bak'];
    function typeRiskBadge(ext, totalSize) {
        if (riskyExts.includes(ext.toLowerCase())) return '<span style="display:inline-block;padding:2px 8px;border-radius:10px;font-size:10px;font-weight:700;background:rgba(239,68,68,0.15);color:#ef4444;margin-left:6px">RISKLI</span>';
        if (largeMediaExts.includes(ext.toLowerCase()) && totalSize > 1073741824) return '<span style="display:inline-block;padding:2px 8px;border-radius:10px;font-size:10px;font-weight:700;background:rgba(234,179,8,0.15);color:#eab308;margin-left:6px">BUYUK</span>';
        return '';
    }

    document.getElementById('types-table').innerHTML = `
        <thead><tr><th>Uzanti</th><th>Risk</th><th>Sayi</th><th>Toplam</th><th>Ortalama</th><th>Min</th><th>Max</th></tr></thead>
        <tbody>${types.slice(0,30).map(t => `<tr class="clickable-row" onclick="showDrilldown('.${t.extension} Dosyalari', '/drilldown/type/${sid}?extension=${encodeURIComponent(t.extension)}', {source_id:${sid}, filter_type:'type', extension:'${t.extension}'})"><td><strong>.${t.extension}</strong></td><td>${typeRiskBadge(t.extension, t.total_size||0)}</td><td>${formatNum(t.file_count)}</td><td>${t.total_size_formatted}</td><td>${t.avg_size_formatted}</td><td>${t.min_size_formatted}</td><td>${t.max_size_formatted}</td></tr>`).join('')}</tbody>
    `;
}

// ═══════════════════════════════════════════════════
// SIZES
// ═══════════════════════════════════════════════════
async function loadSizes() {
    const sid = document.getElementById('sizes-source').value;
    if (!sid) return;

    // Issue #181 Track B2: try partial summary first while scan is running.
    const ps = await _fetchPartialSummaryV2(sid);
    const wasPartial = !!_psv2PrevWasPartial['sizes'];
    if (ps && ps.scan_state && ps.scan_state !== 'completed') {
        _psv2PrevWasPartial['sizes'] = true;
        _psv2ShowBanner('sizes-partial-banner', ps.scan_state, ps.progress);
        const sb = (ps.summary && ps.summary.size_buckets) || {};
        const bucketOrder = ['<1MB', '1-10MB', '10-100MB', '100-1GB', '>1GB'];
        const labels = bucketOrder.filter(k => sb.hasOwnProperty(k));
        if (labels.length) {
            const counts = labels.map(k => sb[k] || 0);
            document.getElementById('sizes-cards').innerHTML = labels.map((k, i) => `
                <div class="card" style="border-top:3px solid ${COLORS[i%COLORS.length]}">
                    <div class="card-label">${escapeHtml(k)}</div>
                    <div class="card-value" style="font-size:22px">${formatNum(counts[i])}</div>
                    <div class="card-sub">Kismi veri</div>
                </div>`).join('');
            destroyChart('sizes-chart');
            chartInstances['sizes-chart'] = new Chart(document.getElementById('sizes-chart'), {
                type: 'bar', data: { labels, datasets: [{ label: 'Dosya Sayisi', data: counts, backgroundColor: COLORS.map(c=>c+'99'), borderColor: COLORS, borderWidth: 1, borderRadius: 6 }] },
                options: { responsive: true, plugins: { legend: { display: false } }, scales: { y: { beginAtZero: true, grid: { color: '#1e293b' } }, x: { grid: { display: false } } } }
            });
            document.getElementById('sizes-table').innerHTML = `
                <thead><tr><th>Kategori</th><th>Dosya Sayisi</th></tr></thead>
                <tbody>${labels.map((k, i) => `<tr><td>${escapeHtml(k)}</td><td>${formatNum(counts[i])}</td></tr>`).join('')}</tbody>
            `;
        } else {
            document.getElementById('sizes-cards').innerHTML = '<div style="padding:20px;color:var(--text-muted)">Kismi veri yok \u2014 Tarama tamamlandiktan sonra goruntulenecek</div>';
        }
        _psv2StartPoll(sid, 'sizes', loadSizes);
        return;
    }

    if (wasPartial && ps && ps.scan_state === 'completed') {
        notify('Tarama tamamlandi, Boyut Dagilimi guncellendi', 'success');
    }
    _psv2PrevWasPartial['sizes'] = false;
    _psv2HideBanner('sizes-partial-banner');
    _psv2StopPoll();

    const data = await api(`/reports/sizes/${sid}`);
    const sizes = data.sizes || [];

    document.getElementById('sizes-cards').innerHTML = sizes.map((s, i) => {
        const maxParam = s.max_bytes != null ? `&max_bytes=${s.max_bytes}` : '';
        return `
        <div class="card clickable-row" style="border-top:3px solid ${COLORS[i%COLORS.length]}"
             onclick="showDrilldown('${s.label}', '/drilldown/size/${sid}?min_bytes=${s.min_bytes||0}${maxParam}',
             {source_id:${sid}, filter_type:'size', min_bytes:${s.min_bytes||0}${s.max_bytes!=null?`, max_bytes:${s.max_bytes}`:''}})">
            <div class="card-label">${s.label}</div>
            <div class="card-value" style="font-size:22px">${formatNum(s.file_count)}</div>
            <div class="card-sub">${s.total_size_formatted}</div>
        </div>`;
    }).join('');

    destroyChart('sizes-chart');
    chartInstances['sizes-chart'] = new Chart(document.getElementById('sizes-chart'), {
        type: 'bar', data: { labels: sizes.map(s=>s.label), datasets: [
            { label: 'Dosya Sayisi', data: sizes.map(s=>s.file_count), backgroundColor: COLORS.map(c=>c+'99'), borderColor: COLORS, borderWidth: 1, borderRadius: 6 }
        ] }, options: { responsive: true, plugins: { legend: { display: false } }, scales: { y: { beginAtZero: true, grid: { color: '#1e293b' } }, x: { grid: { display: false } } } }
    });

    document.getElementById('sizes-table').innerHTML = `
        <thead><tr><th>Kategori</th><th>Aralik</th><th>Dosya Sayisi</th><th>Toplam Boyut</th></tr></thead>
        <tbody>${sizes.map(s => {
            const maxParam = s.max_bytes != null ? `&max_bytes=${s.max_bytes}` : '';
            return `<tr class="clickable-row" onclick="showDrilldown('${s.label}', '/drilldown/size/${sid}?min_bytes=${s.min_bytes||0}${maxParam}', {source_id:${sid}, filter_type:'size', min_bytes:${s.min_bytes||0}${s.max_bytes!=null?`, max_bytes:${s.max_bytes}`:''}})"><td>${s.label}</td><td>${s.range_formatted || '-'}</td><td>${formatNum(s.file_count)}</td><td>${s.total_size_formatted}</td></tr>`;
        }).join('')}</tbody>
    `;
}

// ═══════════════════════════════════════════════════
// TREEMAP (D3.js)
// ═══════════════════════════════════════════════════
async function loadTreemap() {
    const sid = document.getElementById('treemap-source').value;
    if (!sid) return;

    // Issue #181 Track B2: try partial summary first while scan is running.
    const ps = await _fetchPartialSummaryV2(sid);
    const wasPartial = !!_psv2PrevWasPartial['treemap'];
    if (ps && ps.scan_state && ps.scan_state !== 'completed') {
        _psv2PrevWasPartial['treemap'] = true;
        _psv2ShowBanner('treemap-partial-banner', ps.scan_state, ps.progress);
        const dirs = (ps.summary && ps.summary.by_directory) || [];
        if (dirs.length) {
            treemapData = {
                name: 'Root',
                children: dirs.map(d => ({
                    name: d.path || '?',
                    extension: '',
                    value: d.size_bytes || d.count || 1,
                    count: d.count || 0,
                    size: d.size_bytes || 0,
                    avg: d.count ? Math.round((d.size_bytes || 0) / d.count) : 0,
                })),
            };
            treemapZoomPath = null;
            renderTreemap();
        } else {
            d3.select('#treemap-svg').selectAll('*').remove();
            d3.select('#treemap-svg').append('text').attr('x','50%').attr('y','50%').attr('text-anchor','middle').attr('fill','#64748b').text('Kismi veri yok \u2014 Tarama tamamlandiktan sonra goruntulenecek');
        }
        _psv2StartPoll(sid, 'treemap', loadTreemap);
        return;
    }

    // scan completed or no partial data → full load.
    if (wasPartial && ps && ps.scan_state === 'completed') {
        notify('Tarama tamamlandi, Treemap Harita guncellendi', 'success');
    }
    _psv2PrevWasPartial['treemap'] = false;
    _psv2HideBanner('treemap-partial-banner');
    _psv2StopPoll();

    try {
        const data = await api(`/reports/full/${sid}`);
        treemapData = buildTreeFromTypes(data.types || [], data.summary || {});
        treemapZoomPath = null;
        renderTreemap();
    } catch(e) { console.error(e); }
}


function buildTreeFromTypes(types, summary) {
    // Build a tree: root -> extensions -> files (simulated by count/size)
    const children = types.filter(t => t.extension && t.file_count > 0).map(t => ({
        name: '.' + t.extension,
        extension: t.extension,
        value: t.total_size || t.file_count,
        count: t.file_count,
        size: t.total_size || 0,
        avg: t.avg_size || 0,
    }));
    return { name: 'Root', children };
}

function renderTreemap() {
    const container = document.getElementById('treemap-container');
    const svg = d3.select('#treemap-svg');
    svg.selectAll('*').remove();

    if (!treemapData || !treemapData.children || !treemapData.children.length) {
        svg.append('text').attr('x','50%').attr('y','50%').attr('text-anchor','middle').attr('fill','#64748b').text('Tarama verisi yok. Once bir tarama calistirin.');
        return;
    }

    const controlsH = 80; // space for controls + breadcrumb
    const w = container.clientWidth;
    const h = container.clientHeight - controlsH;

    // Issue #194 update #7: when renderTreemap fires while page-treemap
    // is hidden (display:none) — e.g. background scan-complete refresh
    // while operator is on a different page — clientWidth/Height are 0
    // and ``h = 0 - 80 = -80``. d3 throws
    //   <svg> attribute viewBox: A negative value is not valid. ("0 0 0 -80")
    // Skip the render entirely; the next showPage('treemap') will trigger
    // it again with real dimensions.
    if (w <= 0 || h <= 0) {
        return;
    }

    svg.attr('viewBox', `0 0 ${w} ${h}`);

    const colorMode = document.getElementById('treemap-color').value;
    const sizeMode = document.getElementById('treemap-size').value;

    const root = d3.hierarchy(treemapData)
        .sum(d => sizeMode === 'count' ? (d.count || 1) : (d.value || d.size || 1))
        .sort((a, b) => b.value - a.value);

    d3.treemap().size([w, h]).paddingInner(2).paddingOuter(4).round(true)(root);

    const colorScale = colorMode === 'extension'
        ? d3.scaleOrdinal(d3.schemeTableau10)
        : colorMode === 'age'
            ? d3.scaleSequential(d3.interpolateYlOrRd).domain([0, root.leaves().length])
            : d3.scaleSequential(d3.interpolateBlues).domain([0, d3.max(root.leaves(), d => d.value)]);

    const tooltip = document.getElementById('treemap-tooltip');

    const leaf = svg.selectAll('g').data(root.leaves()).join('g').attr('transform', d => `translate(${d.x0},${d.y0})`);

    leaf.append('rect')
        .attr('width', d => Math.max(0, d.x1 - d.x0))
        .attr('height', d => Math.max(0, d.y1 - d.y0))
        .attr('fill', (d, i) => colorMode === 'extension' ? colorScale(d.data.extension || d.data.name) : colorMode === 'size' ? colorScale(d.value) : colorScale(i))
        .attr('rx', 2).attr('opacity', 0.85)
        .style('cursor', 'pointer')
        .on('mousemove', (event, d) => {
            tooltip.style.display = 'block';
            tooltip.style.left = (event.clientX + 12) + 'px';
            tooltip.style.top = (event.clientY - 10) + 'px';
            tooltip.innerHTML = `<div class="tt-name">${escapeHtml(d.data.name)}</div>
                <div class="tt-row"><span>Dosya Sayisi</span><span>${formatNum(d.data.count)}</span></div>
                <div class="tt-row"><span>Toplam Boyut</span><span>${formatSize(d.data.size)}</span></div>
                <div class="tt-row"><span>Ortalama</span><span>${formatSize(d.data.avg)}</span></div>`;
        })
        .on('mouseleave', () => { tooltip.style.display = 'none'; });

    // Labels
    leaf.append('text')
        .attr('x', 4).attr('y', 14)
        .text(d => { const w = d.x1 - d.x0; return w > 40 ? d.data.name : ''; })
        .attr('font-size', '11px').attr('fill', '#fff').attr('font-weight', '600')
        .style('pointer-events', 'none');

    leaf.append('text')
        .attr('x', 4).attr('y', 26)
        .text(d => { const w = d.x1 - d.x0; return w > 60 ? formatSize(d.data.size) : ''; })
        .attr('font-size', '9px').attr('fill', 'rgba(255,255,255,0.7)')
        .style('pointer-events', 'none');
}

function treemapZoomRoot() { treemapZoomPath = null; renderTreemap(); }

// ═══════════════════════════════════════════════════
// USERS
// ═══════════════════════════════════════════════════
async function loadUsers() {
    // Issue #181 Track B2: try partial summary first while scan is running.
    // Users page has no dedicated source select; use the first available source.
    const srcSelect = document.getElementById('freq-source') || document.getElementById('types-source') || document.getElementById('overview-source');
    const psSid = srcSelect ? srcSelect.value : (sources.length ? sources[0].id : '');
    if (psSid) {
        const ps = await _fetchPartialSummaryV2(psSid);
        const wasPartial = !!_psv2PrevWasPartial['users'];
        if (ps && ps.scan_state && ps.scan_state !== 'completed') {
            _psv2PrevWasPartial['users'] = true;
            _psv2ShowBanner('users-partial-banner', ps.scan_state, ps.progress);
            const byOwner = (ps.summary && ps.summary.by_owner) || [];
            if (byOwner.length) {
                const totalSize = byOwner.reduce((s, o) => s + (o.size_bytes || 0), 0);
                const totalFiles = byOwner.reduce((s, o) => s + (o.count || 0), 0);
                document.getElementById('user-cards').innerHTML = `
                    <div class="card accent"><div class="card-label">Dosya Sahipleri</div><div class="card-value">${byOwner.length}</div></div>
                    <div class="card success"><div class="card-label">Toplam Boyut</div><div class="card-value" style="font-size:18px">${formatSize(totalSize)}</div></div>
                    <div class="card purple"><div class="card-label">Toplam Dosya</div><div class="card-value">${formatNum(totalFiles)}</div></div>
                    <div class="card warning"><div class="card-label">Veri Kaynagi</div><div class="card-value" style="font-size:14px">Kismi Veri</div><div class="card-sub">Tarama devam ediyor</div></div>
                `;
                const topBySize = byOwner.slice(0, 8);
                destroyChart('users-donut-chart');
                if (topBySize.length) {
                    chartInstances['users-donut-chart'] = new Chart(document.getElementById('users-donut-chart'), {
                        type: 'doughnut', data: { labels: topBySize.map(o => o.owner || 'Bilinmiyor'), datasets: [{ data: topBySize.map(o => o.size_bytes || 0), backgroundColor: COLORS.slice(0, topBySize.length), borderWidth: 0 }] },
                        options: { responsive: true, cutout: '60%', plugins: { legend: { position: 'right', labels: { boxWidth: 10, padding: 6, font: { size: 11 } } },
                            tooltip: { callbacks: { label: (ctx) => ctx.label + ': ' + formatSize(ctx.raw) } } } }
                    });
                }
                destroyChart('users-top-chart');
                const topByCount = byOwner.slice(0, 10);
                if (topByCount.length) {
                    chartInstances['users-top-chart'] = new Chart(document.getElementById('users-top-chart'), {
                        type: 'bar', data: { labels: topByCount.map(o => o.owner || 'Bilinmiyor'), datasets: [{ label: 'Dosya Sayisi', data: topByCount.map(o => o.count || 0), backgroundColor: COLORS.map(c=>c+'99'), borderColor: COLORS, borderWidth: 1, borderRadius: 4 }] },
                        options: { indexAxis: 'y', responsive: true, plugins: { legend: { display: false } }, scales: { x: { beginAtZero: true, grid: { color: '#1e293b' } }, y: { grid: { display: false } } } }
                    });
                }
                document.getElementById('users-table').innerHTML = `
                    <thead><tr><th>Sahip</th><th>Dosya Sayisi</th><th>Toplam Boyut</th></tr></thead>
                    <tbody>${byOwner.map(o => `<tr><td><strong>${escapeHtml(o.owner||'Bilinmiyor')}</strong></td><td>${formatNum(o.count||0)}</td><td>${formatSize(o.size_bytes||0)}</td></tr>`).join('')}</tbody>
                `;
                document.getElementById('users-heatmap').innerHTML = '<div style="padding:20px;color:var(--text-muted);font-size:12px">Heatmap tarama tamamlandiktan sonra goruntulenecek</div>';
            } else {
                document.getElementById('user-cards').innerHTML = '<div style="padding:20px;color:var(--text-muted)">Kismi veri yok \u2014 Tarama tamamlandiktan sonra goruntulenecek</div>';
            }
            _psv2StartPoll(psSid, 'users', loadUsers);
            return;
        }

        if (wasPartial && ps && ps.scan_state === 'completed') {
            notify('Tarama tamamlandi, Kullanici Aktivite guncellendi', 'success');
        }
        _psv2PrevWasPartial['users'] = false;
        _psv2HideBanner('users-partial-banner');
        _psv2StopPoll();
    }

    try {
        const data = await api('/users/overview', { silent: true });

        if (data.source === 'file_ownership') {
            const owners = data.owners || [];
            const totalSize = owners.reduce((s, o) => s + (o.total_size || 0), 0);
            const totalFiles = owners.reduce((s, o) => s + (o.file_count || 0), 0);

            document.getElementById('user-cards').innerHTML = `
                <div class="card accent"><div class="card-label">Dosya Sahipleri</div><div class="card-value">${owners.length}</div></div>
                <div class="card success"><div class="card-label">Toplam Boyut</div><div class="card-value" style="font-size:18px">${formatSize(totalSize)}</div></div>
                <div class="card purple"><div class="card-label">Toplam Dosya</div><div class="card-value">${formatNum(totalFiles)}</div></div>
                <div class="card warning"><div class="card-label">Veri Kaynagi</div><div class="card-value" style="font-size:14px">Dosya Sahipligi</div><div class="card-sub">Event Log yok</div></div>
            `;

            // Donut chart: owner distribution by SIZE
            destroyChart('users-donut-chart');
            const topBySize = owners.slice(0, 8);
            if (topBySize.length) {
                const otherSize = totalSize - topBySize.reduce((s, o) => s + (o.total_size || 0), 0);
                const labels = topBySize.map(o => o.owner || 'Bilinmiyor');
                const sizes = topBySize.map(o => o.total_size || 0);
                if (otherSize > 0 && owners.length > 8) { labels.push('Diger'); sizes.push(otherSize); }
                chartInstances['users-donut-chart'] = new Chart(document.getElementById('users-donut-chart'), {
                    type: 'doughnut', data: { labels, datasets: [{ data: sizes, backgroundColor: COLORS.slice(0, labels.length), borderWidth: 0 }] },
                    options: { responsive: true, cutout: '60%', plugins: { legend: { position: 'right', labels: { boxWidth: 10, padding: 6, font: { size: 11 } } },
                        tooltip: { callbacks: { label: (ctx) => ctx.label + ': ' + formatSize(ctx.raw) + ' (' + ((ctx.raw/Math.max(totalSize,1))*100).toFixed(1) + '%)' } } } }
                });
            }

            // Bar chart: top 10 owners by file count
            destroyChart('users-top-chart');
            const topByCount = owners.slice(0, 10);
            if (topByCount.length) {
                chartInstances['users-top-chart'] = new Chart(document.getElementById('users-top-chart'), {
                    type: 'bar', data: { labels: topByCount.map(o=>o.owner||'Bilinmiyor'), datasets: [{ label: 'Dosya Sayisi', data: topByCount.map(o=>o.file_count||0), backgroundColor: COLORS.map(c=>c+'99'), borderColor: COLORS, borderWidth: 1, borderRadius: 4 }] },
                    options: { indexAxis: 'y', responsive: true, plugins: { legend: { display: false } }, scales: { x: { beginAtZero: true, grid: { color: '#1e293b' } }, y: { grid: { display: false } } } }
                });
            }

            // Heatmap: empty
            document.getElementById('users-heatmap').innerHTML = '<div style="padding:20px;color:var(--text-muted);font-size:12px">Event log verisi yok - heatmap kullanilamiyor</div>';

            // Owner table with drill-down + archive button
            const srcSelect = document.getElementById('freq-source') || document.getElementById('types-source') || document.getElementById('overview-source');
            const defaultSid = srcSelect ? srcSelect.value : (sources.length ? sources[0].id : '');
            document.getElementById('users-table').innerHTML = `
                <thead><tr><th>Sahip</th><th>Dosya Sayisi</th><th>Toplam Boyut</th><th>Yuzde</th><th>Islem</th></tr></thead>
                <tbody>${owners.length ? owners.map(o => {
                    const pct = totalSize > 0 ? ((o.total_size||0)/totalSize*100).toFixed(1) : '0';
                    const ownerEsc = escapeHtml(o.owner || '');
                    return `<tr class="clickable-row" onclick="drilldownOwner('${ownerEsc}', '${defaultSid}')">
                        <td><strong>${escapeHtml(o.owner || 'Bilinmiyor')}</strong></td>
                        <td>${formatNum(o.file_count)}</td><td>${formatSize(o.total_size)}</td>
                        <td><div style="display:flex;align-items:center;gap:8px"><div style="flex:1;background:var(--bg-primary);border-radius:3px;height:6px;overflow:hidden"><div style="width:${pct}%;height:100%;background:var(--accent);border-radius:3px"></div></div><span style="font-size:11px">${pct}%</span></div></td>
                        <td style="display:flex;gap:4px"><button class="btn btn-sm btn-outline" onclick="event.stopPropagation();drilldownOwner('${ownerEsc}', '${defaultSid}')">Dosyalar</button><button class="btn btn-sm btn-danger" onclick="event.stopPropagation();showDrilldown('Arsivle: ${ownerEsc}', '/drilldown/owner/${defaultSid}?owner=${encodeURIComponent(o.owner||'')}', {source_id:${parseInt(defaultSid)||0}, filter_type:'owner', owner:'${ownerEsc}'})">Arsivle</button></td>
                    </tr>`;
                }).join('') : '<tr><td colspan="5" style="text-align:center;padding:40px;color:var(--text-muted)">Sahiplik verisi bulunamadi. config.yaml dosyasinda read_owner: true yapin.</td></tr>'}</tbody>
            `;
        } else {
            // Normal: event log data available
            const topUsers = data.top_users || [];

            document.getElementById('user-cards').innerHTML = `
                <div class="card accent"><div class="card-label">Aktif Kullanici</div><div class="card-value">${topUsers.length}</div></div>
            `;

            destroyChart('users-donut-chart');
            destroyChart('users-top-chart');
            if (topUsers.length) {
                // Donut by access count
                chartInstances['users-donut-chart'] = new Chart(document.getElementById('users-donut-chart'), {
                    type: 'doughnut', data: { labels: topUsers.slice(0,8).map(u=>u.username), datasets: [{ data: topUsers.slice(0,8).map(u=>u.access_count||u.count||0), backgroundColor: COLORS.slice(0,8), borderWidth: 0 }] },
                    options: { responsive: true, cutout: '60%', plugins: { legend: { position: 'right', labels: { boxWidth: 10, padding: 6, font: { size: 11 } } } } }
                });
                chartInstances['users-top-chart'] = new Chart(document.getElementById('users-top-chart'), {
                    type: 'bar', data: { labels: topUsers.slice(0,10).map(u=>u.username), datasets: [{ label: 'Erisim', data: topUsers.slice(0,10).map(u=>u.access_count||u.count||0), backgroundColor: COLORS.map(c=>c+'99'), borderColor: COLORS, borderWidth: 1, borderRadius: 4 }] },
                    options: { indexAxis: 'y', responsive: true, plugins: { legend: { display: false } }, scales: { x: { beginAtZero: true, grid: { color: '#1e293b' } }, y: { grid: { display: false } } } }
                });
            }

            const heatData = data.heatmap || await api('/users/heatmap', { silent: true }).catch(()=>({matrix:[], days:[], max_value:1}));
            renderHeatmap(heatData);

            document.getElementById('users-table').innerHTML = `
                <thead><tr><th>Kullanici</th><th>Erisim</th><th>Islem</th></tr></thead>
                <tbody>${topUsers.slice(0,20).map(u => { const userEsc = escapeHtml(u.username||''); return `<tr><td><strong>${userEsc}</strong></td><td>${formatNum(u.access_count||u.count||0)}</td>
                    <td style="display:flex;gap:4px">
                        <button class="btn btn-sm btn-outline" onclick="loadUserDetail('${userEsc}')">Detay</button>
                        <button class="btn btn-sm btn-outline" style="border-color:var(--accent);color:var(--accent)" onclick="loadUserEfficiency('${userEsc}')">Verimlilik</button>
                    </td></tr>`; }).join('')}</tbody>
            `;
        }
    } catch(e) { console.error(e); }
}

function renderHeatmap(data) {
    const el = document.getElementById('users-heatmap');
    if (!data || !data.matrix || !data.matrix.length) { el.innerHTML = '<div style="padding:20px;color:var(--text-muted);font-size:12px">Heatmap verisi yok</div>'; return; }
    const days = data.days || ['Pzt','Sal','Car','Per','Cum','Cmt','Paz'];
    const max = data.max_value || 1;

    let html = '<div class="heatmap-label"></div>';
    for (let h = 0; h < 24; h++) html += `<div class="heatmap-hour">${h}</div>`;

    for (let d = 0; d < 7; d++) {
        html += `<div class="heatmap-label">${days[d]}</div>`;
        for (let h = 0; h < 24; h++) {
            const v = (data.matrix[d] && data.matrix[d][h]) || 0;
            const intensity = v / max;
            const color = intensity === 0 ? 'var(--bg-primary)' : `rgba(59,130,246,${0.15 + intensity * 0.85})`;
            html += `<div class="heatmap-cell" style="background:${color}" title="${days[d]} ${h}:00 - ${v} erisim"></div>`;
        }
    }
    el.innerHTML = html;
}

async function loadUserDetail(username) {
    try {
        const data = await api(`/users/${username}/detail`);
        const ad = data.ad || {};
        const s = data.summary || {};

        // AD source etiketi (tooltip icin)
        const adSourceLabel = {
            live: 'canli AD sorgusu',
            cache: 'yerel cache (taze)',
            'stale-cache': 'yerel cache (eski - AD erisilemedi)',
        }[ad.source] || 'AD devre disi';

        const adBlock = ad.found || ad.email ? `
            <div style="background:var(--bg-primary);padding:12px;border-radius:8px;margin-bottom:16px;border-left:3px solid var(--accent)">
                <div style="font-size:11px;color:var(--text-muted);text-transform:uppercase;letter-spacing:0.5px;margin-bottom:4px">Active Directory</div>
                ${ad.display_name ? `<div style="font-size:15px;font-weight:600">${escapeHtml(ad.display_name)}</div>` : ''}
                ${ad.email ? `<div style="font-size:13px;color:var(--accent)"><a href="mailto:${encodeURIComponent(ad.email)}" style="color:inherit">${escapeHtml(ad.email)}</a></div>` : '<div style="font-size:12px;color:var(--text-muted)">E-posta bulunamadi</div>'}
                <div style="font-size:10px;color:var(--text-muted);margin-top:4px" title="${escapeHtml(adSourceLabel)}">Kaynak: ${escapeHtml(adSourceLabel)}</div>
            </div>
        ` : `
            <div style="background:var(--bg-primary);padding:12px;border-radius:8px;margin-bottom:16px;border-left:3px solid var(--text-muted)">
                <div style="font-size:11px;color:var(--text-muted);text-transform:uppercase;letter-spacing:0.5px;margin-bottom:4px">Active Directory</div>
                <div style="font-size:13px;color:var(--text-muted)">AD bilgisi yok</div>
                <div style="font-size:10px;color:var(--text-muted);margin-top:4px">${escapeHtml(adSourceLabel)}</div>
            </div>
        `;

        const modalId = 'modal-user-detail';
        const body = `
            <h3 style="margin:0 0 16px 0">Kullanici Detay: ${escapeHtml(username)}</h3>
            ${adBlock}
            <div class="cards">
                <div class="card accent"><div class="card-label">Toplam Erisim</div><div class="card-value">${formatNum(s.total_access||0)}</div></div>
                <div class="card success"><div class="card-label">Benzersiz Dosya</div><div class="card-value">${formatNum(s.unique_files||0)}</div></div>
                <div class="card purple"><div class="card-label">Aktif Gun</div><div class="card-value">${formatNum(s.active_days||0)}</div></div>
                <div class="card warning"><div class="card-label">Veri Boyutu</div><div class="card-value" style="font-size:16px">${s.total_data_formatted||'0 B'}</div></div>
            </div>
            <div style="display:flex;gap:8px;margin-top:12px;font-size:12px">
                <div style="flex:1;padding:10px;background:var(--bg-primary);border-radius:6px"><strong>Okuma:</strong> ${formatNum(s.reads||0)}</div>
                <div style="flex:1;padding:10px;background:var(--bg-primary);border-radius:6px"><strong>Yazma:</strong> ${formatNum(s.writes||0)}</div>
                <div style="flex:1;padding:10px;background:var(--bg-primary);border-radius:6px"><strong>Silme:</strong> ${formatNum(s.deletes||0)}</div>
            </div>
            <div style="text-align:right;margin-top:16px">
                <button class="btn btn-outline" onclick="closeModal('${modalId}')">Kapat</button>
            </div>
        `;

        let modal = document.getElementById(modalId);
        if (!modal) {
            modal = document.createElement('div');
            modal.id = modalId;
            modal.className = 'modal-overlay';
            modal.innerHTML = '<div class="modal" style="max-width:700px;width:90%"></div>';
            modal.onclick = (e) => { if (e.target === modal) closeModal(modalId); };
            document.body.appendChild(modal);
        }
        modal.querySelector('.modal').innerHTML = body;
        modal.classList.add('active');
    } catch(e) { notify(e.message, 'error'); }
}

// Verimlilik skoru modali — PR B (efficiency score)
async function loadUserEfficiency(username) {
    try {
        const d = await api(`/users/${username}/efficiency`);
        const score = d.score || 0;
        const grade = d.grade || 'A';
        // Renk: A/B yesil, C sari, D/E kirmizi
        const gradeColor = {A:'var(--success)', B:'var(--success)', C:'var(--warning)', D:'var(--danger)', E:'var(--danger)'}[grade] || 'var(--text-primary)';

        const factorsHtml = (d.factors || []).length === 0 ?
            '<p style="color:var(--text-muted);font-size:13px">Hicbir uyumsuzluk tespit edilmedi. Iyi gidiyorsunuz.</p>' :
            (d.factors || []).map(f => `
                <div style="margin-bottom:10px;padding:10px;background:var(--bg-primary);border-radius:6px">
                    <div style="display:flex;justify-content:space-between;align-items:center;font-size:13px">
                        <strong>${escapeHtml(f.label)}</strong>
                        <span style="color:var(--danger);font-weight:600">-${f.penalty} puan</span>
                    </div>
                    <div style="font-size:11px;color:var(--text-muted);margin-top:4px">${formatNum(f.count)} ${f.name === 'dormant' ? 'gun' : 'dosya'} (max ceza: -${f.max})</div>
                </div>
            `).join('');

        const suggestionsHtml = (d.suggestions || []).map(s => `<li style="margin-bottom:6px">${escapeHtml(s)}</li>`).join('');

        const body = `
            <h3 style="margin:0 0 16px 0">Verimlilik Skoru: ${escapeHtml(username)}</h3>

            <div style="display:flex;gap:20px;align-items:center;background:var(--bg-primary);padding:20px;border-radius:8px;margin-bottom:20px">
                <div style="flex:0 0 100px;text-align:center">
                    <div style="font-size:56px;font-weight:700;color:${gradeColor};line-height:1">${score}</div>
                    <div style="font-size:14px;color:var(--text-muted);margin-top:4px">/ 100</div>
                </div>
                <div style="flex:0 0 80px;text-align:center">
                    <div style="font-size:48px;font-weight:700;color:${gradeColor};line-height:1">${escapeHtml(grade)}</div>
                    <div style="font-size:11px;color:var(--text-muted);margin-top:4px">Sinif</div>
                </div>
                <div style="flex:1;font-size:12px;color:var(--text-muted)">
                    <div><strong>Toplam dosya:</strong> ${formatNum(d.total_files||0)}</div>
                    <div><strong>Toplam boyut:</strong> ${formatSize(d.total_size||0)}</div>
                    <div><strong>Toplam ceza:</strong> -${d.total_penalty||0} puan</div>
                    <div style="font-size:10px;margin-top:4px">Son guncelleme: ${d.computed_at ? new Date(d.computed_at).toLocaleString() : '-'}</div>
                </div>
            </div>

            <h4 style="font-size:13px;margin:0 0 8px 0;color:var(--text-muted);text-transform:uppercase;letter-spacing:0.5px">Faktorler</h4>
            ${factorsHtml}

            ${suggestionsHtml ? `
                <h4 style="font-size:13px;margin:16px 0 8px 0;color:var(--text-muted);text-transform:uppercase;letter-spacing:0.5px">Oneriler</h4>
                <ul style="font-size:13px;line-height:1.6;padding-left:20px;margin:0">${suggestionsHtml}</ul>
            ` : ''}

            <div style="text-align:right;margin-top:20px">
                <button class="btn btn-outline" onclick="closeModal('modal-user-efficiency')">Kapat</button>
            </div>
        `;

        let modal = document.getElementById('modal-user-efficiency');
        if (!modal) {
            modal = document.createElement('div');
            modal.id = 'modal-user-efficiency';
            modal.className = 'modal-overlay';
            modal.innerHTML = '<div class="modal" style="max-width:700px;width:90%"></div>';
            modal.onclick = (e) => { if (e.target === modal) closeModal('modal-user-efficiency'); };
            document.body.appendChild(modal);
        }
        modal.querySelector('.modal').innerHTML = body;
        modal.classList.add('active');
    } catch(e) { notify(e.message || 'Verimlilik skoru alinamadi', 'error'); }
}

// ═══════════════════════════════════════════════════
// ANOMALIES
// ═══════════════════════════════════════════════════
async function loadAnomalies() {
    // Issue #181 Track B2: try partial summary first while scan is running.
    const srcSelect = document.getElementById('freq-source') || document.getElementById('types-source') || document.getElementById('overview-source');
    const psSid = srcSelect ? srcSelect.value : (sources.length ? sources[0].id : '');
    if (psSid) {
        const ps = await _fetchPartialSummaryV2(psSid);
        const wasPartial = !!_psv2PrevWasPartial['anomalies'];
        if (ps && ps.scan_state && ps.scan_state !== 'completed') {
            _psv2PrevWasPartial['anomalies'] = true;
            _psv2ShowBanner('anomalies-partial-banner', ps.scan_state, ps.progress);
            const anom = (ps.summary && ps.summary.anomalies_so_far) || {};
            const naming = anom.naming || 0;
            const extension = anom.extension || 0;
            const ransomware = anom.ransomware || 0;
            document.getElementById('anomaly-cards').innerHTML = `
                <div class="card danger"><div class="card-label">Adlandirma</div><div class="card-value">${formatNum(naming)}</div><div class="card-sub">Kismi veri</div></div>
                <div class="card warning"><div class="card-label">Uzanti Anomalisi</div><div class="card-value">${formatNum(extension)}</div><div class="card-sub">Kismi veri</div></div>
                <div class="card accent"><div class="card-label">Ransomware Belirtisi</div><div class="card-value">${formatNum(ransomware)}</div><div class="card-sub">Kismi veri</div></div>
            `;
            document.getElementById('anomaly-table').innerHTML = `
                <thead><tr><th>Tur</th><th>Sayim</th><th>Not</th></tr></thead>
                <tbody>
                    <tr><td>Adlandirma</td><td>${formatNum(naming)}</td><td style="color:var(--text-muted)">Tarama tamamlandiktan sonra detay goruntulenecek</td></tr>
                    <tr><td>Uzanti Anomalisi</td><td>${formatNum(extension)}</td><td style="color:var(--text-muted)">Tarama tamamlandiktan sonra detay goruntulenecek</td></tr>
                    <tr><td>Ransomware Belirtisi</td><td>${formatNum(ransomware)}</td><td style="color:var(--text-muted)">Tarama tamamlandiktan sonra detay goruntulenecek</td></tr>
                </tbody>
            `;
            _psv2StartPoll(psSid, 'anomalies', loadAnomalies);
            return;
        }

        if (wasPartial && ps && ps.scan_state === 'completed') {
            notify('Tarama tamamlandi, Anomaliler guncellendi', 'success');
        }
        _psv2PrevWasPartial['anomalies'] = false;
        _psv2HideBanner('anomalies-partial-banner');
        _psv2StopPoll();
    }

    try {
        const data = await api('/anomalies', { silent: true });
        const alerts = Array.isArray(data) ? data : [];

        const badge = document.getElementById('anomaly-badge');
        if (alerts.length > 0) { badge.style.display = 'inline'; badge.textContent = alerts.length; }
        else { badge.style.display = 'none'; }

        const crit = alerts.filter(a => a.severity === 'critical').length;
        const warn = alerts.filter(a => a.severity === 'warning').length;

        document.getElementById('anomaly-cards').innerHTML = `
            <div class="card danger"><div class="card-label">Kritik</div><div class="card-value">${crit}</div></div>
            <div class="card warning"><div class="card-label">Uyari</div><div class="card-value">${warn}</div></div>
            <div class="card accent"><div class="card-label">Toplam</div><div class="card-value">${alerts.length}</div></div>
        `;

        document.getElementById('anomaly-table').innerHTML = `
            <thead><tr><th>Tur</th><th>Ciddiyet</th><th>Kullanici</th><th>Aciklama</th></tr></thead>
            <tbody>${alerts.length ? alerts.map(a => `<tr><td>${escapeHtml(a.alert_type||a.type||'')}</td><td><span class="badge badge-${a.severity==='critical'?'danger':'warning'}">${escapeHtml(a.severity||'')}</span></td><td>${escapeHtml(a.username||'-')}</td><td>${escapeHtml(a.description||'')}</td></tr>`).join('') : '<tr><td colspan="4" style="text-align:center;padding:40px;color:var(--text-muted)">Anomali bulunamadi ✅</td></tr>'}</tbody>
        `;
    } catch(e) { console.error(e); }
}

// ═══════════════════════════════════════════════════
// ARCHIVE
// ═══════════════════════════════════════════════════
async function loadArchive() {
    try {
        const stats = await api('/archive/stats', { silent: true });
        document.getElementById('archive-cards').innerHTML = `
            <div class="card accent"><div class="card-label">Arsivlenen</div><div class="card-value">${formatNum(stats.total_archived)}</div></div>
            <div class="card success"><div class="card-label">Su An Arsivde</div><div class="card-value">${formatNum(stats.currently_archived)}</div></div>
            <div class="card warning"><div class="card-label">Geri Yuklenen</div><div class="card-value">${formatNum(stats.total_restored)}</div></div>
            <div class="card purple"><div class="card-label">Arsiv Boyutu</div><div class="card-value">${formatSize(stats.archived_size)}</div></div>
        `;
    } catch(e) {}

    // Islem gecmisi yukle
    try {
        const ops = await api('/archive/operations', { silent: true });
        const opTable = document.getElementById('archive-operations-table');
        if (ops && ops.length) {
            const statusBadge = (s) => {
                const colors = { completed: 'badge-success', running: 'badge-info', failed: 'badge-danger', partial: 'badge-warning' };
                return `<span class="badge ${colors[s]||'badge-info'}">${s}</span>`;
            };
            const typeBadge = (t) => t === 'archive' ? '<span class="badge badge-accent">Arsiv</span>' : '<span class="badge badge-success">Geri Yukleme</span>';
            opTable.innerHTML = `
                <thead><tr><th>ID</th><th>Tarih</th><th>Tur</th><th>Tetikleyen</th><th>Dosya</th><th>Boyut</th><th>Durum</th><th>Islem</th></tr></thead>
                <tbody>${ops.map(o => `<tr style="cursor:pointer" onclick="showOperationDetail(${o.id})">
                    <td>${o.id}</td>
                    <td>${(o.started_at||'').substring(0,19)}</td>
                    <td>${typeBadge(o.operation_type)}</td>
                    <td><span style="font-size:11px">${o.trigger_type||'-'} ${o.trigger_detail ? '('+o.trigger_detail+')' : ''}</span></td>
                    <td>${formatNum(o.total_files)}</td>
                    <td>${formatSize(o.total_size)}</td>
                    <td>${statusBadge(o.status)}</td>
                    <td>${o.operation_type==='archive' && o.status==='completed' ? `<button class="btn btn-sm btn-success" onclick="event.stopPropagation();restoreByOperation(${o.id})">Geri Yukle</button>` : ''}</td>
                </tr>`).join('')}</tbody>
            `;
        } else {
            opTable.innerHTML = '<div style="text-align:center;padding:40px;color:var(--text-muted);font-size:12px">Henuz arsiv islemi yapilmamis</div>';
        }
    } catch(e) { console.error('Operations load error:', e); }
}

async function showOperationDetail(opId) {
    try {
        const detail = await api(`/archive/operations/${opId}`);
        const files = detail.files || [];
        const filesHtml = files.length ? files.map(f => `
            <tr>
                <td>${escapeHtml(f.file_name)}</td>
                <td style="max-width:250px;word-break:break-all;font-size:11px">${escapeHtml(f.original_path)}</td>
                <td>${formatSize(f.file_size)}</td>
                <td>${escapeHtml((f.archived_at||'').substring(0,19))}</td>
                <td>${f.restored_at ? '<span class="badge badge-success">Geri Yuklendi</span>' : '<span class="badge badge-info">Arsivde</span>'}</td>
            </tr>
        `).join('') : '<tr><td colspan="5" style="text-align:center;padding:20px;color:var(--text-muted)">Dosya detayi bulunamadi</td></tr>';

        const modalContent = `
            <div style="padding:20px">
                <h3 style="margin:0 0 16px 0">Islem Detayi #${detail.id}</h3>
                <div class="cards" style="margin-bottom:16px">
                    <div class="card accent"><div class="card-label">Tur</div><div class="card-value" style="font-size:16px">${escapeHtml(detail.operation_type)}</div></div>
                    <div class="card warning"><div class="card-label">Dosya Sayisi</div><div class="card-value">${formatNum(detail.total_files)}</div></div>
                    <div class="card purple"><div class="card-label">Toplam Boyut</div><div class="card-value">${formatSize(detail.total_size)}</div></div>
                </div>
                <div style="font-size:12px;color:var(--text-secondary);margin-bottom:12px">
                    <strong>Baslangic:</strong> ${escapeHtml(detail.started_at || '-')} | <strong>Bitis:</strong> ${escapeHtml(detail.completed_at || '-')}<br>
                    <strong>Tetikleyen:</strong> ${escapeHtml(detail.trigger_type || '-')} ${detail.trigger_detail ? '('+escapeHtml(detail.trigger_detail)+')' : ''} | <strong>Yapan:</strong> ${escapeHtml(detail.performed_by || '-')}
                    ${detail.error_message ? `<br><strong style="color:var(--danger)">Hata:</strong> ${escapeHtml(detail.error_message)}` : ''}
                </div>
                <div style="max-height:300px;overflow-y:auto">
                    <table style="width:100%;font-size:12px">
                        <thead><tr><th>Dosya</th><th>Orijinal Yol</th><th>Boyut</th><th>Tarih</th><th>Durum</th></tr></thead>
                        <tbody>${filesHtml}</tbody>
                    </table>
                </div>
                <div style="display:flex;gap:12px;justify-content:flex-end;margin-top:16px">
                    <button class="btn btn-outline" onclick="closeModal('modal-op-detail')">Kapat</button>
                    ${detail.operation_type==='archive' && detail.status==='completed' ? `<button class="btn btn-primary" onclick="restoreByOperation(${detail.id});closeModal('modal-op-detail')">Tumunu Geri Yukle</button>` : ''}
                </div>
            </div>
        `;

        let modal = document.getElementById('modal-op-detail');
        if (!modal) {
            modal = document.createElement('div');
            modal.id = 'modal-op-detail';
            modal.className = 'modal-overlay';
            modal.innerHTML = '<div class="modal" style="max-width:800px"></div>';
            modal.onclick = (e) => { if (e.target === modal) closeModal('modal-op-detail'); };
            document.body.appendChild(modal);
        }
        modal.querySelector('.modal').innerHTML = modalContent;
        modal.classList.add('active');
    } catch(e) {
        notify('Islem detayi yuklenemedi: ' + (e.message || e), 'error');
    }
}

async function restoreByOperation(opId) {
    if (!confirm('Bu islemdeki tum dosyalari geri yuklemek istiyor musunuz?')) return;
    notify('Geri yukleme baslatildi...', 'info');
    try {
        const result = await api(`/restore/by-operation/${opId}`, { method: 'POST' });
        if (result.restored > 0) {
            notify(`Geri yukleme tamamlandi: ${result.restored} dosya`, 'success');
        } else {
            notify('Geri yuklenecek dosya bulunamadi.', 'warning');
        }
        loadArchive();
    } catch(e) {
        notify('Geri yukleme hatasi: ' + (e.message || e), 'error');
    }
}

let restoreSelectedIds = new Set();

async function searchArchive() {
    const q = document.getElementById('archive-search').value;
    const ext = document.getElementById('archive-ext').value;
    if (!q) return;
    const data = await api(`/archive/search?q=${encodeURIComponent(q)}${ext ? '&extension='+encodeURIComponent(ext) : ''}`);
    restoreSelectedIds.clear();
    document.getElementById('archive-table').innerHTML = `
        <thead><tr><th style="width:30px"><input type="checkbox" onchange="toggleAllRestore(this.checked)"></th><th>ID</th><th>Dosya</th><th>Boyut</th><th>Tarih</th><th>Orijinal Yol</th><th>Durum</th><th>Islem</th></tr></thead>
        <tbody>${(data.results||[]).map(r => `<tr>
            <td>${!r.restored_at ? `<input type="checkbox" class="restore-check" data-id="${r.id}" onchange="updateRestoreSelection()">` : ''}</td>
            <td>${r.id}</td><td>${escapeHtml(r.file_name)}</td><td>${formatSize(r.file_size)}</td>
            <td>${escapeHtml((r.archived_at||'').substring(0,19))}</td>
            <td style="max-width:300px;word-break:break-all">${escapeHtml(r.original_path)}</td>
            <td>${r.restored_at ? '<span class="badge badge-success">Geri Yuklendi</span>' : '<span class="badge badge-info">Arsivde</span>'}</td>
            <td>${!r.restored_at ? `<button class="btn btn-sm btn-success" onclick="restoreFile(${r.id})">Geri Yukle</button>` : ''}</td>
        </tr>`).join('')}</tbody>
    `;
    updateRestoreSelection();
}

function toggleAllRestore(checked) {
    document.querySelectorAll('.restore-check').forEach(cb => { cb.checked = checked; });
    updateRestoreSelection();
}

function updateRestoreSelection() {
    restoreSelectedIds.clear();
    document.querySelectorAll('.restore-check:checked').forEach(cb => {
        restoreSelectedIds.add(parseInt(cb.dataset.id));
    });
    const actionsDiv = document.getElementById('restore-actions');
    const infoSpan = document.getElementById('restore-selected-info');
    if (restoreSelectedIds.size > 0) {
        actionsDiv.style.display = 'block';
        infoSpan.textContent = `${restoreSelectedIds.size} dosya secildi`;
    } else {
        actionsDiv.style.display = 'none';
    }
}

async function restoreFile(id) {
    if (!confirm('Bu dosyayi geri yuklemek istiyor musunuz?')) return;
    const r = await api('/archive/restore', { method: 'POST', body: JSON.stringify({ archive_id: id }) });
    notify(r.success ? 'Dosya geri yuklendi!' : ('Hata: ' + r.error), r.success ? 'success' : 'error');
    if (r.success) searchArchive();
}

async function previewBulkRestore() {
    if (restoreSelectedIds.size === 0) return;
    try {
        const data = await api('/restore/bulk', {
            method: 'POST',
            body: JSON.stringify({ archive_ids: Array.from(restoreSelectedIds), confirm: false })
        });

        const dirsHtml = (data.dirs_to_create || []).map(d => `<li style="font-size:11px;word-break:break-all">${escapeHtml(d)}</li>`).join('');
        const conflictsHtml = (data.conflicts || []).map(c => `<li style="font-size:11px">${escapeHtml(c.file_name)} - ${escapeHtml(c.original_path)}</li>`).join('');

        const modalContent = `
            <div style="padding:20px">
                <h3 style="margin:0 0 16px 0">Geri Yukleme Onizlemesi</h3>
                <div class="cards" style="margin-bottom:16px">
                    <div class="card success"><div class="card-label">Geri Yuklenecek</div><div class="card-value">${data.restorable_count}</div></div>
                    <div class="card danger"><div class="card-label">Cakisma</div><div class="card-value">${data.conflict_count}</div></div>
                    <div class="card warning"><div class="card-label">Bulunamayan</div><div class="card-value">${data.missing_count}</div></div>
                    <div class="card purple"><div class="card-label">Toplam Boyut</div><div class="card-value">${data.total_size_formatted}</div></div>
                </div>
                ${data.dirs_to_create_count > 0 ? `
                    <div style="margin-bottom:12px">
                        <h4 style="font-size:13px;margin-bottom:8px;color:var(--accent)">Olusturulacak Dizinler (${data.dirs_to_create_count})</h4>
                        <ul style="max-height:150px;overflow-y:auto;padding-left:20px;color:var(--text-secondary)">${dirsHtml}</ul>
                    </div>
                ` : ''}
                ${data.conflict_count > 0 ? `
                    <div style="margin-bottom:12px">
                        <h4 style="font-size:13px;margin-bottom:8px;color:var(--danger)">Cakismalar (atlanacak)</h4>
                        <ul style="max-height:150px;overflow-y:auto;padding-left:20px;color:var(--text-secondary)">${conflictsHtml}</ul>
                    </div>
                ` : ''}
                <div style="display:flex;gap:12px;justify-content:flex-end;margin-top:16px">
                    <button class="btn btn-outline" onclick="closeModal('modal-restore-preview')">Iptal</button>
                    ${data.restorable_count > 0 ? `<button class="btn btn-primary" onclick="executeBulkRestore();closeModal('modal-restore-preview')">Onayla ve Geri Yukle (${data.restorable_count} dosya)</button>` : ''}
                </div>
            </div>
        `;

        let modal = document.getElementById('modal-restore-preview');
        if (!modal) {
            modal = document.createElement('div');
            modal.id = 'modal-restore-preview';
            modal.className = 'modal-overlay';
            modal.onclick = (e) => { if(e.target===modal) modal.classList.remove('active'); };
            modal.innerHTML = '<div class="modal" style="max-width:700px;width:90%"></div>';
            document.body.appendChild(modal);
        }
        modal.querySelector('.modal').innerHTML = modalContent;
        modal.classList.add('active');
    } catch(e) { notify('Onizleme hatasi: ' + e.message, 'error'); }
}

async function executeBulkRestore() {
    if (restoreSelectedIds.size === 0) return;
    if (!confirm(`${restoreSelectedIds.size} dosya geri yuklenecek. Devam etmek istiyor musunuz?`)) return;

    try {
        const resp = await api('/restore/bulk', {
            method: 'POST',
            body: JSON.stringify({ archive_ids: Array.from(restoreSelectedIds), confirm: true })
        });
        notify(`${resp.restored || 0} dosya geri yuklendi (${resp.total_size_formatted || ''})`, 'success');
        if (resp.failed > 0) notify(`${resp.failed} dosya geri yuklenemedi`, 'warning');
        searchArchive();
        loadArchive();
    } catch(e) { notify('Geri yukleme hatasi: ' + e.message, 'error'); }
}

// ═══════════════════════════════════════════════════
// POLICIES
// ═══════════════════════════════════════════════════
async function loadPolicies() {
    const data = await api('/policies', { silent: true }).catch(()=>[]);
    document.getElementById('policy-table').innerHTML = `
        <thead><tr><th>ID</th><th>Ad</th><th>Kurallar</th><th>Durum</th><th>Islem</th></tr></thead>
        <tbody>${data.length ? data.map(p => `<tr><td>${p.id}</td><td><strong>${escapeHtml(p.name)}</strong></td><td style="max-width:300px;word-break:break-all;font-size:11px">${escapeHtml(JSON.stringify(p.rules_json||{}))}</td><td><span class="badge ${p.enabled?'badge-success':'badge-warning'}">${p.enabled?'Aktif':'Pasif'}</span></td><td><button class="btn btn-sm btn-danger" onclick="deletePolicy(${p.id})">Sil</button></td></tr>`).join('') : '<tr><td colspan="5" style="text-align:center;padding:40px;color:var(--text-muted)">Politika bulunamadi</td></tr>'}</tbody>
    `;
}

async function addPolicy() {
    const name = document.getElementById('pol-name').value.trim();
    if (!name) { notify('Politika adi zorunlu!', 'warning'); return; }
    const body = { name, access_days: parseInt(document.getElementById('pol-access-days').value) || null, modify_days: parseInt(document.getElementById('pol-modify-days').value) || null, min_size: parseInt(document.getElementById('pol-min-size').value) || null, max_size: parseInt(document.getElementById('pol-max-size').value) || null };
    const ext = document.getElementById('pol-extensions').value.trim();
    if (ext) body.extensions = ext.split(',').map(e => e.trim());
    await api('/policies', { method: 'POST', body: JSON.stringify(body) });
    closeModal('modal-policy');
    notify(`Politika olusturuldu: ${name}`, 'success');
    loadPolicies();
}

async function deletePolicy(id) {
    if (!confirm('Bu politikayi silmek istiyor musunuz?')) return;
    await api(`/policies/${id}`, { method: 'DELETE' });
    notify('Politika silindi', 'success');
    loadPolicies();
}

// ═══════════════════════════════════════════════════
// SCHEDULES
// ═══════════════════════════════════════════════════
async function loadSchedules() {
    const data = await api('/schedules', { silent: true }).catch(()=>[]);
    document.getElementById('schedule-table').innerHTML = `
        <thead><tr><th>ID</th><th>Tur</th><th>Kaynak</th><th>Cron</th><th>Durum</th><th>Son Calisma</th><th>Islem</th></tr></thead>
        <tbody>${data.length ? data.map(t => `<tr><td>${t.id}</td><td><span class="badge badge-info">${t.task_type}</span></td><td>${t.source_name||'-'}</td><td><code>${t.cron_expression}</code></td><td><span class="badge ${t.enabled?'badge-success':'badge-warning'}">${t.enabled?'Aktif':'Pasif'}</span></td><td>${t.last_run_at ? t.last_run_at.substring(0,19) : 'Hic'}</td><td><button class="btn btn-sm btn-danger" onclick="deleteSchedule(${t.id})">Sil</button></td></tr>`).join('') : '<tr><td colspan="7" style="text-align:center;padding:40px;color:var(--text-muted)">Zamanlanmis gorev bulunamadi</td></tr>'}</tbody>
    `;
}

async function addSchedule() {
    const type = document.getElementById('sch-type').value;
    const sourceId = document.getElementById('sch-source').value;
    const cron = document.getElementById('sch-cron').value.trim();
    if (!sourceId || !cron) { notify('Kaynak ve cron zorunlu!', 'warning'); return; }
    await api('/schedules', { method: 'POST', body: JSON.stringify({ task_type: type, source_id: parseInt(sourceId), cron_expression: cron }) });
    closeModal('modal-schedule');
    notify('Gorev olusturuldu', 'success');
    loadSchedules();
}

async function deleteSchedule(id) {
    if (!confirm('Bu gorevi silmek istiyor musunuz?')) return;
    await api(`/schedules/${id}`, { method: 'DELETE' });
    notify('Gorev silindi', 'success');
    loadSchedules();
}

// ═══════════════════════════════════════════════════
// DRILL-DOWN
// ═══════════════════════════════════════════════════
let ddCurrentUrl = '';
let ddCurrentPage = 1;
let ddCurrentArchiveParams = null;
let ddCurrentTitle = '';

function showDrilldown(title, url, archiveParams) {
    ddCurrentUrl = url;
    ddCurrentPage = 1;
    ddCurrentArchiveParams = archiveParams || null;
    ddCurrentTitle = title;
    document.getElementById('dd-title').textContent = title;
    document.getElementById('dd-archive-btn').style.display = archiveParams ? 'inline-flex' : 'none';
    document.getElementById('drilldown-overlay').classList.add('active');
    loadDrilldownPage(url, 1);
}

function closeDrilldown() {
    document.getElementById('drilldown-overlay').classList.remove('active');
}

async function loadDrilldownPage(url, page) {
    ddCurrentPage = page;
    const sep = url.includes('?') ? '&' : '?';
    const fullUrl = `${url}${sep}page=${page}&limit=100`;
    try {
        const data = await api(fullUrl);
        const files = data.files || [];
        const total = data.total || 0;
        const totalPages = Math.ceil(total / 100);

        document.getElementById('dd-table').innerHTML = `
            <thead><tr><th>Dosya Adi</th><th>Uzanti</th><th>Boyut</th><th>Son Erisim</th><th>Son Degisiklik</th><th>Sahip</th></tr></thead>
            <tbody>${files.length ? files.map(f => `<tr>
                <td title="${escapeHtml(f.file_path||'')}">${escapeHtml(f.file_name||'')}</td>
                <td>${escapeHtml(f.extension||'-')}</td>
                <td>${formatSize(f.file_size||0)}</td>
                <td>${escapeHtml((f.last_access_time||'').substring(0,16))}</td>
                <td>${escapeHtml((f.last_modify_time||'').substring(0,16))}</td>
                <td>${escapeHtml(f.owner||'-')}</td>
            </tr>`).join('') : '<tr><td colspan="6" style="text-align:center;padding:30px;color:var(--text-muted)">Dosya bulunamadi</td></tr>'}</tbody>
        `;

        // Pagination
        let pagHtml = `<span>Toplam: ${formatNum(total)} dosya</span>`;
        document.getElementById('dd-info').innerHTML = pagHtml;

        let navHtml = '';
        if (page > 1) navHtml += `<button class="dd-page-btn" onclick="loadDrilldownPage('${ddCurrentUrl}',${page-1})">Onceki</button>`;
        navHtml += `<span>Sayfa ${page} / ${totalPages || 1}</span>`;
        if (page < totalPages) navHtml += `<button class="dd-page-btn" onclick="loadDrilldownPage('${ddCurrentUrl}',${page+1})">Sonraki</button>`;
        document.getElementById('dd-pagination').innerHTML = navHtml;
    } catch(e) { notify(e.message, 'error'); }
}

async function archiveDrilldown() {
    if (!ddCurrentArchiveParams) return;
    if (!confirm('Bu gruptaki dosyalari arsivlemek istiyor musunuz?')) return;
    try {
        const result = await api('/drilldown/archive', {
            method: 'POST',
            body: JSON.stringify(ddCurrentArchiveParams)
        });
        notify(`${result.archived || 0} dosya arsivlendi (${result.total_size_formatted || ''})`, 'success');
        closeDrilldown();
    } catch(e) { notify(e.message, 'error'); }
}

async function downloadDrilldownXLS(event) {
    // Use the URL to determine source_id
    const parts = ddCurrentUrl.split('/');
    const sid = parts[parts.length - 1].split('?')[0];
    if (!sid) { notify('Kaynak bulunamadi', 'warning'); return; }
    const btn = event?.currentTarget;
    await withButtonLoading(btn, async (signal) => {
        await fetchAndDownload(`/api/export/xls/${sid}`, signal, `drilldown_${sid}.xlsx`);
    });
}

function drilldownOwner(owner, sourceId) {
    const url = `/drilldown/owner/${sourceId}?owner=${encodeURIComponent(owner)}`;
    showDrilldown(`Sahip: ${owner}`, url, {
        source_id: parseInt(sourceId),
        filter_type: 'owner',
        owner: owner
    });
}

// ═══════════════════════════════════════════════════
// EXPORT HELPERS (Arka plan XLS sistemi)
// ═══════════════════════════════════════════════════
async function exportXLS(sourceId, event) {
    if (!sourceId) { notify('Onceeerce kaynak secin', 'warning'); return; }
    const btn = event?.currentTarget;
    await withButtonLoading(btn, async (signal) => {
        await fetchAndDownload(`/api/export/xls/${sourceId}`, signal, `report_${sourceId}.xlsx`);
    });
}
async function exportPDF(sourceId, event) {
    if (!sourceId) { notify('Onceeerce kaynak secin', 'warning'); return; }
    const btn = event?.currentTarget;
    await withButtonLoading(btn, async (signal) => {
        await fetchAndDownload(`/api/export/pdf/${sourceId}`, signal, `report_${sourceId}.pdf`);
    });
}

async function startBackgroundExport(reportType, sourceId) {
    if (!sourceId) { notify('Kaynak secin', 'warning'); return; }
    try {
        const r = await api(`/export/start?report_type=${reportType}&source_id=${sourceId}`, {method:'POST'});
        notify('Excel raporu arka planda hazirlaniyor...', 'info');
        pollExportStatus(r.job_id);
    } catch(e) {
        // Fallback: eski yontem
        notify('Arka plan export basarilamadi, dogrudan indiriliyor...', 'warning');
        downloadFile(`/api/export/xls/${sourceId}`);
    }
}

function pollExportStatus(jobId) {
    const check = async () => {
        try {
            const r = await api(`/export/status/${jobId}`, {silent:true});
            if (r.status === 'completed') {
                const sizeStr = r.file_size ? ` (${formatSize(r.file_size)})` : '';
                notify(`Excel hazir: ${r.file_name}${sizeStr} - Indiriliyor...`, 'success');
                downloadFile(`/api/export/download/${jobId}`);
                return;
            } else if (r.status === 'error') {
                notify('Export hatasi: ' + (r.error || 'Bilinmeyen hata'), 'error');
                return;
            } else {
                // Hala calisiyor
                if (r.progress > 0) {
                    notify(`Export hazirlaniyor: %${r.progress}...`, 'info');
                }
                setTimeout(check, 2000);
            }
        } catch(e) { notify('Export durum sorgu hatasi', 'error'); }
    };
    setTimeout(check, 1500);
}

// ═══════════════════════════════════════════════════
// WATCHER CONTROLS
// ═══════════════════════════════════════════════════
async function startWatcher(sourceId) {
    try {
        await api(`/watcher/${sourceId}/start`, { method: 'POST' });
        notify('Dosya izleme baslatildi', 'success');
        updateWatcherStatus(sourceId);
    } catch(e) { notify(e.message, 'error'); }
}

async function stopWatcher(sourceId) {
    try {
        await api(`/watcher/${sourceId}/stop`, { method: 'POST' });
        notify('Dosya izleme durduruldu', 'info');
        updateWatcherStatus(sourceId);
    } catch(e) { notify(e.message, 'error'); }
}

async function updateWatcherStatus(sourceId) {
    try {
        const data = await api(`/watcher/status?source_id=${sourceId}`, { silent: true });
        const el = document.getElementById(`watcher-status-${sourceId}`);
        if (el) {
            const running = data.running || false;
            const changes = data.total_changes || 0;
            el.innerHTML = `<span class="watcher-dot ${running?'active':'inactive'}"></span>
                <span style="font-size:11px;color:var(--text-muted)">${running?'Izleniyor':'Durduruldu'} | ${changes} degisiklik</span>`;
        }
    } catch(e) {}
}

// ═══════════════════════════════════════════════════
// AI INSIGHTS
// ═══════════════════════════════════════════════════
async function loadInsights() {
    const sid = document.getElementById('insights-source')?.value;
    if (!sid) return;
    try {
        const data = await api(`/insights/${sid}`);
        const score = data.score || 0;
        const insights = data.insights || [];

        // Update gauge
        const circumference = 326.7;
        const offset = circumference - (circumference * score / 100);
        const gauge = document.getElementById('insights-gauge');
        gauge.style.strokeDashoffset = offset;
        gauge.style.stroke = score >= 70 ? 'var(--success)' : score >= 40 ? 'var(--warning)' : 'var(--danger)';
        document.getElementById('insights-score-text').textContent = score;

        const desc = score >= 80 ? 'Cok iyi durumda' : score >= 60 ? 'Iyi, iyilestirme mevcut' : score >= 40 ? 'Orta - aksiyon gerekli' : 'Kritik - acil mudahale';
        document.getElementById('insights-score-desc').textContent = desc;

        // Reclaimable space
        const reclaim = insights.find(i => i.category === 'recommendation' && i.impact_size);
        const reclaimEl = document.getElementById('insights-reclaimable');
        if (reclaim) {
            reclaimEl.style.display = 'block';
            document.getElementById('insights-reclaim-value').textContent = formatSize(reclaim.impact_size);
        } else {
            reclaimEl.style.display = 'none';
        }

        // Render insight cards
        const sevColors = { critical: 'var(--danger)', warning: 'var(--warning)', info: 'var(--info)', success: 'var(--success)' };
        const sevIcons = { critical: '🔴', warning: '🟡', info: '🔵', success: '🟢' };
        const catIcons = { storage: '💾', stale: '📅', duplicates: '📋', security: '🔒', growth: '📈', recommendation: '✨', audit: '📝' };

        document.getElementById('insights-list').innerHTML = insights.length ? insights.map(i => `
            <div style="background:var(--bg-card);border:1px solid var(--border);border-left:4px solid ${sevColors[i.severity] || 'var(--border-light)'};border-radius:var(--radius);padding:16px 20px;display:flex;align-items:flex-start;gap:14px">
                <div style="font-size:22px;flex-shrink:0">${catIcons[i.category] || sevIcons[i.severity] || '📌'}</div>
                <div style="flex:1;min-width:0">
                    <div style="font-size:14px;font-weight:700;color:var(--text-primary)">${escapeHtml(i.title)}</div>
                    <div style="font-size:12px;color:var(--text-secondary);margin-top:4px">${escapeHtml(i.description)}</div>
                    ${i.action ? `<div style="font-size:11px;color:var(--accent-light);margin-top:6px;font-weight:600">→ ${escapeHtml(i.action)}</div>` : ''}
                </div>
                <div style="text-align:right;flex-shrink:0">
                    <span style="display:inline-block;padding:2px 10px;border-radius:12px;font-size:10px;font-weight:700;background:${sevColors[i.severity] || 'var(--border)'}22;color:${sevColors[i.severity] || 'var(--text-muted)'}">${escapeHtml((i.severity||'info').toUpperCase())}</span>
                    ${i.impact_size ? `<div style="font-size:12px;color:var(--text-muted);margin-top:6px">${formatSize(i.impact_size)}</div>` : ''}
                    ${i.file_count ? `<div style="font-size:11px;color:var(--text-muted)">${formatNum(i.file_count)} dosya</div>` : ''}
                    <div style="display:flex;gap:6px;margin-top:8px;justify-content:flex-end">
                        <button class="btn btn-sm btn-accent" onclick="showInsightFilesByCategory('${escapeHtml(i.category||'')}', '${escapeHtml(i.insight_type||'')}', ${sid})">Incele</button>
                        ${i.action ? `<button class="btn btn-sm btn-outline" onclick="applyInsight('${escapeHtml(i.category)}', ${sid})">Uygula</button>` : ''}
                    </div>
                </div>
            </div>
        `).join('') : '<div style="text-align:center;padding:60px;color:var(--text-muted)">Insight bulunamadi. Once bir tarama yapin.</div>';
    } catch(e) { notify(e.message, 'error'); }
}

// ═══════════════════════════════════════════════════
// ADLANDIRMA UYUMU (MIT NAMING)
// ═══════════════════════════════════════════════════

async function loadNaming() {
    const sourceId = document.getElementById('naming-source')?.value;
    if (!sourceId) {
        _setHtmlSafe('naming-summary-cards', '<div style="padding:20px;color:var(--text-muted)">Lutfen bir kaynak secin</div>');
        // Attach the toggle even without data so the user can switch modes.
        _attachNamingViewToggle();
        return;
    }

    // Issue #181 Track B2: try partial summary first while scan is running.
    const ps = await _fetchPartialSummaryV2(sourceId);
    const wasPartial = !!_psv2PrevWasPartial['naming'];
    if (ps && ps.scan_state && ps.scan_state !== 'completed') {
        _psv2PrevWasPartial['naming'] = true;
        _psv2ShowBanner('naming-partial-banner', ps.scan_state, ps.progress);
        const namingCount = (ps.summary && ps.summary.anomalies_so_far && ps.summary.anomalies_so_far.naming) || 0;
        _setHtmlSafe('naming-summary-cards', `
            <div class="card danger"><div class="card-label">Adlandirma Ihlalleri</div><div class="card-value">${formatNum(namingCount)}</div><div class="card-sub">Kismi veri</div></div>
            <div class="card warning"><div class="card-label">Detay</div><div class="card-value" style="font-size:14px">Tarama devam ediyor</div><div class="card-sub">Tarama bitince tam rapor goruntulenecek</div></div>
        `);
        const reqTable = document.getElementById('naming-req-table');
        if (reqTable) reqTable.innerHTML = '<div style="padding:20px;color:var(--text-muted)">Tarama tamamlandiktan sonra kural detaylari goruntulenecek</div>';
        const bpTable = document.getElementById('naming-bp-table');
        if (bpTable) bpTable.innerHTML = '<div style="padding:20px;color:var(--text-muted)">Tarama tamamlandiktan sonra en iyi uygulama detaylari goruntulenecek</div>';
        _attachNamingViewToggle();
        _psv2StartPoll(sourceId, 'naming', loadNaming);
        return;
    }

    if (wasPartial && ps && ps.scan_state === 'completed') {
        notify('Tarama tamamlandi, Adlandirma Uyumu guncellendi', 'success');
    }
    _psv2PrevWasPartial['naming'] = false;
    _psv2HideBanner('naming-partial-banner');
    _psv2StopPoll();

    try {
        const data = await api(`/reports/mit-naming/${sourceId}`);

        // Ozet kartlar
        const total = data.total_files_analyzed || 0;
        const reqOk = data.req_compliant_count || 0;
        const fullOk = data.fully_compliant_count || 0;
        const reqViolations = data.summary?.total_requirement_violations || 0;
        const bpViolations = data.summary?.total_bp_violations || 0;

        _setHtmlSafe('naming-summary-cards', `
            <div class="card accent"><div class="card-label">Toplam Dosya</div><div class="card-value">${formatNum(total)}</div></div>
            <div class="card success"><div class="card-label">Zorunlu Uyumlu</div><div class="card-value">${formatNum(reqOk)}</div><div class="card-sub">${data.requirement_compliance || 0}%</div></div>
            <div class="card purple"><div class="card-label">Tam Uyumlu</div><div class="card-value">${formatNum(fullOk)}</div><div class="card-sub">${data.full_compliance || 0}%</div></div>
            <div class="card danger"><div class="card-label">Zorunlu Ihlal</div><div class="card-value">${formatNum(reqViolations)}</div></div>
            <div class="card warning"><div class="card-label">BP Sapma</div><div class="card-value">${formatNum(bpViolations)}</div></div>
        `);

        // Skor gauge
        const score = data.compliance_score || 0;
        const circumference = 213.6;
        const offset = circumference - (circumference * score / 100);
        const gauge = document.getElementById('naming-gauge');
        gauge.style.strokeDashoffset = offset;
        gauge.style.stroke = score >= 70 ? 'var(--success)' : score >= 40 ? 'var(--warning)' : 'var(--danger)';
        document.getElementById('naming-score-text').textContent = Math.round(score);
        document.getElementById('naming-req-pct').textContent = (data.requirement_compliance || 0) + '%';
        document.getElementById('naming-full-pct').textContent = (data.full_compliance || 0) + '%';

        // Zorunlu kurallar tablosu
        const reqs = data.requirements || [];
        const reqTable = document.getElementById('naming-req-table');
        if (reqs.length) {
            reqTable.innerHTML = `
                <thead><tr><th>Kod</th><th>Kural</th><th>Ihlal Sayisi</th><th>Oran</th><th>Ciddiyet</th><th>Ornekler</th><th>Islem</th></tr></thead>
                <tbody>${reqs.map(r => `<tr>
                    <td><strong>${escapeHtml(r.code)}</strong></td>
                    <td>${escapeHtml(r.label)}</td>
                    <td>${formatNum(r.count)}</td>
                    <td>
                        <div style="display:flex;align-items:center;gap:8px">
                            <div style="flex:1;height:6px;background:var(--bg-secondary);border-radius:3px;overflow:hidden;min-width:60px">
                                <div style="width:${Math.min(r.percentage, 100)}%;height:100%;background:${r.severity==='critical'?'var(--danger)':'var(--warning)'};border-radius:3px"></div>
                            </div>
                            <span style="font-size:11px;min-width:40px">${r.percentage}%</span>
                        </div>
                    </td>
                    <td><span class="badge ${r.severity==='critical'?'badge-danger':r.severity==='warning'?'badge-warning':'badge-info'}">${escapeHtml(r.severity||'')}</span></td>
                    <td style="font-size:11px;color:var(--text-muted);max-width:250px;word-break:break-word">${(r.samples||[]).slice(0,3).map(escapeHtml).join('<br>')}</td>
                    <td><button class="btn btn-sm btn-outline" onclick="showNamingFiles('${escapeHtml(r.code)}', ${sourceId})">Dosyalari Gor</button></td>
                </tr>`).join('')}</tbody>
            `;
        } else {
            reqTable.innerHTML = '<div style="text-align:center;padding:20px;color:var(--success);font-size:13px">Tum dosyalar zorunlu kurallara uyumlu!</div>';
        }

        // Best practices tablosu
        const bps = data.best_practices || [];
        const bpTable = document.getElementById('naming-bp-table');
        if (bps.length) {
            bpTable.innerHTML = `
                <thead><tr><th>Kod</th><th>Uygulama</th><th>Sapma Sayisi</th><th>Oran</th><th>Ciddiyet</th><th>Ornekler</th><th>Islem</th></tr></thead>
                <tbody>${bps.map(b => `<tr>
                    <td><strong>${escapeHtml(b.code)}</strong></td>
                    <td>${escapeHtml(b.label)}</td>
                    <td>${formatNum(b.count)}</td>
                    <td>
                        <div style="display:flex;align-items:center;gap:8px">
                            <div style="flex:1;height:6px;background:var(--bg-secondary);border-radius:3px;overflow:hidden;min-width:60px">
                                <div style="width:${Math.min(b.percentage, 100)}%;height:100%;background:${b.severity==='warning'?'var(--warning)':'var(--info)'};border-radius:3px"></div>
                            </div>
                            <span style="font-size:11px;min-width:40px">${b.percentage}%</span>
                        </div>
                    </td>
                    <td><span class="badge ${b.severity==='warning'?'badge-warning':'badge-info'}">${escapeHtml(b.severity||'')}</span></td>
                    <td style="font-size:11px;color:var(--text-muted);max-width:250px;word-break:break-word">${(b.samples||[]).slice(0,3).map(escapeHtml).join('<br>')}</td>
                    <td><button class="btn btn-sm btn-outline" onclick="showNamingFiles('${escapeHtml(b.code)}', ${sourceId})">Dosyalari Gor</button></td>
                </tr>`).join('')}</tbody>
            `;
        } else {
            bpTable.innerHTML = '<div style="text-align:center;padding:20px;color:var(--success);font-size:13px">Tum dosyalar en iyi uygulamalara uyumlu!</div>';
        }
        // Export butonunu goster
        document.getElementById('naming-export-btn').style.display = '';

        // Issue #80: reusable entity-list ile ihlal eden dosyalari listele
        renderMitNamingEntityList(sourceId, data);
        _attachNamingViewToggle();
    } catch(e) { console.error('MIT naming load error:', e); }
}

// Issue #84 — Gorsel ↔ Profesyonel mod toggle for Adlandirma Uyumu.
function _renderNamingVisual() {
    const v = document.getElementById('naming-visual-host');
    const g = document.getElementById('naming-grid-host');
    if (v) v.style.display = '';
    if (g) g.style.display = 'none';
}

function _renderNamingGrid() {
    const v = document.getElementById('naming-visual-host');
    const g = document.getElementById('naming-grid-host');
    if (v) v.style.display = 'none';
    if (g) g.style.display = '';
    // entity-list itself is rendered by renderMitNamingEntityList() during
    // loadNaming(); nothing else to do here. If the container is empty (e.g.
    // user toggled before selecting a source), show a hint.
    const host = document.getElementById('mit-naming-container');
    if (host && !host.innerHTML.trim()) {
        host.innerHTML = '<div style="padding:20px;color:var(--text-muted)">Once bir kaynak secin</div>';
    }
}

function _attachNamingViewToggle() {
    const page = document.getElementById('page-naming');
    if (!page || typeof attachViewToggle !== 'function') return;
    attachViewToggle(page, {
        pageKey: 'naming',
        renderVisual: _renderNamingVisual,
        renderGrid: _renderNamingGrid,
        defaultMode: 'visual',
    });
}

// Issue #80 — entity-list component'ini mit-naming sayfasina baglar.
async function renderMitNamingEntityList(sourceId, report) {
    const container = document.getElementById('mit-naming-container');
    if (!container || typeof renderEntityList !== 'function') return;

    // Loading shimmer
    container.innerHTML =
        '<div style="padding:30px;text-align:center;color:var(--text-muted);font-size:12px">' +
        'Ihlal eden dosyalar yukleniyor...</div>';

    // Hangi kurallar icin dosya cekilecek? Rapordaki kurallar (count > 0).
    const codesToFetch = []
        .concat((report.requirements || []).map(r => r.code))
        .concat((report.best_practices || []).map(b => b.code));

    if (codesToFetch.length === 0) {
        container.innerHTML =
            '<div style="padding:30px;text-align:center;color:var(--success);font-size:13px">' +
            'Hicbir ihlal yok — listelenecek dosya bulunamadi.</div>';
        return;
    }

    // Her kural icin ilk 50 dosyayi cek; ayni dosyayi birden fazla kural
    // birlestirecekse "rules" alaninda biriktir. UI tarafinda her satir =
    // bir dosya (cogu rule violating); XLSX export'unda her (dosya × rule).
    try {
        const results = await Promise.all(codesToFetch.map(code =>
            api(`/reports/mit-naming/${sourceId}/files?code=${code}&page=1&page_size=50`,
                {silent: true}).catch(() => ({files: []}))
        ));

        const byId = new Map();
        codesToFetch.forEach((code, i) => {
            (results[i].files || []).forEach(f => {
                const key = f.id != null ? f.id : f.file_path;
                if (!byId.has(key)) {
                    byId.set(key, {
                        id: f.id,
                        file_path: f.file_path,
                        file_name: f.file_name,
                        directory: f.directory || '',
                        owner: f.owner || '',
                        last_modify_time: f.last_modify_time || '',
                        file_size: f.file_size || 0,
                        file_size_formatted: f.file_size_formatted || formatSize(f.file_size || 0),
                        rules: [],
                    });
                }
                byId.get(key).rules.push(code);
            });
        });

        const rows = Array.from(byId.values())
            .sort((a, b) => b.rules.length - a.rules.length)
            .slice(0, 1000); // UI cap; XLSX export covers everything server-side

        // Determine highest severity per row.
        const severityOf = code => {
            if (code && code[0] === 'R') return 'danger';
            if (code === 'B1' || code === 'B2' || code === 'B3') return 'warning';
            return 'info';
        };

        rows.forEach(r => {
            r.severity = r.rules.some(c => c[0] === 'R') ? 'danger'
                : r.rules.some(c => ['B1','B2','B3'].includes(c)) ? 'warning' : 'info';
            r.rule_summary = r.rules.join(', ');
        });

        renderEntityList(container, {
            rows: rows,
            rowKey: 'id',
            pageSize: 50,
            searchKeys: ['file_path', 'file_name', 'owner', 'rule_summary'],
            columns: [
                {key: 'file_name', label: 'Dosya Adi'},
                {key: 'file_path', label: 'Tam Yol'},
                {key: 'rule_summary', label: 'Ihlal Kodlari'},
                {key: 'severity', label: 'Onem',
                 render: v => `<span class="badge badge-${v}">${v}</span>`},
                {key: 'file_size', label: 'Boyut',
                 render: (v, row) => row.file_size_formatted || formatSize(v || 0)},
                {key: 'owner', label: 'Sahip'},
            ],
            toolbar: {
                xlsxExport: {
                    endpoint: `/api/reports/mit-naming/${sourceId}/export.xlsx`,
                    filenameBase: `naming-${sourceId}`,
                },
                // Issue #80: endpoint-driven bulk actions with dry-run preview
                // + audit banner. The component renders a confirmation modal
                // for confirmRequired actions; max-cap is enforced client-side.
                bulkActions: [
                    {id: 'open-target', action: 'open-folder',
                     label: 'Hedefe Git', icon: 'open',
                     confirmRequired: false, max: 5},
                    {id: 'archive-bulk', action: 'bulk-archive',
                     label: 'Toplu Arsivle',
                     confirmRequired: true, dryRun: true, danger: true,
                     endpoint: '/api/archive/bulk-from-list'},
                    {id: 'legal-hold-add', action: 'add-hold',
                     label: 'Legal Hold Ekle',
                     confirmRequired: true,
                     endpoint: '/api/compliance/legal-holds'},
                ],
            },
            onBulkAction: async (actionId, selectedRows) => {
                // Reached only for actions without an `endpoint` set.
                if (actionId === 'open-folder' || actionId === 'open-target') {
                    for (const r of selectedRows) {
                        const target = r.directory || (r.file_path || '').replace(/[\\/][^\\/]*$/, '');
                        if (target) openFolder(target);
                    }
                    return;
                }
                notify('Bilinmeyen toplu islem: ' + actionId, 'warning');
            },
            emptyMessage: 'Ihlal eden dosya bulunamadi',
        });
    } catch (e) {
        container.innerHTML =
            '<div style="padding:20px;color:var(--danger);font-size:12px">' +
            'Liste yuklenemedi: ' + (e && e.message ? e.message : e) + '</div>';
    }
}

async function showNamingFiles(code, sourceId) {
    try {
        const data = await api(`/reports/mit-naming/${sourceId}/files?code=${code}&page=1&page_size=200`);
        const files = data.files || [];
        const filesHtml = files.length ? files.map((f, i) => `
            <tr>
                <td>${i+1}</td>
                <td style="max-width:200px;word-break:break-all;font-size:11px">${escapeHtml(f.file_name)}</td>
                <td style="max-width:300px;word-break:break-all;font-size:11px" title="${escapeHtml(f.file_path)}">${escapeHtml(f.file_path)}</td>
                <td>${escapeHtml(f.file_size_formatted || formatSize(f.file_size))}</td>
                <td>${escapeHtml(f.owner || '-')}</td>
                <td style="white-space:nowrap"><button class="btn btn-sm btn-outline" onclick="openFolder('${escapeHtml((f.directory||'').replace(/\\/g,'\\\\\\\\'))}')">Konuma Git</button></td>
            </tr>
        `).join('') : '<tr><td colspan="6" style="text-align:center;padding:20px;color:var(--text-muted)">Dosya bulunamadi</td></tr>';

        const modalContent = `
            <div style="padding:20px">
                <h3 style="margin:0 0 16px 0">${escapeHtml(code)} Ihlali - ${data.total} Dosya</h3>
                <div style="max-height:500px;overflow-y:auto;border:1px solid var(--border);border-radius:var(--radius)">
                    <table style="width:100%;font-size:12px">
                        <thead style="position:sticky;top:0;background:var(--bg-secondary)"><tr><th>#</th><th>Dosya Adi</th><th>Tam Yol</th><th>Boyut</th><th>Sahip</th><th>Islem</th></tr></thead>
                        <tbody>${filesHtml}</tbody>
                    </table>
                </div>
                ${data.total > 200 ? `<div style="text-align:center;padding:8px;color:var(--text-muted);font-size:11px">Ilk 200 dosya gosteriliyor (toplam ${data.total}). Tum liste icin Excel Export kullanin.</div>` : ''}
                <div style="display:flex;gap:12px;justify-content:flex-end;margin-top:16px">
                    <button class="btn btn-outline" onclick="closeModal('modal-naming-files')">Kapat</button>
                    <button class="btn btn-primary" onclick="exportNamingReport(event)">Tum Ihlalleri Excel Export</button>
                </div>
            </div>
        `;

        let modal = document.getElementById('modal-naming-files');
        if (!modal) {
            modal = document.createElement('div');
            modal.id = 'modal-naming-files';
            modal.className = 'modal-overlay';
            modal.onclick = (e) => { if(e.target===modal) modal.classList.remove('active'); };
            modal.innerHTML = '<div class="modal" style="max-width:1100px;width:95%"></div>';
            document.body.appendChild(modal);
        }
        modal.querySelector('.modal').innerHTML = modalContent;
        modal.classList.add('active');
    } catch(e) { notify('Dosya listesi yuklenemedi: ' + e.message, 'error'); }
}

async function exportNamingReport(event) {
    const sourceId = document.getElementById('naming-source')?.value;
    if (!sourceId) { notify('Lutfen kaynak secin', 'warning'); return; }
    // Issue #80: streaming XLSX endpoint -- direct download via fetch
    // wrapped in #82 Bug 3 withButtonLoading helper (spinner + abort timeout).
    const btn = event?.currentTarget;
    await withButtonLoading(btn, async (signal) => {
        await fetchAndDownload(
            `${API}/reports/mit-naming/${sourceId}/export.xlsx`,
            signal,
            `naming-${sourceId}.xlsx`
        );
    });
}

function openFolder(path) {
    // Issue #82 (Bug 1): Dashboard genellikle bir dosya sunucusunda calisir;
    // kullanici kendi PC'sinden tarayici ile acar. Backend artik istemcinin
    // uzak olup olmadigini tespit edip "remote_client" modunu dondurur.
    // Frontend her iki durumda da once yolu panoya kopyalar (best-effort),
    // boylece kullanici kendi Explorer'ina yapistirabilsin.

    // Panoya kopyalama yardimci fonksiyonu — API yoksa veya izin reddedilirse
    // kullaniciya prompt ile secilebilir metin gosterir.
    function copyPathFallback(p) {
        try {
            if (navigator.clipboard && navigator.clipboard.writeText) {
                navigator.clipboard.writeText(p).catch(() => {
                    window.prompt('Yolu kopyalamak icin Ctrl+C yapin:', p);
                });
                return;
            }
        } catch (_) { /* noop, prompt fallback below */ }
        // Clipboard API yok (http baglam vb.) — manuel kopya icin prompt goster.
        window.prompt('Yolu kopyalamak icin Ctrl+C yapin:', p);
    }

    // 1) Her zaman once panoya kopyalamayi dene (await etmeden).
    copyPathFallback(path);

    // 2) Endpoint'i cagir. api() helper 4xx/5xx'te throw eder — burada manuel
    // fetch kullaniyoruz ki "remote_client" (200 + success:false) durumunu
    // hatasiz isleyebilelim.
    fetch(API + '/system/open-folder', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ path: path })
    }).then(async (r) => {
        if (!r.ok) {
            let detail = '';
            try { const e = await r.json(); detail = e.detail || ''; } catch (_) {}
            notify('Dizin acilamadi (' + r.status + (detail ? ': ' + detail : '') + '). Yol panoya kopyalandi: ' + path, 'error');
            return;
        }
        const data = await r.json().catch(() => ({}));
        if (data && data.success === true && data.mode === 'native') {
            notify('Explorer acildi: ' + path, 'success');
        } else if (data && data.success === false && data.mode === 'remote_client') {
            // Uzak istemci: daha gorunur ve uzun sureli bildirim.
            const el = document.createElement('div');
            el.className = 'notification warning';
            el.innerHTML = `<span>⚠️</span> Yol panoya kopyalandi. Kendi Explorer'inda yapistirin: ${escapeHtml(path)}`;
            el.onclick = () => el.remove();
            document.getElementById('notifications').appendChild(el);
            setTimeout(() => el.remove(), 12000);
        } else {
            // Beklenmeyen sekil — yine de yol kullaniciya gosterilsin.
            notify('Yol panoya kopyalandi: ' + path, 'info');
        }
    }).catch(() => {
        // Ag hatasi / fetch basarisiz — pano fallback'i zaten tetiklenmisti.
        notify('Sunucuya ulasilamadi. Yol panoya kopyalandi: ' + path, 'error');
    });
}

// ═══════════════════════════════════════════════════
// AI INSIGHTS DOSYA LISTELEME
// ═══════════════════════════════════════════════════

// Insight category -> backend insight_type mapping. Centralized so callers
// and showInsightFilesByCategory can warn on unknown categories instead of
// silently falling back to 'all_files' (issue #82, Bug 2).
const INSIGHT_CATEGORY_TO_TYPE = {
    stale: 'stale_1year',
    storage: 'temp_files',
    duplicates: 'duplicates',
    security: 'temp_files',
    growth: 'stale_180',
    recommendation: 'large_files',
    audit: 'stale_1year'
};

// Resolve a (category, explicitInsightType) pair to a backend type string.
// If we have to fall back to 'all_files' because the category isn't in the
// map, warn loudly so unknown categories don't go unnoticed.
function resolveInsightType(category, explicitInsightType) {
    if (explicitInsightType) return explicitInsightType;
    if (category && Object.prototype.hasOwnProperty.call(INSIGHT_CATEGORY_TO_TYPE, category)) {
        return INSIGHT_CATEGORY_TO_TYPE[category];
    }
    // Mapping fell back: warn so the broken category surfaces to devs/users.
    console.warn('Unknown insight category: %s -> falling back to all_files', category);
    notify('Bilinmeyen insight kategorisi: ' + (category || '(bos)') + ' — tum dosyalar gosteriliyor', 'warning');
    return 'all_files';
}

// Wrapper used from inline onclick handlers: takes the raw category +
// optional explicit insight_type, resolves with warning on fallback, then
// opens the modal. Kept separate from showInsightFiles so the latter can
// still be called directly with a known-good type.
function showInsightFilesByCategory(category, explicitInsightType, sourceId) {
    showInsightFiles(resolveInsightType(category, explicitInsightType), sourceId);
}

async function showInsightFiles(insightType, sourceId) {
    let data;
    try {
        data = await api(
            `/insights/${sourceId}/files?insight_type=${encodeURIComponent(insightType)}&page=1&page_size=200`,
            { silent: true }
        );
    } catch (e) {
        // 400 from backend (unknown insight_type) carries a useful detail
        // message — show it inside the modal so the user understands why
        // the list is empty (issue #82, Bug 2).
        notify('Dosya listesi yuklenemedi: ' + e.message, 'error');
        showInsightFilesError(insightType, e.message);
        return;
    }

    const files = data.files || [];
    const filesHtml = files.length ? files.map((f, i) => `
        <tr>
            <td>${i+1}</td>
            <td style="max-width:200px;word-break:break-all;font-size:11px">${escapeHtml(f.file_name)}</td>
            <td style="max-width:250px;word-break:break-all;font-size:11px" title="${escapeHtml(f.file_path)}">${escapeHtml(f.file_path)}</td>
            <td>${escapeHtml(f.file_size_formatted || formatSize(f.file_size))}</td>
            <td>${escapeHtml(f.owner || '-')}</td>
            <td>${escapeHtml((f.last_access_time||'').substring(0,10))}</td>
            <td style="white-space:nowrap"><button class="btn btn-sm btn-outline" onclick="openFolder('${escapeHtml((f.directory||'').replace(/\\/g,'\\\\\\\\'))}')">Konuma Git</button></td>
        </tr>
    `).join('') : '<tr><td colspan="7" style="text-align:center;padding:20px;color:var(--text-muted)">Dosya bulunamadi</td></tr>';

    const typeLabels = {
        stale_1year: '1 Yildan Eski Dosyalar',
        stale_3year: '3+ Yillik Eski Dosyalar',
        large_files: 'Buyuk Dosyalar (>100MB)',
        temp_files: 'Gecici/Yedek Dosyalar',
        duplicates: 'Kopya Dosyalar'
    };

    const modalContent = `
        <div style="padding:20px">
            <h3 style="margin:0 0 16px 0">${escapeHtml(typeLabels[insightType] || insightType)} - ${data.total} Dosya</h3>
            <div style="max-height:500px;overflow-y:auto;border:1px solid var(--border);border-radius:var(--radius)">
                <table style="width:100%;font-size:12px">
                    <thead style="position:sticky;top:0;background:var(--bg-secondary)"><tr><th>#</th><th>Dosya Adi</th><th>Tam Yol</th><th>Boyut</th><th>Sahip</th><th>Son Erisim</th><th>Islem</th></tr></thead>
                    <tbody>${filesHtml}</tbody>
                </table>
            </div>
            ${data.total > 200 ? `<div style="text-align:center;padding:8px;color:var(--text-muted);font-size:11px">Ilk 200 dosya (toplam ${data.total})</div>` : ''}
            <div style="display:flex;gap:12px;justify-content:flex-end;margin-top:16px">
                <button class="btn btn-outline" onclick="closeModal('modal-insight-files')">Kapat</button>
            </div>
        </div>
    `;

    _renderInsightFilesModal(modalContent);
}

// Render an error state inside the same modal we'd use for results, so the
// user sees *why* the list is empty rather than a phantom "broken button".
function showInsightFilesError(insightType, detail) {
    const safeDetail = String(detail || 'Bilinmeyen hata')
        .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    const safeType = String(insightType || '')
        .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    const modalContent = `
        <div style="padding:20px">
            <h3 style="margin:0 0 12px 0;color:var(--danger,#dc2626)">Dosya listesi alinamadi</h3>
            <div style="font-size:12px;color:var(--text-secondary);margin-bottom:8px">
                Insight tipi: <code>${safeType}</code>
            </div>
            <div style="font-size:13px;color:var(--text-primary);background:var(--bg-secondary);padding:12px;border-radius:var(--radius);border:1px solid var(--border);white-space:pre-wrap;word-break:break-word">
                ${safeDetail}
            </div>
            <div style="display:flex;gap:12px;justify-content:flex-end;margin-top:16px">
                <button class="btn btn-outline" onclick="closeModal('modal-insight-files')">Kapat</button>
            </div>
        </div>
    `;
    _renderInsightFilesModal(modalContent);
}

function _renderInsightFilesModal(html) {
    let modal = document.getElementById('modal-insight-files');
    if (!modal) {
        modal = document.createElement('div');
        modal.id = 'modal-insight-files';
        modal.className = 'modal-overlay';
        modal.onclick = (e) => { if(e.target===modal) modal.classList.remove('active'); };
        modal.innerHTML = '<div class="modal" style="max-width:1100px;width:95%"></div>';
        document.body.appendChild(modal);
    }
    modal.querySelector('.modal').innerHTML = html;
    modal.classList.add('active');
}

// ═══════════════════════════════════════════════════
// ARSIV GECMISI
// ═══════════════════════════════════════════════════
let ahCurrentPage = 1;

async function loadArchiveHistory(page) {
    ahCurrentPage = page || 1;
    const sourceId = document.getElementById('ah-source')?.value || '';
    const dateFrom = document.getElementById('ah-date-from')?.value || '';
    const dateTo = document.getElementById('ah-date-to')?.value || '';
    const opType = document.getElementById('ah-type')?.value || '';

    let url = `/archive/history?page=${ahCurrentPage}&page_size=20`;
    if (sourceId) url += `&source_id=${sourceId}`;
    if (dateFrom) url += `&date_from=${dateFrom}`;
    if (dateTo) url += `&date_to=${dateTo}`;
    if (opType) url += `&op_type=${opType}`;

    try {
        const data = await api(url);
        const ops = data.operations || [];

        // Cache for grid mode (issue #84).
        window.__ahLastReport = { url: url, data: data };

        // Ozet kartlar
        const totalOps = data.total || 0;
        const archiveOps = ops.filter(o => o.operation_type === 'archive').length;
        const restoreOps = ops.filter(o => o.operation_type === 'restore').length;
        document.getElementById('ah-summary-cards').innerHTML = `
            <div class="card accent"><div class="card-label">Toplam Islem</div><div class="card-value">${formatNum(totalOps)}</div></div>
            <div class="card warning"><div class="card-label">Bu Sayfada</div><div class="card-value">${ops.length}</div></div>
            <div class="card purple"><div class="card-label">Sayfa</div><div class="card-value">${data.page}/${data.total_pages}</div></div>
        `;

        // Tablo
        const statusBadge = (s) => {
            const colors = { completed: 'badge-success', running: 'badge-info', failed: 'badge-danger', partial: 'badge-warning' };
            return `<span class="badge ${colors[s]||'badge-info'}">${s}</span>`;
        };
        const typeBadge = (t) => t === 'archive' ? '<span class="badge badge-accent">Arsiv</span>' : '<span class="badge badge-success">Geri Yukleme</span>';

        const table = document.getElementById('ah-table');
        if (ops.length) {
            table.innerHTML = `
                <thead><tr><th>ID</th><th>Tarih</th><th>Tur</th><th>Tetikleyen</th><th>Dosya</th><th>Boyut</th><th>Durum</th><th>Detay</th></tr></thead>
                <tbody>${ops.map(o => `<tr>
                    <td>${o.id}</td>
                    <td>${(o.started_at||'').substring(0,19)}</td>
                    <td>${typeBadge(o.operation_type)}</td>
                    <td><span style="font-size:11px">${o.trigger_type||'-'} ${o.trigger_detail ? '('+o.trigger_detail+')' : ''}</span></td>
                    <td>${formatNum(o.total_files)}</td>
                    <td>${o.total_size_formatted || formatSize(o.total_size)}</td>
                    <td>${statusBadge(o.status)}</td>
                    <td>
                        <button class="btn btn-sm btn-outline" onclick="showOperationFiles(${o.id})">Dosyalar</button>
                        ${o.operation_type==='archive' && o.status==='completed' ? `<button class="btn btn-sm btn-success" onclick="restoreByOperation(${o.id})">Geri Yukle</button>` : ''}
                    </td>
                </tr>`).join('')}</tbody>
            `;
        } else {
            table.innerHTML = '<div style="text-align:center;padding:40px;color:var(--text-muted);font-size:12px">Filtrelere uygun islem bulunamadi</div>';
        }

        // Sayfalama
        const pag = document.getElementById('ah-pagination');
        if (data.total_pages > 1) {
            let html = '';
            if (ahCurrentPage > 1) html += `<button class="btn btn-sm btn-outline" onclick="loadArchiveHistory(${ahCurrentPage-1})">Onceki</button>`;
            for (let i = Math.max(1, ahCurrentPage-2); i <= Math.min(data.total_pages, ahCurrentPage+2); i++) {
                html += `<button class="btn btn-sm ${i===ahCurrentPage?'btn-primary':'btn-outline'}" onclick="loadArchiveHistory(${i})">${i}</button>`;
            }
            if (ahCurrentPage < data.total_pages) html += `<button class="btn btn-sm btn-outline" onclick="loadArchiveHistory(${ahCurrentPage+1})">Sonraki</button>`;
            pag.innerHTML = html;
        } else {
            pag.innerHTML = '';
        }
        _attachArchiveHistoryViewToggle();
    } catch(e) { console.error('Archive history error:', e); }
}

// Issue #84 — Gorsel ↔ Profesyonel mod toggle for Arsiv Gecmisi.
function _renderArchiveHistoryVisual() {
    const v = document.getElementById('ah-visual-host');
    const g = document.getElementById('ah-grid-host');
    if (v) v.style.display = '';
    if (g) { g.style.display = 'none'; g.innerHTML = ''; }
}

function _renderArchiveHistoryGrid() {
    const v = document.getElementById('ah-visual-host');
    const host = document.getElementById('ah-grid-host');
    if (v) v.style.display = 'none';
    host.style.display = '';
    if (typeof renderEntityList !== 'function') {
        host.innerHTML = '<div style="padding:20px;color:var(--text-muted)">entity-list yuklenmedi</div>';
        return;
    }
    const cached = window.__ahLastReport;
    const ops = (cached && cached.data) ? (cached.data.operations || []) : [];
    if (!ops.length) {
        host.innerHTML = '<div style="padding:20px;color:var(--text-muted)">Filtrelere uygun islem bulunamadi</div>';
        return;
    }
    const rows = ops.map(o => ({
        id: o.id,
        started_at: (o.started_at || '').substring(0, 19),
        operation_type: o.operation_type || '',
        trigger_type: o.trigger_type || '',
        trigger_detail: o.trigger_detail || '',
        total_files: o.total_files || 0,
        total_size: o.total_size || 0,
        total_size_formatted: o.total_size_formatted || formatSize(o.total_size || 0),
        status: o.status || '',
    }));
    renderEntityList(host, {
        rows: rows,
        rowKey: 'id',
        pageSize: 50,
        searchKeys: ['operation_type', 'trigger_type', 'trigger_detail', 'status'],
        columns: [
            {key: 'id', label: 'ID'},
            {key: 'started_at', label: 'Tarih'},
            {key: 'operation_type', label: 'Tur'},
            {key: 'trigger_type', label: 'Tetikleyen'},
            {key: 'trigger_detail', label: 'Detay'},
            {key: 'total_files', label: 'Dosya'},
            {key: 'total_size', label: 'Boyut',
             render: (v, row) => row.total_size_formatted || formatSize(v || 0)},
            {key: 'status', label: 'Durum'},
        ],
        emptyMessage: 'Islem bulunamadi',
    });
}

function _attachArchiveHistoryViewToggle() {
    const page = document.getElementById('page-archive-history');
    if (!page || typeof attachViewToggle !== 'function') return;
    attachViewToggle(page, {
        pageKey: 'archive-history',
        renderVisual: _renderArchiveHistoryVisual,
        renderGrid: _renderArchiveHistoryGrid,
        defaultMode: 'visual',
    });
}

async function showOperationFiles(opId) {
    try {
        const data = await api(`/archive/operations/${opId}/files?page=1&page_size=200`);
        const files = data.files || [];
        const filesHtml = files.length ? files.map(f => `
            <tr>
                <td>${escapeHtml(f.file_name)}</td>
                <td style="max-width:200px;word-break:break-all;font-size:11px" title="${escapeHtml(f.original_path)}">${escapeHtml(f.original_path)}</td>
                <td style="max-width:200px;word-break:break-all;font-size:11px" title="${escapeHtml(f.archive_path||'')}">${escapeHtml(f.archive_path||'-')}</td>
                <td>${escapeHtml(f.file_size_formatted || formatSize(f.file_size))}</td>
                <td>${escapeHtml(f.owner || '-')}</td>
                <td>${escapeHtml((f.archived_at||'').substring(0,19))}</td>
                <td>${f.restored_at ? '<span class="badge badge-success">Geri Yuklendi</span>' : '<span class="badge badge-info">Arsivde</span>'}</td>
            </tr>
        `).join('') : '<tr><td colspan="7" style="text-align:center;padding:20px;color:var(--text-muted)">Dosya bulunamadi</td></tr>';

        const modalContent = `
            <div style="padding:20px">
                <h3 style="margin:0 0 16px 0">Islem #${opId} - Dosya Detaylari (${data.total} dosya)</h3>
                <div style="max-height:500px;overflow-y:auto">
                    <table style="width:100%;font-size:12px">
                        <thead><tr><th>Dosya Adi</th><th>Orijinal Yol</th><th>Arsiv Yolu</th><th>Boyut</th><th>Sahip</th><th>Tarih</th><th>Durum</th></tr></thead>
                        <tbody>${filesHtml}</tbody>
                    </table>
                </div>
                <div style="display:flex;gap:12px;justify-content:flex-end;margin-top:16px">
                    <button class="btn btn-outline" onclick="closeModal('modal-op-detail')">Kapat</button>
                </div>
            </div>
        `;
        let modal = document.getElementById('modal-op-detail');
        if (!modal) {
            modal = document.createElement('div');
            modal.id = 'modal-op-detail';
            modal.className = 'modal-overlay';
            modal.onclick = (e) => { if(e.target===modal) modal.classList.remove('active'); };
            modal.innerHTML = '<div class="modal" style="max-width:1100px;width:95%"></div>';
            document.body.appendChild(modal);
        }
        modal.querySelector('.modal').innerHTML = modalContent;
        modal.classList.add('active');
    } catch(e) { notify('Dosya detaylari yuklenemedi', 'error'); }
}

async function exportArchiveHistory(event) {
    const btn = event?.currentTarget;
    await withButtonLoading(btn, async (signal) => {
        notify('XLS export hazirlaniyor...', 'info');
        // Tum gecmisi cek ve CSV olarak indir
        const resp = await fetch(API + '/archive/history?page=1&page_size=10000', { signal });
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        const data = await resp.json();
        const ops = data.operations || [];
        if (!ops.length) { notify('Export edilecek veri yok', 'warning'); return; }
        let csv = 'ID,Tarih,Tur,Tetikleyen,Detay,Dosya Sayisi,Toplam Boyut,Durum\n';
        ops.forEach(o => {
            csv += `${o.id},"${o.started_at||''}",${o.operation_type},${o.trigger_type||''},${o.trigger_detail||''},${o.total_files||0},${o.total_size||0},${o.status}\n`;
        });
        const blob = new Blob(['\uFEFF' + csv], { type: 'text/csv;charset=utf-8;' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url; a.download = `arsiv_gecmisi_${new Date().toISOString().split('T')[0]}.csv`;
        document.body.appendChild(a); a.click();
        a.remove(); URL.revokeObjectURL(url);
        notify('Export tamamlandi', 'success');
    });
}

// ═══════════════════════════════════════════════════
// KOPYA DOSYALAR
// ═══════════════════════════════════════════════════
let dupCurrentPage = 1;
let dupSelectedFiles = new Set();

async function loadDuplicates(page) {
    dupCurrentPage = page || 1;
    const sourceId = document.getElementById('dup-source')?.value;
    if (!sourceId) {
        _setHtmlSafe('dup-summary-cards', '<div style="padding:20px;color:var(--text-muted)">Lutfen bir kaynak secin</div>');
        _setHtmlSafe('dup-table', '');
        // Still attach the toggle so it appears even without a source.
        _attachDuplicatesViewToggle();
        return;
    }

    try {
        const data = await api(`/reports/duplicates/${sourceId}?page=${dupCurrentPage}&page_size=50`);
        const groups = data.groups || [];
        // Cache for grid mode (avoid re-fetching when user toggles view).
        window.__dupLastReport = { sourceId: sourceId, page: dupCurrentPage, data: data };

        // Export butonu goster
        const expBtn = document.getElementById('dup-export-btn');
        if (expBtn) expBtn.style.display = (data.total_groups > 0) ? '' : 'none';

        // Ozet kartlar
        _setHtmlSafe('dup-summary-cards', `
            <div class="card accent"><div class="card-label">Kopya Gruplar</div><div class="card-value">${formatNum(data.total_groups || 0)}</div></div>
            <div class="card danger"><div class="card-label">Israf Edilen Alan</div><div class="card-value">${formatSize(data.total_waste_size || 0)}</div></div>
            <div class="card warning"><div class="card-label">Toplam Kopya Dosya</div><div class="card-value">${formatNum(data.total_files || 0)}</div></div>
        `);

        // Tablo
        const table = document.getElementById('dup-table');
        if (table) {
        if (groups.length) {
            table.innerHTML = `
                <thead><tr><th style="width:30px"></th><th>Dosya Adi</th><th>Boyut</th><th>Kopya Sayisi</th><th>Israf</th><th>Detay</th></tr></thead>
                <tbody>${groups.map((g, idx) => `
                    <tr class="dup-group-row" data-group="${idx}">
                        <td><span class="icon" style="cursor:pointer" onclick="toggleDupGroup(${idx})">▶</span></td>
                        <td><strong>${escapeHtml(g.file_name)}</strong></td>
                        <td>${formatSize(g.file_size)}</td>
                        <td><span class="badge badge-warning">${g.count} kopya</span></td>
                        <td style="color:var(--danger)">${formatSize(g.waste_size)}</td>
                        <td><button class="btn btn-sm btn-outline" onclick="toggleDupGroup(${idx})">Dosyalari Gor</button></td>
                    </tr>
                    <tr class="dup-detail-row" id="dup-detail-${idx}" style="display:none">
                        <td colspan="6" style="padding:0 0 0 40px;background:var(--bg-secondary)">
                            <table style="width:100%;font-size:12px;margin:8px 0">
                                <thead><tr><th style="width:30px"><input type="checkbox" onchange="toggleDupGroupSelect(${idx}, this.checked)"></th><th>Yol</th><th>Sahip</th><th>Son Erisim</th><th>Son Degisiklik</th></tr></thead>
                                <tbody>${(g.files||[]).map((f, fi) => `
                                    <tr>
                                        <td><input type="checkbox" class="dup-check" data-file-id="${f.id}" data-group="${idx}" onchange="updateDupSelection()"></td>
                                        <td style="word-break:break-all;max-width:400px" title="${escapeHtml(f.file_path)}">${escapeHtml(f.file_path)}</td>
                                        <td>${escapeHtml(f.owner || '-')}</td>
                                        <td>${escapeHtml((f.last_access_time||'').substring(0,10))}</td>
                                        <td>${escapeHtml((f.last_modify_time||'').substring(0,10))}</td>
                                    </tr>
                                `).join('')}</tbody>
                            </table>
                        </td>
                    </tr>
                `).join('')}</tbody>
            `;
        } else {
            table.innerHTML = '<div style="text-align:center;padding:40px;color:var(--text-muted);font-size:12px">Kopya dosya bulunamadi</div>';
        }
        }  // /if (table)

        // Sayfalama
        const totalPages = data.total_pages || 1;
        const pag = document.getElementById('dup-pagination');
        if (pag) {
        if (totalPages > 1) {
            let html = '';
            if (dupCurrentPage > 1) html += `<button class="btn btn-sm btn-outline" onclick="loadDuplicates(${dupCurrentPage-1})">Onceki</button>`;
            for (let i = Math.max(1, dupCurrentPage-2); i <= Math.min(totalPages, dupCurrentPage+2); i++) {
                html += `<button class="btn btn-sm ${i===dupCurrentPage?'btn-primary':'btn-outline'}" onclick="loadDuplicates(${i})">${i}</button>`;
            }
            if (dupCurrentPage < totalPages) html += `<button class="btn btn-sm btn-outline" onclick="loadDuplicates(${dupCurrentPage+1})">Sonraki</button>`;
            pag.innerHTML = html;
        } else {
            pag.innerHTML = '';
        }
        }  // /if (pag)

        dupSelectedFiles.clear();
        updateDupSelection();
        _attachDuplicatesViewToggle();
    } catch(e) { console.error('Duplicates load error:', e); }
}

// Issue #84 — Gorsel ↔ Profesyonel mod toggle for Kopya Dosyalar.
function _renderDuplicatesVisual() {
    document.getElementById('dup-visual-host').style.display = '';
    document.getElementById('dup-grid-host').style.display = 'none';
    document.getElementById('dup-grid-host').innerHTML = '';
}

function _renderDuplicatesGrid() {
    const host = document.getElementById('dup-grid-host');
    document.getElementById('dup-visual-host').style.display = 'none';
    host.style.display = '';
    if (typeof renderEntityList !== 'function') {
        host.innerHTML = '<div style="padding:20px;color:var(--text-muted)">entity-list yuklenmedi</div>';
        return;
    }
    const cached = window.__dupLastReport;
    if (!cached || !cached.data) {
        host.innerHTML = '<div style="padding:20px;color:var(--text-muted)">Once bir kaynak secin</div>';
        return;
    }
    const groups = cached.data.groups || [];
    // Flatten group → file rows; expose dup_group_id and last_modify on the row.
    const rows = [];
    groups.forEach((g, gi) => {
        (g.files || []).forEach(f => {
            rows.push({
                id: f.id,
                file_path: f.file_path,
                file_name: f.file_name || g.file_name,
                size: g.file_size,
                size_formatted: g.file_size_formatted || formatSize(g.file_size || 0),
                dup_group_id: gi + 1,
                last_modify: (f.last_modify_time || '').substring(0, 19),
                owner: f.owner || '',
            });
        });
    });
    renderEntityList(host, {
        rows: rows,
        rowKey: 'id',
        pageSize: 50,
        searchKeys: ['file_path', 'file_name', 'owner'],
        columns: [
            {key: 'file_path', label: 'Dosya Yolu'},
            {key: 'file_name', label: 'Dosya Adi'},
            {key: 'size', label: 'Boyut',
             render: (v, row) => row.size_formatted || formatSize(v || 0)},
            {key: 'dup_group_id', label: 'Grup'},
            {key: 'last_modify', label: 'Son Degisiklik'},
            {key: 'owner', label: 'Sahip'},
        ],
        toolbar: {
            // Issue #83 Phase 1 — quarantine-only delete. Hard delete is
            // intentionally NOT exposed in this round.
            bulkActions: [
                {label: 'Hedefe Git', action: 'open-folder'},
                {label: "Quarantine'e Tasi", action: 'quarantine',
                 danger: true},
            ],
        },
        onBulkAction: async (actionId, selectedRows) => {
            if (actionId === 'open-folder') {
                if (selectedRows.length > 5) {
                    notify('Bir defada en fazla 5 konum acilabilir', 'warning');
                    return;
                }
                for (const r of selectedRows) {
                    const target = (r.file_path || '').replace(/[\\/][^\\/]*$/, '');
                    if (target) openFolder(target);
                }
                return;
            }
            if (actionId === 'quarantine') {
                const sourceId = document.getElementById('dup-source')?.value;
                if (!sourceId) {
                    notify('Once bir kaynak secin', 'warning');
                    return;
                }
                const fileIds = selectedRows.map(r => r.id).filter(v => v != null);
                if (!fileIds.length) {
                    notify('Once en az bir dosya secin', 'warning');
                    return;
                }
                await openQuarantineConfirmModal(parseInt(sourceId), fileIds);
                return;
            }
            notify('Bilinmeyen toplu islem: ' + actionId, 'warning');
        },
        emptyMessage: 'Kopya dosya bulunamadi',
    });
}

function _attachDuplicatesViewToggle() {
    const page = document.getElementById('page-duplicates');
    if (!page || typeof attachViewToggle !== 'function') return;
    attachViewToggle(page, {
        pageKey: 'duplicates',
        renderVisual: _renderDuplicatesVisual,
        renderGrid: _renderDuplicatesGrid,
        defaultMode: 'visual',
    });
}

function toggleDupGroup(idx) {
    const row = document.getElementById('dup-detail-' + idx);
    const icon = document.querySelector(`.dup-group-row[data-group="${idx}"] .icon`);
    if (row.style.display === 'none') {
        row.style.display = '';
        if (icon) icon.textContent = '▼';
    } else {
        row.style.display = 'none';
        if (icon) icon.textContent = '▶';
    }
}

function toggleDupGroupSelect(idx, checked) {
    document.querySelectorAll(`.dup-check[data-group="${idx}"]`).forEach(cb => { cb.checked = checked; });
    updateDupSelection();
}

function updateDupSelection() {
    dupSelectedFiles.clear();
    let totalSize = 0;
    document.querySelectorAll('.dup-check:checked').forEach(cb => {
        dupSelectedFiles.add(parseInt(cb.dataset.fileId));
    });
    const actionsDiv = document.getElementById('dup-actions');
    const infoSpan = document.getElementById('dup-selected-info');
    if (dupSelectedFiles.size > 0) {
        actionsDiv.style.display = 'flex';
        infoSpan.textContent = `${dupSelectedFiles.size} dosya secildi`;
    } else {
        actionsDiv.style.display = 'none';
    }
}

async function archiveSelectedDuplicates() {
    if (dupSelectedFiles.size === 0) return;
    const sourceId = document.getElementById('dup-source')?.value;
    if (!sourceId) return;

    if (!confirm(`${dupSelectedFiles.size} dosya arsivlenecek. Devam etmek istiyor musunuz?`)) return;

    try {
        const resp = await api('/archive/selective', {
            method: 'POST',
            body: JSON.stringify({ source_id: parseInt(sourceId), file_ids: Array.from(dupSelectedFiles) })
        });
        notify(`${resp.archived || 0} dosya arsivlendi (${resp.total_size_formatted || ''})`, 'success');
        loadDuplicates(dupCurrentPage);
    } catch(e) { notify('Arsivleme hatasi: ' + e.message, 'error'); }
}

// Issue #83 Phase 1 — quarantine-only delete (no hard delete in this round).
// Visual mode entry point: gathers the checkbox selection and hands off to
// the same modal that the entity-list bulk action uses.
async function quarantineSelectedDuplicates() {
    if (dupSelectedFiles.size === 0) return;
    const sourceId = document.getElementById('dup-source')?.value;
    if (!sourceId) return;
    await openQuarantineConfirmModal(parseInt(sourceId),
                                       Array.from(dupSelectedFiles));
}

async function openQuarantineConfirmModal(sourceId, fileIds) {
    if (!fileIds || !fileIds.length) {
        notify('Once en az bir dosya secin', 'warning');
        return;
    }
    let preview;
    try {
        preview = await api(
            `/reports/duplicates/${sourceId}/quarantine/preview`,
            {method: 'POST',
             body: JSON.stringify({file_ids: fileIds})}
        );
    } catch (e) {
        notify('Onizleme hatasi: ' + (e && e.message ? e.message : e), 'error');
        return;
    }
    const wouldMove = preview.would_move || 0;
    const skippedHeld = preview.skipped_held || 0;
    const skippedLast = preview.skipped_last_copy || 0;
    const skippedMissing = preview.skipped_missing || 0;
    const sizeGb = preview.total_size_freed_gb || 0;
    const errors = (preview.errors || []).slice(0, 5);

    const modal = _ensureModal('modal-quarantine-confirm', 720);
    modal.querySelector('.modal').innerHTML = `
        <div style="padding:20px">
            <h3 style="margin:0 0 16px 0;color:var(--danger)">Quarantine'e Tasi</h3>
            <div style="background:var(--bg-secondary);border:1px solid var(--border);border-radius:var(--radius);padding:14px;margin-bottom:14px;font-size:13px;line-height:1.7">
                <div><strong>${wouldMove}</strong> dosya quarantine'e tasinacak (<strong>${sizeGb.toFixed(4)} GB</strong> serbest kalacak).</div>
                <div>${skippedHeld} dosya legal hold nedeniyle atlanacak.</div>
                <div>${skippedLast} dosya son kopya oldugu icin atlanacak (veri kaybi koruma).</div>
                ${skippedMissing ? `<div>${skippedMissing} dosya disk uzerinde bulunamadi.</div>` : ''}
                ${errors.length ? `<div style="margin-top:8px;color:var(--warning)">Uyarilar:<br>${errors.map(er => escapeHtml('- ' + (er.error || er) + (er.id ? ' (id=' + er.id + ')' : ''))).join('<br>')}</div>` : ''}
            </div>
            <div style="background:rgba(220,38,38,0.08);border:1px solid var(--danger);border-radius:var(--radius);padding:12px;margin-bottom:14px;font-size:12px;color:var(--text-primary)">
                <strong>Phase 1: Quarantine-only.</strong> Dosyalar fiziksel olarak silinmez —
                <code>data/quarantine/&lt;YYYYMMDD&gt;/...</code> altina tasinir. Geri yukleme
                forensik kayitlardan yapilabilir.
            </div>
            <div class="form-group">
                <label>Onaylamak icin <code>QUARANTINE</code> yazin:</label>
                <input id="quar-confirm-token" placeholder="QUARANTINE" autocomplete="off"
                       style="font-family:monospace;text-transform:none">
            </div>
            <div class="modal-actions">
                <button class="btn btn-outline" onclick="closeModal('modal-quarantine-confirm')">Iptal</button>
                <button id="quar-confirm-btn" class="btn btn-danger" ${wouldMove === 0 ? 'disabled' : ''}>Onayla ve Tasi</button>
            </div>
        </div>
    `;
    modal.classList.add('active');
    const btn = modal.querySelector('#quar-confirm-btn');
    if (btn) {
        btn.addEventListener('click', async () => {
            const token = (modal.querySelector('#quar-confirm-token')?.value || '').trim();
            if (token !== 'QUARANTINE') {
                notify('Onay icin "QUARANTINE" yazmaniz gerekiyor', 'warning');
                return;
            }
            btn.disabled = true;
            btn.textContent = 'Tasiniyor...';
            try {
                const result = await api(
                    `/reports/duplicates/${sourceId}/quarantine`,
                    {method: 'POST',
                     body: JSON.stringify({
                         file_ids: fileIds,
                         confirm: true,
                         safety_token: 'QUARANTINE',
                     })}
                );
                closeModal('modal-quarantine-confirm');
                notify(`${result.moved || 0} dosya quarantine'e tasindi`, 'success');
                showQuarantineGainModal(result);
                loadDuplicates(dupCurrentPage);
            } catch (e) {
                notify('Quarantine hatasi: ' + (e && e.message ? e.message : e), 'error');
                btn.disabled = false;
                btn.textContent = 'Onayla ve Tasi';
            }
        });
    }
}

function showQuarantineGainModal(result) {
    const before = result.before || {};
    const after = result.after || {};
    const delta = result.delta || {};
    const reportId = result.gain_report_id;
    const fmtGb = v => (typeof v === 'number' ? v.toFixed(4) + ' GB' : '-');

    const card = (title, snap) => `
        <div class="card" style="border-top:3px solid var(--accent)">
            <div class="card-label">${title}</div>
            <div style="font-size:11px;line-height:1.7;color:var(--text-secondary);margin-top:6px">
                Toplam Dosya: <strong>${snap.total_files ?? '-'}</strong><br>
                Toplam Boyut: <strong>${fmtGb(snap.total_size_gb)}</strong><br>
                Kopya Grup: <strong>${snap.duplicate_groups ?? '-'}</strong><br>
                Israf: <strong>${fmtGb(snap.duplicate_waste_gb)}</strong>
            </div>
        </div>`;

    const modal = _ensureModal('modal-quarantine-gain', 820);
    modal.querySelector('.modal').innerHTML = `
        <div style="padding:20px">
            <h3 style="margin:0 0 16px 0">Kazanim Raporu</h3>
            <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px;margin-bottom:16px">
                ${card('Neydik (Before)', before)}
                ${card('Ne Olduk (After)', after)}
                <div class="card" style="border-top:3px solid var(--success)">
                    <div class="card-label">Kazanim (Delta)</div>
                    <div style="font-size:11px;line-height:1.7;color:var(--text-secondary);margin-top:6px">
                        Tasinan: <strong>${result.moved || 0}</strong> dosya<br>
                        Atlanan (hold): <strong>${result.skipped_held || 0}</strong><br>
                        Atlanan (son kopya): <strong>${result.skipped_last_copy || 0}</strong><br>
                        Serbest Alan: <strong>${(result.total_size_freed_gb || 0).toFixed(4)} GB</strong><br>
                        Kopya Grup Azalmasi: <strong>${delta.duplicate_groups || 0}</strong>
                    </div>
                </div>
            </div>
            ${(result.errors && result.errors.length) ? `
            <div style="background:rgba(245,158,11,0.08);border:1px solid var(--warning);border-radius:var(--radius);padding:10px;margin-bottom:14px;font-size:12px">
                <strong>Uyarilar (${result.errors.length}):</strong><br>
                ${result.errors.slice(0, 10).map(er => '- ' + (er.error || JSON.stringify(er))).join('<br>')}
            </div>` : ''}
            <div class="modal-actions">
                ${reportId != null ? `<button class="btn btn-outline" onclick="closeModal('modal-quarantine-gain');showPage('operations');setTimeout(()=>showGainReportDetail(${reportId}), 250)">Operations History'de Gor</button>` : ''}
                <button class="btn btn-primary" onclick="closeModal('modal-quarantine-gain')">Kapat</button>
            </div>
        </div>
    `;
    modal.classList.add('active');
}

function _ensureModal(id, maxWidth) {
    let modal = document.getElementById(id);
    if (!modal) {
        modal = document.createElement('div');
        modal.id = id;
        modal.className = 'modal-overlay';
        modal.onclick = (e) => { if (e.target === modal) modal.classList.remove('active'); };
        modal.innerHTML = `<div class="modal" style="max-width:${maxWidth || 720}px;width:95%"></div>`;
        document.body.appendChild(modal);
    }
    return modal;
}

// ═══════════════════════════════════════════════════
// OPERATIONS HISTORY (issue #83 — gain reports)
// ═══════════════════════════════════════════════════
// ═══════════════════════════════════════════════════
// QUARANTINE BROWSER (issue #110 Phase 2)
// ═══════════════════════════════════════════════════
async function loadQuarantine() {
    const host = document.getElementById('qrn-list-host');
    const banner = document.getElementById('qrn-disabled-banner');
    const daysSpan = document.getElementById('qrn-days');
    const filter = document.getElementById('qrn-filter')?.value || '';
    if (!host) return;
    if (typeof renderEntityList !== 'function') {
        host.innerHTML = '<div style="padding:20px;color:var(--text-muted)">entity-list yuklenmedi</div>';
        return;
    }
    host.innerHTML = '<div style="padding:20px;color:var(--text-muted)">Yukleniyor...</div>';
    let data;
    try {
        const qs = filter ? `?status=${encodeURIComponent(filter)}` : '';
        data = await api('/quarantine' + qs);
    } catch (e) {
        host.innerHTML = `<div style="padding:20px;color:var(--danger)">Yukleme hatasi: ${escapeHtml(e.message || e)}</div>`;
        return;
    }
    if (banner) banner.style.display = data.enabled === false ? 'block' : 'none';
    if (daysSpan) daysSpan.textContent = String(data.quarantine_days || 30);
    const rows = (data.rows || []).map(r => Object.assign({}, r, {
        _status_label: r.status === 'purged' ? 'Hard Delete Edildi'
            : (r.status === 'restored' ? 'Geri Yuklendi' : 'Karantinada'),
    }));
    if (!rows.length) {
        host.innerHTML = '<div style="padding:20px;color:var(--text-muted)">Kayit yok</div>';
        return;
    }
    renderEntityList(host, {
        rows,
        rowKey: 'id',
        pageSize: 50,
        searchKeys: ['original_path', 'quarantine_path', 'moved_by'],
        columns: [
            {key: 'id', label: 'ID', sort: true},
            {key: 'original_path', label: 'Orijinal Yol', sort: true},
            {key: 'quarantine_path', label: 'Karantina Yolu', sort: true},
            {key: 'moved_at', label: 'Tasinma', sort: true},
            {key: 'will_purge_at', label: 'Otomatik Silinme', sort: true,
             render: (v, row) => row.status === 'quarantined' ? (v || '-') : '<span style="color:var(--text-muted)">-</span>'},
            {key: '_status_label', label: 'Durum', sort: true,
             render: (v, row) => {
                 const color = row.status === 'purged' ? 'var(--danger)'
                     : (row.status === 'restored' ? 'var(--success)' : 'var(--accent)');
                 return `<span style="color:${color};font-weight:600">${v}</span>`;
             }},
        ],
        toolbar: {
            bulkActions: [
                {label: 'Geri Yukle', action: 'restore', confirm: true},
                {label: 'Hemen Sil', action: 'purge-now', confirm: true},
            ],
        },
        onBulkAction: async (actionId, selectedRows) => {
            if (!selectedRows.length) {
                notify('Lutfen en az bir kayit secin', 'warning');
                return;
            }
            if (actionId === 'restore') {
                await _bulkQuarantineRestore(selectedRows);
            } else if (actionId === 'purge-now') {
                await _bulkQuarantinePurge(selectedRows);
            }
        },
        emptyMessage: 'Bu filtre icin kayit yok',
    });
}

async function _bulkQuarantineRestore(selectedRows) {
    const eligible = selectedRows.filter(r => r.status === 'quarantined');
    if (!eligible.length) {
        notify('Sadece "Karantinada" durumundaki kayitlar geri yuklenebilir', 'warning');
        return;
    }
    if (!confirm(`${eligible.length} dosya orijinal konumlarina geri yuklenecek. Emin misiniz?`)) return;
    let ok = 0, failed = 0;
    for (const row of eligible) {
        try {
            await api(`/quarantine/${row.id}/restore`, {
                method: 'POST',
                body: JSON.stringify({confirm: true}),
            });
            ok += 1;
        } catch (e) {
            failed += 1;
            console.warn('restore failed', row.id, e);
        }
    }
    notify(`Geri yukleme: ${ok} basarili, ${failed} basarisiz`,
           failed ? 'warning' : 'success');
    loadQuarantine();
}

async function _bulkQuarantinePurge(selectedRows) {
    const eligible = selectedRows.filter(r => r.status === 'quarantined');
    if (!eligible.length) {
        notify('Sadece "Karantinada" durumundaki kayitlar hard delete edilebilir', 'warning');
        return;
    }
    const typed = prompt(
        `${eligible.length} dosya KALICI OLARAK silinecek. ` +
        `Onay icin "PURGE" yazin (buyuk harflerle):`
    );
    if (typed !== 'PURGE') {
        notify('Onay metni "PURGE" yazilmadi — islem iptal edildi', 'info');
        return;
    }
    let ok = 0, sha = 0, failed = 0;
    for (const row of eligible) {
        try {
            await api(`/quarantine/${row.id}/purge`, {
                method: 'POST',
                body: JSON.stringify({confirm: true, safety_token: 'PURGE'}),
            });
            ok += 1;
        } catch (e) {
            // 409 with sha_mismatch — surface separately so the operator
            // knows which files were preserved.
            if (e && e.status === 409 && /sha_mismatch/i.test(e.message || '')) {
                sha += 1;
            } else {
                failed += 1;
            }
            console.warn('purge failed', row.id, e);
        }
    }
    let msg = `Hard delete: ${ok} basarili`;
    if (sha) msg += `, ${sha} SHA uyusmazligi (adli koruma)`;
    if (failed) msg += `, ${failed} basarisiz`;
    notify(msg, (failed || sha) ? 'warning' : 'success');
    loadQuarantine();
}

async function loadOperations() {
    const tbody = document.getElementById('ops-tbody');
    const filter = document.getElementById('ops-filter')?.value || '';
    if (tbody) tbody.innerHTML = '<tr><td colspan="6" style="padding:20px;text-align:center;color:var(--text-muted)">Yukleniyor...</td></tr>';
    try {
        const qs = filter ? `?operation=${encodeURIComponent(filter)}&limit=50` : '?limit=50';
        const data = await api('/operations/history' + qs);
        const reports = data.reports || [];
        if (!reports.length) {
            if (tbody) tbody.innerHTML = '<tr><td colspan="6" style="padding:20px;text-align:center;color:var(--text-muted)">Henuz islem kaydi yok</td></tr>';
            return;
        }
        if (tbody) tbody.innerHTML = reports.map(r => {
            const delta = r.delta || {};
            const after = r.after || {};
            const wasteGb = (delta.duplicate_waste_gb || 0).toFixed(4);
            return `<tr>
                <td>${r.id}</td>
                <td>${escapeHtml(r.started_at || '')}</td>
                <td><code>${escapeHtml(r.operation || '-')}</code></td>
                <td>${after.total_files ?? '-'} dosya</td>
                <td style="color:var(--success)">${wasteGb} GB</td>
                <td><button class="btn btn-sm btn-outline" onclick="showGainReportDetail(${r.id})">Detay</button></td>
            </tr>`;
        }).join('');
    } catch (e) {
        if (tbody) tbody.innerHTML = `<tr><td colspan="6" style="padding:20px;text-align:center;color:var(--danger)">Yukleme hatasi: ${escapeHtml(e.message || e)}</td></tr>`;
    }
}

async function showGainReportDetail(opId) {
    let detail;
    try {
        detail = await api('/operations/' + opId);
    } catch (e) {
        notify('Detay yuklenemedi: ' + (e && e.message ? e.message : e), 'error');
        return;
    }
    const before = detail.before || {};
    const after = detail.after || {};
    const delta = detail.delta || {};
    const fmtGb = v => (typeof v === 'number' ? v.toFixed(4) + ' GB' : '-');
    const card = (title, snap, color) => `
        <div class="card" style="border-top:3px solid ${color}">
            <div class="card-label">${title}</div>
            <div style="font-size:11px;line-height:1.7;color:var(--text-secondary);margin-top:6px">
                Toplam Dosya: <strong>${snap.total_files ?? '-'}</strong><br>
                Toplam Boyut: <strong>${fmtGb(snap.total_size_gb)}</strong><br>
                Kopya Grup: <strong>${snap.duplicate_groups ?? '-'}</strong><br>
                Israf: <strong>${fmtGb(snap.duplicate_waste_gb)}</strong>
            </div>
        </div>`;

    const modal = _ensureModal('modal-operation-detail', 900);
    modal.querySelector('.modal').innerHTML = `
        <div style="padding:20px">
            <h3 style="margin:0 0 8px 0">Islem #${detail.id} — <code>${detail.operation}</code></h3>
            <div style="font-size:12px;color:var(--text-muted);margin-bottom:16px">
                Baslangic: ${detail.started_at || '-'} &nbsp;|&nbsp;
                Tamamlandi: ${detail.completed_at || '-'} &nbsp;|&nbsp;
                Scan: ${detail.scan_id ?? '-'}
            </div>
            <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px;margin-bottom:16px">
                ${card('Neydik (Before)', before, 'var(--accent)')}
                ${card('Ne Olduk (After)', after, 'var(--accent)')}
                ${card('Kazanim (Delta)', delta, 'var(--success)')}
            </div>
            <details style="margin-bottom:14px">
                <summary style="cursor:pointer;font-size:12px;color:var(--text-secondary)">Ham JSON</summary>
                <pre style="background:var(--bg-secondary);border:1px solid var(--border);border-radius:6px;padding:10px;font-size:11px;max-height:280px;overflow:auto">${_escapeHtml(JSON.stringify({before, after, delta}, null, 2))}</pre>
            </details>
            <div class="modal-actions">
                <button class="btn btn-outline" onclick="downloadOperationXlsx(${detail.id})">XLS indir</button>
                <button class="btn btn-primary" onclick="closeModal('modal-operation-detail')">Kapat</button>
            </div>
        </div>
    `;
    modal.classList.add('active');
}

function downloadOperationXlsx(opId) {
    // Issue #83 — link to /api/operations/{id}/export.xlsx. The endpoint
    // streams a single-sheet Metric / Before / After / Delta workbook.
    try {
        window.location.href = `/api/operations/${opId}/export.xlsx`;
    } catch (e) {
        if (typeof notify === 'function') {
            notify('XLS indirilemedi: ' + (e && e.message ? e.message : e),
                   'error');
        }
    }
}

function _escapeHtml(s) {
    return String(s).replace(/[&<>"']/g, c => ({
        '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'
    }[c]));
}

async function exportDuplicatesCSV(event) {
    const sourceId = document.getElementById('dup-source')?.value;
    if (!sourceId) { notify('Lutfen kaynak secin', 'warning'); return; }
    const btn = event?.currentTarget;
    await withButtonLoading(btn, async (signal) => {
        const r = await fetch(`/api/export/start?report_type=duplicates&source_id=${sourceId}`, { method: 'POST', signal });
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        const data = await r.json();
        notify('Excel raporu arka planda hazirlaniyor...', 'info');
        if (data && data.job_id) pollExportStatus(data.job_id);
    });
}

// ═══════════════════════════════════════════════════
// BUYUME ANALIZI
// ═══════════════════════════════════════════════════
let growthCharts = {};

async function loadGrowth() {
    const sourceId = document.getElementById('growth-source')?.value;
    if (!sourceId) {
        _setHtmlSafe('growth-summary-cards', '<div style="padding:20px;color:var(--text-muted)">Lutfen bir kaynak secin</div>');
        return;
    }

    try {
        const data = await api(`/growth/${sourceId}`);

        // Ozet kartlar
        const yearly = data.yearly || [];
        const monthly = data.monthly || [];
        const daily = data.daily || [];
        const totalGrowth = yearly.length >= 2 ? yearly[yearly.length-1].total_size - yearly[0].total_size : 0;
        const monthlyAvg = monthly.length >= 2 ? Math.round((monthly[monthly.length-1].total_size - monthly[0].total_size) / monthly.length) : 0;

        _setHtmlSafe('growth-summary-cards', `
            <div class="card accent"><div class="card-label">Toplam Buyume</div><div class="card-value">${formatSize(Math.abs(totalGrowth))}</div></div>
            <div class="card warning"><div class="card-label">Aylik Ortalama</div><div class="card-value">${formatSize(Math.abs(monthlyAvg))}</div></div>
            <div class="card purple"><div class="card-label">Tarama Sayisi</div><div class="card-value">${formatNum(data.total_scans || 0)}</div></div>
        `);

        // Yillik grafik
        if (yearly.length) {
            const ctx = document.getElementById('growth-yearly-chart').getContext('2d');
            if (growthCharts.yearly) growthCharts.yearly.destroy();
            growthCharts.yearly = new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: yearly.map(y => y.year),
                    datasets: [{
                        label: 'Toplam Boyut (GB)',
                        data: yearly.map(y => (y.total_size / (1024*1024*1024)).toFixed(2)),
                        backgroundColor: 'rgba(0, 212, 255, 0.6)',
                        borderColor: 'rgba(0, 212, 255, 1)',
                        borderWidth: 1
                    }, {
                        label: 'Dosya Sayisi (K)',
                        data: yearly.map(y => (y.total_files / 1000).toFixed(1)),
                        backgroundColor: 'rgba(255, 171, 0, 0.6)',
                        borderColor: 'rgba(255, 171, 0, 1)',
                        borderWidth: 1,
                        yAxisID: 'y1'
                    }]
                },
                options: {
                    responsive: true,
                    plugins: { legend: { labels: { color: '#94a3b8' } } },
                    scales: {
                        y: { ticks: { color: '#94a3b8' }, grid: { color: 'rgba(255,255,255,0.05)' } },
                        y1: { position: 'right', ticks: { color: '#94a3b8' }, grid: { display: false } },
                        x: { ticks: { color: '#94a3b8' }, grid: { color: 'rgba(255,255,255,0.05)' } }
                    }
                }
            });
        }

        // Aylik grafik
        if (monthly.length) {
            const ctx = document.getElementById('growth-monthly-chart').getContext('2d');
            if (growthCharts.monthly) growthCharts.monthly.destroy();
            growthCharts.monthly = new Chart(ctx, {
                type: 'line',
                data: {
                    labels: monthly.map(m => m.month),
                    datasets: [{
                        label: 'Toplam Boyut (GB)',
                        data: monthly.map(m => (m.total_size / (1024*1024*1024)).toFixed(2)),
                        borderColor: 'rgba(0, 212, 255, 1)',
                        backgroundColor: 'rgba(0, 212, 255, 0.1)',
                        fill: true, tension: 0.3
                    }]
                },
                options: {
                    responsive: true,
                    plugins: { legend: { labels: { color: '#94a3b8' } } },
                    scales: {
                        y: { ticks: { color: '#94a3b8' }, grid: { color: 'rgba(255,255,255,0.05)' } },
                        x: { ticks: { color: '#94a3b8', maxRotation: 45 }, grid: { color: 'rgba(255,255,255,0.05)' } }
                    }
                }
            });
        }

        // Gunluk grafik
        if (daily.length) {
            const ctx = document.getElementById('growth-daily-chart').getContext('2d');
            if (growthCharts.daily) growthCharts.daily.destroy();
            growthCharts.daily = new Chart(ctx, {
                type: 'line',
                data: {
                    labels: daily.map(d => d.day),
                    datasets: [{
                        label: 'Toplam Boyut (GB)',
                        data: daily.map(d => (d.total_size / (1024*1024*1024)).toFixed(2)),
                        borderColor: 'rgba(76, 175, 80, 1)',
                        backgroundColor: 'rgba(76, 175, 80, 0.1)',
                        fill: true, tension: 0.3
                    }, {
                        label: 'Dosya Sayisi (K)',
                        data: daily.map(d => (d.total_files / 1000).toFixed(1)),
                        borderColor: 'rgba(255, 171, 0, 1)',
                        backgroundColor: 'rgba(255, 171, 0, 0.1)',
                        fill: false, tension: 0.3, yAxisID: 'y1'
                    }]
                },
                options: {
                    responsive: true,
                    plugins: { legend: { labels: { color: '#94a3b8' } } },
                    scales: {
                        y: { ticks: { color: '#94a3b8' }, grid: { color: 'rgba(255,255,255,0.05)' } },
                        y1: { position: 'right', ticks: { color: '#94a3b8' }, grid: { display: false } },
                        x: { ticks: { color: '#94a3b8', maxRotation: 45 }, grid: { color: 'rgba(255,255,255,0.05)' } }
                    }
                }
            });
        }

        // En cok dosya olusturanlar
        const creators = data.top_creators || [];
        const creatorsTable = document.getElementById('growth-creators-table');
        if (creators.length) {
            creatorsTable.innerHTML = `
                <thead><tr><th>Kullanici</th><th>Dosya Sayisi</th><th>Toplam Boyut</th><th>Oran</th></tr></thead>
                <tbody>${creators.map(c => `
                    <tr>
                        <td><strong>${escapeHtml(c.owner || 'Bilinmiyor')}</strong></td>
                        <td>${formatNum(c.file_count)}</td>
                        <td>${formatSize(c.total_size)}</td>
                        <td>
                            <div style="display:flex;align-items:center;gap:8px">
                                <div style="flex:1;height:6px;background:var(--bg-secondary);border-radius:3px;overflow:hidden">
                                    <div style="width:${c.percentage || 0}%;height:100%;background:var(--accent);border-radius:3px"></div>
                                </div>
                                <span style="font-size:11px;color:var(--text-muted)">${(c.percentage || 0).toFixed(1)}%</span>
                            </div>
                        </td>
                    </tr>
                `).join('')}</tbody>
            `;
        } else {
            creatorsTable.innerHTML = '<div style="text-align:center;padding:20px;color:var(--text-muted);font-size:12px">Veri bulunamadi</div>';
        }
    } catch(e) { console.error('Growth load error:', e); }
}

// ═══════════════════════════════════════════════════
// LEGAL HOLDS (issue #59 + Bug 4 follow-up)
// ═══════════════════════════════════════════════════
function _lhEsc(s) { return String(s == null ? '' : s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }

function _renderLhTable(container, rows, opts) {
    const showRelease = !!(opts && opts.showRelease);
    if (!rows.length) {
        container.innerHTML = '<div style="padding:20px;text-align:center;color:var(--text-muted)">' + (opts && opts.empty || 'Kayit yok') + '</div>';
        return;
    }
    const head = `
        <tr>
            <th style="padding:8px 10px;text-align:left;border-bottom:1px solid var(--border);font-size:12px;color:var(--text-secondary)">#</th>
            <th style="padding:8px 10px;text-align:left;border-bottom:1px solid var(--border);font-size:12px;color:var(--text-secondary)">Pattern</th>
            <th style="padding:8px 10px;text-align:left;border-bottom:1px solid var(--border);font-size:12px;color:var(--text-secondary)">Sebep</th>
            <th style="padding:8px 10px;text-align:left;border-bottom:1px solid var(--border);font-size:12px;color:var(--text-secondary)">Vaka</th>
            <th style="padding:8px 10px;text-align:left;border-bottom:1px solid var(--border);font-size:12px;color:var(--text-secondary)">Olusturan</th>
            <th style="padding:8px 10px;text-align:left;border-bottom:1px solid var(--border);font-size:12px;color:var(--text-secondary)">Olusturma</th>
            ${showRelease ? '<th style="padding:8px 10px;text-align:left;border-bottom:1px solid var(--border);font-size:12px;color:var(--text-secondary)">Islem</th>' : '<th style="padding:8px 10px;text-align:left;border-bottom:1px solid var(--border);font-size:12px;color:var(--text-secondary)">Birakilma</th>'}
        </tr>`;
    const body = rows.map(h => `
        <tr>
            <td style="padding:10px;border-bottom:1px solid var(--border)">#${_lhEsc(h.id)}</td>
            <td style="padding:10px;border-bottom:1px solid var(--border)"><code>${_lhEsc(h.path_pattern)}</code></td>
            <td style="padding:10px;border-bottom:1px solid var(--border)">${_lhEsc(h.reason)}</td>
            <td style="padding:10px;border-bottom:1px solid var(--border)">${_lhEsc(h.case_reference || '-')}</td>
            <td style="padding:10px;border-bottom:1px solid var(--border)">${_lhEsc(h.created_by)}</td>
            <td style="padding:10px;border-bottom:1px solid var(--border)">${_lhEsc(h.created_at)}</td>
            ${showRelease
                ? `<td style="padding:10px;border-bottom:1px solid var(--border)"><button class="btn btn-sm btn-outline" onclick="releaseLegalHold(${h.id})">Birak</button></td>`
                : `<td style="padding:10px;border-bottom:1px solid var(--border)">${_lhEsc(h.released_at || '-')}</td>`}
        </tr>`).join('');
    container.innerHTML = `<table style="width:100%;border-collapse:collapse"><thead>${head}</thead><tbody>${body}</tbody></table>`;
}

async function loadLegalHolds() {
    const container = document.getElementById('lh-active-container');
    if (!container) return;
    container.innerHTML = '<div style="padding:14px;color:var(--text-muted)">Yukleniyor...</div>';
    try {
        const r = await fetch('/api/compliance/legal-holds/active');
        const d = r.ok ? await r.json() : { holds: [] };
        _renderLhTable(container, d.holds || [], { showRelease: true, empty: 'Aktif legal hold yok' });
    } catch (e) {
        console.error('loadLegalHolds error:', e);
        container.innerHTML = '<div style="padding:14px;color:var(--danger)">Hata: ' + _lhEsc(e.message || e) + '</div>';
    }
}

async function loadLegalHoldHistory() {
    const container = document.getElementById('lh-history-container');
    if (!container) return;
    container.innerHTML = '<div style="padding:14px;color:var(--text-muted)">Yukleniyor...</div>';
    try {
        const r = await fetch('/api/compliance/legal-holds/history?page=1&page_size=100');
        const d = r.ok ? await r.json() : { items: [] };
        const items = d.items || d.holds || [];
        _renderLhTable(container, items, { showRelease: false, empty: 'Gecmis kaydi yok' });
    } catch (e) {
        console.error('loadLegalHoldHistory error:', e);
        container.innerHTML = '<div style="padding:14px;color:var(--danger)">Hata: ' + _lhEsc(e.message || e) + '</div>';
    }
}

// Tab switcher between "Aktif" and "Gecmis" panes on the Legal Holds page.
// Toggles display + active styling, and lazy-loads history data on first
// switch into the Gecmis tab.
function lhSwitchTab(tabName) {
    const tabs = { active: 'lh-tab-active', history: 'lh-tab-history' };
    const panes = { active: 'lh-active-pane', history: 'lh-history-pane' };
    Object.keys(tabs).forEach(k => {
        const tabEl = document.getElementById(tabs[k]);
        const paneEl = document.getElementById(panes[k]);
        const isActive = (k === tabName);
        if (paneEl) paneEl.style.display = isActive ? '' : 'none';
        if (tabEl) {
            tabEl.style.borderBottom = '2px solid ' + (isActive ? 'var(--accent)' : 'transparent');
            tabEl.style.color = isActive ? 'var(--accent)' : 'var(--text-muted)';
            tabEl.style.fontWeight = isActive ? '600' : '';
        }
    });
    if (tabName === 'history') {
        loadLegalHoldHistory();
    } else if (tabName === 'active') {
        loadLegalHolds();
    }
}

// Path-check: small input + "Path Check" button on Legal Holds page.
// Calls GET /api/compliance/legal-holds/check?path=X and renders a green
// "Hold altinda" or red "Hold yok" badge inline.
async function lhPathCheck() {
    const input = document.getElementById('lh-check-path');
    const result = document.getElementById('lh-check-result');
    const path = (input && input.value || '').trim();
    if (!path) { notify('Once kontrol edilecek yolu girin', 'warning'); return; }
    if (result) {
        result.style.display = 'inline-block';
        result.style.background = 'var(--bg-secondary)';
        result.style.color = 'var(--text-muted)';
        result.textContent = 'Kontrol ediliyor...';
    }
    try {
        const r = await fetch('/api/compliance/legal-holds/check?path=' + encodeURIComponent(path));
        if (!r.ok) throw new Error('HTTP ' + r.status);
        const d = await r.json();
        if (!result) return;
        if (d.is_held) {
            result.style.background = 'rgba(34,197,94,0.15)';
            result.style.color = 'var(--success,#22c55e)';
            const hold = d.hold || {};
            const ref = hold.case_reference ? ' (' + _lhEsc(hold.case_reference) + ')' : '';
            result.innerHTML = '✓ Hold altinda' + ref;
        } else {
            result.style.background = 'rgba(220,38,38,0.15)';
            result.style.color = 'var(--danger,#dc2626)';
            result.textContent = '✗ Hold yok';
        }
    } catch (e) {
        if (result) {
            result.style.background = 'rgba(220,38,38,0.15)';
            result.style.color = 'var(--danger,#dc2626)';
            result.textContent = 'Hata: ' + (e.message || e);
        }
    }
}

// Open the "+ Yeni Hold" modal — clears all fields except created_by which
// defaults to "dashboard" so the audit log records something readable.
function openLegalHoldModal() {
    const set = (id, val) => { const el = document.getElementById(id); if (el) el.value = val; };
    set('lh-form-pattern', '');
    set('lh-form-reason', '');
    set('lh-form-case-ref', '');
    set('lh-form-created-by', 'dashboard');
    openModal('modal-legal-hold');
}

async function submitLegalHoldForm(btn) {
    const pattern = (document.getElementById('lh-form-pattern')?.value || '').trim();
    const reason = (document.getElementById('lh-form-reason')?.value || '').trim();
    const caseRef = (document.getElementById('lh-form-case-ref')?.value || '').trim();
    const createdBy = (document.getElementById('lh-form-created-by')?.value || '').trim() || 'dashboard';
    if (!pattern || !reason) {
        notify('Pattern ve sebep zorunlu', 'warning');
        return;
    }
    await withButtonLoading(btn, async (signal) => {
        const r = await fetch('/api/compliance/legal-holds', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ pattern, reason, case_ref: caseRef || null, created_by: createdBy }),
            signal,
        });
        if (!r.ok) {
            const err = await r.json().catch(() => ({}));
            throw new Error(err.detail || ('HTTP ' + r.status));
        }
        const d = await r.json().catch(() => ({}));
        notify('Legal hold olusturuldu: #' + (d.id ?? '?'), 'success');
        closeModal('modal-legal-hold');
        loadLegalHolds();
    });
}

async function releaseLegalHold(holdId) {
    if (!confirm('Bu hold birakilsin mi? Eslesen yollar yeniden retention/arsiv kapsamina girer.')) return;
    try {
        const r = await fetch('/api/compliance/legal-holds/' + holdId + '/release', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ released_by: 'dashboard' }),
        });
        const d = await r.json().catch(() => ({}));
        if (d.ok) {
            notify('Hold birakildi', 'success');
        } else {
            notify('Birakilamadi: ' + (d.reason || 'bilinmeyen'), 'warning');
        }
        loadLegalHolds();
    } catch (e) {
        notify('Hata: ' + (e.message || e), 'error');
    }
}

// ═══════════════════════════════════════════════════
// STANDARDS — W3C PROV / DCAT v3 (issue #145)
// ═══════════════════════════════════════════════════
// Both endpoints return JSON-LD documents. We download them as files
// so customers can ingest them into Apache Atlas / DataHub / Collibra
// directly — no XLSX, JSON-LD is the on-the-wire format by spec.
function _stdSetStatus(elId, msg, kind) {
    const el = document.getElementById(elId);
    if (!el) return;
    const color = kind === 'error' ? 'var(--danger,#dc2626)' :
                  kind === 'success' ? 'var(--success,#10b981)' :
                  'var(--text-muted)';
    el.style.color = color;
    el.textContent = msg || '';
}

function _stdDownloadBlob(blob, filename) {
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    setTimeout(() => URL.revokeObjectURL(url), 1000);
}

async function standardsDownloadLineage() {
    const path = (document.getElementById('std-lineage-path')?.value || '').trim();
    if (!path) {
        notify('Dosya yolu zorunlu', 'warning');
        return;
    }
    _stdSetStatus('std-lineage-status', 'Lineage cekiliyor...', 'info');
    try {
        const r = await fetch('/api/compliance/lineage/file.jsonld?path=' + encodeURIComponent(path));
        if (!r.ok) {
            const err = await r.json().catch(() => ({}));
            throw new Error(err.detail || ('HTTP ' + r.status));
        }
        const blob = await r.blob();
        const safe = path.replace(/[^A-Za-z0-9]/g, '_').slice(0, 80);
        _stdDownloadBlob(blob, 'lineage_' + safe + '.jsonld');
        _stdSetStatus('std-lineage-status', 'Lineage indirildi.', 'success');
    } catch (e) {
        _stdSetStatus('std-lineage-status', 'Hata: ' + e.message, 'error');
    }
}

async function standardsDownloadCatalog() {
    _stdSetStatus('std-catalog-status', 'Katalog cekiliyor...', 'info');
    try {
        const r = await fetch('/api/compliance/dcat/catalog.jsonld');
        if (!r.ok) {
            const err = await r.json().catch(() => ({}));
            throw new Error(err.detail || ('HTTP ' + r.status));
        }
        const blob = await r.blob();
        const ts = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
        _stdDownloadBlob(blob, 'dcat_catalog_' + ts + '.jsonld');
        _stdSetStatus('std-catalog-status', 'Katalog indirildi.', 'success');
    } catch (e) {
        _stdSetStatus('std-catalog-status', 'Hata: ' + e.message, 'error');
    }
}

function loadStandards() {
    // Page is static — no auto-refresh; just clear the status fields.
    _stdSetStatus('std-lineage-status', '', 'info');
    _stdSetStatus('std-catalog-status', '', 'info');
}

// ═══════════════════════════════════════════════════
// MISSING-PAGE STUBS — issue #194 Update #4 (2026-04-29)
// ═══════════════════════════════════════════════════
// The sidebar dispatches via `loaders[pageName]()` (line ~2664). The
// loaders dict referenced `loadPii` and `loadRetention` for sidebar
// items wired up earlier, but no implementations ever landed in this
// file — clicking those menus fired:
//   Uncaught ReferenceError: loadPii is not defined
//
// That single ReferenceError was caught by `showPage` but it bubbled
// to the console and kept the customer's "menüler arası gezinti yok"
// experience even after PR #193 fixed the prior parse error.
//
// These stubs render a "Yakında" placeholder so the menus don't crash;
// real implementations belong in the next-cycle backlog (frontend
// integration wave #81 follow-up).
function loadPii() {
    const el = document.getElementById('page-pii');
    if (!el) return;
    el.innerHTML = `
        <div style="padding:32px;text-align:center;color:var(--text-muted)">
            <div style="font-size:48px;margin-bottom:12px">🔒</div>
            <h2 style="margin:0 0 8px 0;color:var(--text-primary)">PII Bulgular</h2>
            <p style="margin:0;font-size:13px">Bu sayfa yakında etkinleştirilecek (issue #81 follow-up).
            Mevcut PII verisi <a href="/api/compliance/pii/findings" target="_blank">/api/compliance/pii/findings</a> endpoint'inden alınabilir.</p>
        </div>
    `;
}

function loadRetention() {
    const el = document.getElementById('page-retention');
    if (!el) return;
    el.innerHTML = `
        <div style="padding:32px;text-align:center;color:var(--text-muted)">
            <div style="font-size:48px;margin-bottom:12px">📅</div>
            <h2 style="margin:0 0 8px 0;color:var(--text-primary)">Saklama Politikaları</h2>
            <p style="margin:0;font-size:13px">Bu sayfa yakında etkinleştirilecek (issue #81 follow-up).
            Saklama hesabı için <a href="javascript:loadRetentionAttestation()">attestation endpoint</a>'i
            kullanılabilir; tam policy CRUD UI yakında gelecek.</p>
        </div>
    `;
}

// ═══════════════════════════════════════════════════
// PII SUBJECT EXPORT (GDPR Article 17 / 30)
// ═══════════════════════════════════════════════════
function openPiiSubjectModal() {
    const inp = document.getElementById('pii-subject-term');
    if (inp) inp.value = '';
    openModal('modal-pii-subject');
    setTimeout(() => { try { inp && inp.focus(); } catch(_){} }, 50);
}

async function submitPiiSubjectForm(btn) {
    const term = (document.getElementById('pii-subject-term')?.value || '').trim();
    if (!term) { notify('Arama terimi zorunlu', 'warning'); return; }
    await withButtonLoading(btn, async (signal) => {
        const url = '/api/compliance/pii/subject?format=csv&term=' + encodeURIComponent(term);
        await fetchAndDownload(url, signal, 'pii_subject_' + term.replace(/[^A-Za-z0-9]/g, '_').slice(0, 40) + '.csv');
        notify('Subject export indirildi', 'success');
        closeModal('modal-pii-subject');
    });
}

// ═══════════════════════════════════════════════════
// RETENTION POLICIES — modals (issue #82, Bug 4)
// ═══════════════════════════════════════════════════
function openRetentionPolicyModal() {
    const set = (id, val) => { const el = document.getElementById(id); if (el) el.value = val; };
    set('ret-form-name', '');
    set('ret-form-pattern', '');
    set('ret-form-retain-days', '');
    set('ret-form-action', 'archive');
    openModal('modal-retention-policy');
}

async function submitRetentionPolicyForm(btn) {
    const name = (document.getElementById('ret-form-name')?.value || '').trim();
    const pattern = (document.getElementById('ret-form-pattern')?.value || '').trim();
    const retainDaysRaw = (document.getElementById('ret-form-retain-days')?.value || '').trim();
    const action = (document.getElementById('ret-form-action')?.value || '').trim();
    if (!name || !retainDaysRaw || !action) {
        notify('Ad, gun ve eylem zorunlu', 'warning');
        return;
    }
    const retainDays = parseInt(retainDaysRaw, 10);
    if (!Number.isFinite(retainDays) || retainDays < 1) {
        notify('Saklama suresi pozitif tam sayi olmali', 'warning');
        return;
    }
    await withButtonLoading(btn, async (signal) => {
        const r = await fetch('/api/compliance/retention/policies', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ name, pattern_match: pattern || '', retain_days: retainDays, action }),
            signal,
        });
        if (!r.ok) {
            const err = await r.json().catch(() => ({}));
            throw new Error(err.detail || ('HTTP ' + r.status));
        }
        notify('Retention policy olusturuldu: ' + name, 'success');
        closeModal('modal-retention-policy');
        if (typeof loadRetention === 'function') loadRetention();
    });
}

function openRetentionAttestationModal() {
    const sel = document.getElementById('ret-attest-since-days');
    if (sel) sel.value = '30';
    const body = document.getElementById('ret-attest-body');
    if (body) body.innerHTML = '<div style="color:var(--text-muted)">Yukleniyor...</div>';
    const summary = document.getElementById('ret-attest-summary');
    if (summary) summary.textContent = '';
    openModal('modal-retention-attestation');
    loadRetentionAttestation();
}

async function loadRetentionAttestation() {
    const since = (document.getElementById('ret-attest-since-days')?.value || '30');
    const body = document.getElementById('ret-attest-body');
    const summary = document.getElementById('ret-attest-summary');
    if (body) body.innerHTML = '<div style="color:var(--text-muted)">Yukleniyor...</div>';
    try {
        const r = await fetch('/api/compliance/retention/attestation?since_days=' + encodeURIComponent(since));
        if (!r.ok) throw new Error('HTTP ' + r.status);
        const d = await r.json();
        const totals = d.totals || {};
        const archive = totals.archive || 0;
        const del = totals.delete || 0;
        if (summary) summary.textContent = 'Arsiv: ' + archive + ' · Silme: ' + del + ' · Olusturuldu: ' + (d.generated_at || '-');
        const byPolicy = d.by_policy || [];
        const events = d.events || [];
        const policyRows = byPolicy.length
            ? byPolicy.map(p => `
                <tr>
                    <td style="padding:6px 8px;border-bottom:1px solid var(--border)">${_lhEsc(p.policy)}</td>
                    <td style="padding:6px 8px;border-bottom:1px solid var(--border)">${_lhEsc(p.action)}</td>
                    <td style="padding:6px 8px;border-bottom:1px solid var(--border);text-align:right">${_lhEsc(p.count)}</td>
                    <td style="padding:6px 8px;border-bottom:1px solid var(--border)">${_lhEsc(p.first_event || '-')}</td>
                    <td style="padding:6px 8px;border-bottom:1px solid var(--border)">${_lhEsc(p.last_event || '-')}</td>
                </tr>`).join('')
            : '<tr><td colspan="5" style="padding:10px;color:var(--text-muted)">Bu pencerede politika eylemi yok</td></tr>';
        const eventRows = events.length
            ? events.slice(0, 200).map(ev => `
                <tr>
                    <td style="padding:6px 8px;border-bottom:1px solid var(--border)">#${_lhEsc(ev.id)}</td>
                    <td style="padding:6px 8px;border-bottom:1px solid var(--border)">${_lhEsc(ev.event_time)}</td>
                    <td style="padding:6px 8px;border-bottom:1px solid var(--border)">${_lhEsc(ev.event_type)}</td>
                    <td style="padding:6px 8px;border-bottom:1px solid var(--border);font-family:Consolas,monospace;font-size:11px">${_lhEsc(ev.file_path)}</td>
                </tr>`).join('')
            : '<tr><td colspan="4" style="padding:10px;color:var(--text-muted)">Olay yok</td></tr>';
        if (body) body.innerHTML = `
            <div style="font-size:11px;color:var(--text-secondary);text-transform:uppercase;letter-spacing:.4px;margin-bottom:6px">Politika Bazli</div>
            <table style="width:100%;border-collapse:collapse;margin-bottom:14px">
                <thead><tr>
                    <th style="text-align:left;padding:6px 8px;border-bottom:1px solid var(--border);font-size:11px;color:var(--text-muted)">Politika</th>
                    <th style="text-align:left;padding:6px 8px;border-bottom:1px solid var(--border);font-size:11px;color:var(--text-muted)">Eylem</th>
                    <th style="text-align:right;padding:6px 8px;border-bottom:1px solid var(--border);font-size:11px;color:var(--text-muted)">Adet</th>
                    <th style="text-align:left;padding:6px 8px;border-bottom:1px solid var(--border);font-size:11px;color:var(--text-muted)">Ilk olay</th>
                    <th style="text-align:left;padding:6px 8px;border-bottom:1px solid var(--border);font-size:11px;color:var(--text-muted)">Son olay</th>
                </tr></thead>
                <tbody>${policyRows}</tbody>
            </table>
            <div style="font-size:11px;color:var(--text-secondary);text-transform:uppercase;letter-spacing:.4px;margin-bottom:6px">Olaylar (ilk 200)</div>
            <table style="width:100%;border-collapse:collapse">
                <thead><tr>
                    <th style="text-align:left;padding:6px 8px;border-bottom:1px solid var(--border);font-size:11px;color:var(--text-muted)">#</th>
                    <th style="text-align:left;padding:6px 8px;border-bottom:1px solid var(--border);font-size:11px;color:var(--text-muted)">Zaman</th>
                    <th style="text-align:left;padding:6px 8px;border-bottom:1px solid var(--border);font-size:11px;color:var(--text-muted)">Tur</th>
                    <th style="text-align:left;padding:6px 8px;border-bottom:1px solid var(--border);font-size:11px;color:var(--text-muted)">Dosya</th>
                </tr></thead>
                <tbody>${eventRows}</tbody>
            </table>`;
    } catch (e) {
        if (body) body.innerHTML = '<div style="color:var(--danger)">Hata: ' + _lhEsc(e.message || e) + '</div>';
    }
}

async function exportRetentionAttestationXlsx(btn) {
    const since = (document.getElementById('ret-attest-since-days')?.value || '30');
    await withButtonLoading(btn, async (signal) => {
        const url = '/api/compliance/retention/attestation/export.xlsx?since_days=' + encodeURIComponent(since);
        await fetchAndDownload(url, signal, 'retention_attestation.xlsx');
    });
}

// ═══════════════════════════════════════════════════
// SYSLOG / SIEM (Issue #81 — Integrations page)
// ═══════════════════════════════════════════════════
function _esc(s) { return String(s == null ? '' : s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }

async function loadSyslog() {
    const banner = document.getElementById('syslog-flag-banner');
    try {
        const r = await fetch('/api/integrations/syslog/status');
        const d = r.ok ? await r.json() : {};
        const enabled = !!d.available;
        const configured = !!d.configured;
        if (banner) banner.style.display = (enabled || configured) ? 'none' : 'block';
        const setText = (id, val) => { const el = document.getElementById(id); if (el) el.textContent = val == null || val === '' ? '-' : val; };
        setText('syslog-status-val', enabled ? 'Aktif' : (configured ? 'Yapilandirildi' : 'Kapali'));
        setText('syslog-status-sub', enabled ? `${d.transport || '-'} -> ${d.host || '-'}:${d.port || '-'}` : 'Yapilandirma yapilmamis');
        const qd = (d.queue_depth != null) ? d.queue_depth : '-';
        const qm = (d.queue_max != null) ? d.queue_max : '-';
        setText('syslog-queue-val', String(qd));
        setText('syslog-queue-sub', `${qd} / ${qm}`);
        setText('syslog-error-val', d.last_error || 'Yok');
        setText('syslog-error-sub', `Son emit: ${d.last_emit_at || '-'}`);
        setText('sl-enabled', enabled ? 'Evet' : 'Hayir');
        setText('sl-host', d.host);
        setText('sl-port', d.port);
        setText('sl-transport', d.transport);
        setText('sl-format', d.format);
        setText('sl-sent', formatNum(d.sent_count || 0));
        setText('sl-dropped', formatNum(d.dropped_count || 0));
        setText('sl-last-emit', d.last_emit_at || '-');
    } catch (e) {
        console.error('loadSyslog error:', e);
        if (banner) { banner.style.display = 'block'; banner.innerHTML = '<strong>Syslog durumu okunamadi.</strong> ' + _esc(e.message); }
    }
}

async function sendSyslogTest(btn) {
    await withButtonLoading(btn, async (signal) => {
        const r = await fetch('/api/integrations/syslog/test', { method: 'POST', signal });
        const d = await r.json().catch(() => ({}));
        if (d.sent) {
            notify('Test event gonderildi', 'success');
            setTimeout(loadSyslog, 600);  // refresh queue/last_emit
        } else {
            notify('Test event gonderilemedi: ' + (d.error || 'bilinmeyen hata'), 'error');
        }
    });
}

function showSyslogConfig() { openModal('modal-syslog-config'); }

// ═══════════════════════════════════════════════════
// MCP SERVER (Issue #81 — Integrations page)
// ═══════════════════════════════════════════════════
async function loadMcp() {
    const banner = document.getElementById('mcp-flag-banner');
    const tbody = document.getElementById('mcp-tools-tbody');
    try {
        const r = await fetch('/api/system/mcp/info');
        const d = r.ok ? await r.json() : { tools: [], configured: false };
        if (banner) banner.style.display = d.configured ? 'none' : 'block';
        const cmdEl = document.getElementById('mcp-install-cmd');
        if (cmdEl) cmdEl.textContent = d.install_command || '-';
        const trEl = document.getElementById('mcp-transports');
        if (trEl) trEl.textContent = (d.transports || ['stdio']).join(', ');
        const badge = document.getElementById('mcp-tools-count-badge');
        if (badge) badge.textContent = `${d.tools_count || 0} arac`;
        const tools = d.tools || [];
        if (!tools.length) {
            tbody.innerHTML = '<tr><td colspan="3" style="padding:20px;text-align:center;color:var(--text-muted)">Arac listesi alinamadi.</td></tr>';
            return;
        }
        tbody.innerHTML = tools.map(t => `
            <tr>
                <td style="padding:10px;border-bottom:1px solid var(--border)"><code style="color:var(--accent)">${_esc(t.name)}</code></td>
                <td style="padding:10px;border-bottom:1px solid var(--border);color:var(--text-secondary);font-size:13px">${_esc(t.description || '')}</td>
                <td style="padding:10px;border-bottom:1px solid var(--border)">${t.is_write
                    ? '<span style="display:inline-block;padding:2px 8px;background:var(--danger);color:#fff;border-radius:4px;font-size:11px">write</span>'
                    : '<span style="display:inline-block;padding:2px 8px;background:var(--bg-secondary);color:var(--text-muted);border-radius:4px;font-size:11px">read</span>'}</td>
            </tr>`).join('');
    } catch (e) {
        console.error('loadMcp error:', e);
        if (tbody) tbody.innerHTML = `<tr><td colspan="3" style="padding:20px;text-align:center;color:var(--danger)">Hata: ${_esc(e.message)}</td></tr>`;
    }
}

function copyMcpInstallCmd() {
    const cmd = document.getElementById('mcp-install-cmd')?.textContent || '';
    if (!cmd || cmd === '-') { notify('Kopyalanacak komut yok', 'warning'); return; }
    if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(cmd).then(
            () => notify('Komut panoya kopyalandi', 'success'),
            () => notify('Pano erisimi reddedildi', 'error')
        );
    } else {
        // Fallback
        const ta = document.createElement('textarea');
        ta.value = cmd;
        document.body.appendChild(ta);
        ta.select();
        try { document.execCommand('copy'); notify('Komut panoya kopyalandi', 'success'); }
        catch { notify('Kopyalama desteklenmiyor', 'error'); }
        ta.remove();
    }
}

// ═══════════════════════════════════════════════════
// SYSTEM BACKUPS (Issue #81 — System page)
// ═══════════════════════════════════════════════════
let _backupsCache = [];

async function loadBackups() {
    const banner = document.getElementById('backups-flag-banner');
    const tbody = document.getElementById('backups-tbody');
    const meta = document.getElementById('backups-meta');
    try {
        const r = await fetch('/api/system/backups');
        const d = r.ok ? await r.json() : { rows: [], enabled: false };
        if (banner) banner.style.display = d.enabled ? 'none' : 'block';
        if (meta) {
            meta.style.display = 'block';
            const dirEl = document.getElementById('backups-dir');
            if (dirEl) dirEl.textContent = d.backup_dir || '-';
            const klEl = document.getElementById('backups-keep-last');
            if (klEl) klEl.textContent = d.keep_last_n != null ? d.keep_last_n : '-';
            const kwEl = document.getElementById('backups-keep-weekly');
            if (kwEl) kwEl.textContent = d.keep_weekly != null ? d.keep_weekly : '-';
        }
        const rows = (d.rows || []).slice().sort((a, b) => (b.id || '').localeCompare(a.id || ''));
        _backupsCache = rows;
        if (!rows.length) {
            tbody.innerHTML = '<tr><td colspan="6" style="padding:20px;text-align:center;color:var(--text-muted)">Henuz snapshot yok</td></tr>';
            return;
        }
        tbody.innerHTML = rows.map(s => {
            const sha = (s.sha256 || '').slice(0, 12);
            return `
            <tr>
                <td style="padding:10px;border-bottom:1px solid var(--border)"><code>${_esc(s.id)}</code></td>
                <td style="padding:10px;border-bottom:1px solid var(--border)">${_esc(s.created_at || '-')}</td>
                <td style="padding:10px;border-bottom:1px solid var(--border)">${_esc(s.reason || '-')}</td>
                <td style="padding:10px;border-bottom:1px solid var(--border);text-align:right">${formatSize(s.size_bytes || 0)}</td>
                <td style="padding:10px;border-bottom:1px solid var(--border)"><code style="color:var(--text-muted);font-size:11px" title="${_esc(s.sha256 || '')}">${_esc(sha)}…</code></td>
                <td style="padding:10px;border-bottom:1px solid var(--border);text-align:right">
                    <button class="btn btn-outline" style="font-size:11px;padding:4px 10px;border-color:var(--danger);color:var(--danger)" onclick="openRestoreModal('${_esc(s.id)}')">Restore</button>
                </td>
            </tr>`;
        }).join('');
    } catch (e) {
        console.error('loadBackups error:', e);
        if (tbody) tbody.innerHTML = `<tr><td colspan="6" style="padding:20px;text-align:center;color:var(--danger)">Hata: ${_esc(e.message)}</td></tr>`;
    }
}

function openSnapshotModal() {
    const reason = document.getElementById('snap-reason');
    if (reason) reason.value = 'manual';
    openModal('modal-snapshot');
}

async function createSnapshot(btn) {
    const reason = (document.getElementById('snap-reason')?.value || 'manual').trim() || 'manual';
    await withButtonLoading(btn, async (signal) => {
        const r = await fetch('/api/system/backups/snapshot', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ reason, confirm: true }),
            signal,
        });
        const d = await r.json().catch(() => ({}));
        if (!r.ok || !d.ok) {
            notify('Snapshot olusturulamadi: ' + (d.detail || d.error || `HTTP ${r.status}`), 'error');
            return;
        }
        notify(`Snapshot olusturuldu: ${d.id} (${formatSize(d.size_bytes || 0)})`, 'success');
        closeModal('modal-snapshot');
        loadBackups();
    });
}

function openRestoreModal(snapId) {
    const tgt = document.getElementById('restore-target-id');
    const txt = document.getElementById('restore-confirm-text');
    const goBtn = document.getElementById('restore-go-btn');
    if (tgt) tgt.value = snapId;
    if (txt) txt.value = '';
    if (goBtn) goBtn.disabled = true;
    openModal('modal-restore');
}

function onRestoreConfirmInput(input) {
    const goBtn = document.getElementById('restore-go-btn');
    if (goBtn) goBtn.disabled = (input.value !== 'RESTORE');
}

async function executeRestore(btn) {
    const id = document.getElementById('restore-target-id')?.value || '';
    const txt = document.getElementById('restore-confirm-text')?.value || '';
    if (!id) { notify('Snapshot id bos', 'warning'); return; }
    if (txt !== 'RESTORE') { notify('Onay metni hatali', 'warning'); return; }
    await withButtonLoading(btn, async (signal) => {
        const r = await fetch(`/api/system/backups/restore/${encodeURIComponent(id)}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ confirm: true }),
            signal,
        });
        const d = await r.json().catch(() => ({}));
        if (!r.ok) {
            notify('Restore basarisiz: ' + (d.detail || `HTTP ${r.status}`), 'error');
            return;
        }
        notify(`Snapshot geri yuklendi: ${d.restored || id}`, 'success');
        closeModal('modal-restore');
        loadBackups();
    }, 120000);
}

async function exportBackupsXlsx(ev) {
    const btn = ev && ev.currentTarget ? ev.currentTarget : null;
    if (!btn) return;
    await withButtonLoading(btn, async (signal) => {
        await fetchAndDownload('/api/system/backups/export', signal, 'backups.xlsx');
    });
}

// ═══════════════════════════════════════════════════
// APPROVALS (issue #112)
// ═══════════════════════════════════════════════════

let _approvalsConfig = { enabled: false, require_for: [], expiry_hours: 24, identity_source: 'client_supplied' };
let _approvalsTab = 'pending';

function _approvalsCurrentUser() {
    const el = document.getElementById('approvals-current-user');
    return (el && el.value || '').trim();
}

function _formatExpiry(expiresAt) {
    if (!expiresAt) return '-';
    const exp = new Date(expiresAt.replace(' ', 'T'));
    if (isNaN(exp.getTime())) return _esc(expiresAt);
    const ms = exp.getTime() - Date.now();
    if (ms <= 0) return '<span style="color:var(--danger)">Sona erdi</span>';
    const h = Math.floor(ms / 3600000);
    const m = Math.floor((ms % 3600000) / 60000);
    return `<span title="${_esc(expiresAt)}">${h}s ${m}d</span>`;
}

async function loadApprovalsConfig() {
    try {
        const r = await fetch('/api/approvals/config');
        if (!r.ok) return;
        const d = await r.json();
        _approvalsConfig = d || _approvalsConfig;
        const banner = document.getElementById('approvals-flag-banner');
        if (banner) banner.style.display = _approvalsConfig.enabled ? 'none' : 'block';
        const meta = document.getElementById('approvals-meta');
        if (meta) {
            meta.style.display = 'block';
            const rf = (_approvalsConfig.require_for || []).join(', ') || '(yok)';
            const rfEl = document.getElementById('approvals-require-for');
            if (rfEl) rfEl.textContent = rf;
            const exEl = document.getElementById('approvals-expiry-hours');
            if (exEl) exEl.textContent = _approvalsConfig.expiry_hours;
            const idEl = document.getElementById('approvals-identity-source');
            if (idEl) idEl.textContent = _approvalsConfig.identity_source;
        }
    } catch (e) {
        console.error('loadApprovalsConfig error:', e);
    }
}

async function loadApprovalsAll() {
    await loadApprovalsConfig();
    await loadApprovalsPending();
    await loadApprovalsHistory();
}

function switchApprovalsTab(tab) {
    _approvalsTab = tab;
    document.querySelectorAll('.approvals-tab').forEach(b => {
        if (b.getAttribute('data-tab') === tab) b.classList.add('active');
        else b.classList.remove('active');
    });
    const pendingWrap = document.getElementById('approvals-pending-wrap');
    const historyWrap = document.getElementById('approvals-history-wrap');
    if (pendingWrap) pendingWrap.style.display = (tab === 'pending') ? 'block' : 'none';
    if (historyWrap) historyWrap.style.display = (tab === 'history') ? 'block' : 'none';
}

async function loadApprovalsPending() {
    const tbody = document.getElementById('approvals-pending-tbody');
    if (!tbody) return;
    try {
        const r = await fetch('/api/approvals/pending');
        const d = r.ok ? await r.json() : { rows: [] };
        const rows = d.rows || [];
        if (!rows.length) {
            tbody.innerHTML = '<tr><td colspan="7" style="padding:20px;text-align:center;color:var(--text-muted)">Bekleyen onay yok</td></tr>';
            return;
        }
        const me = _approvalsCurrentUser();
        tbody.innerHTML = rows.map(row => {
            const isSelf = me && (me === row.requested_by);
            const buttons = `
                <button class="btn btn-primary" style="font-size:11px;padding:4px 10px;margin-right:4px"
                        ${isSelf ? 'disabled title="Kendi talebinizi onaylayamazsiniz"' : ''}
                        onclick="approveApproval(${row.id}, this)">Onayla</button>
                <button class="btn btn-outline" style="font-size:11px;padding:4px 10px;border-color:var(--danger);color:var(--danger);margin-right:4px"
                        onclick="openRejectModal(${row.id})">Reddet</button>
                <button class="btn btn-outline" style="font-size:11px;padding:4px 10px"
                        onclick="executeApproval(${row.id}, this)">Calistir</button>`;
            return `
                <tr>
                    <td style="padding:10px;border-bottom:1px solid var(--border)"><code>${row.id}</code></td>
                    <td style="padding:10px;border-bottom:1px solid var(--border)"><code>${_esc(row.operation_type)}</code></td>
                    <td style="padding:10px;border-bottom:1px solid var(--border);font-size:11px"><code>${_esc(JSON.stringify(row.payload || {}))}</code></td>
                    <td style="padding:10px;border-bottom:1px solid var(--border)">${_esc(row.requested_by)}</td>
                    <td style="padding:10px;border-bottom:1px solid var(--border);font-size:11px">${_esc(row.requested_at)}</td>
                    <td style="padding:10px;border-bottom:1px solid var(--border)">${_formatExpiry(row.expires_at)}</td>
                    <td style="padding:10px;border-bottom:1px solid var(--border);text-align:right">${buttons}</td>
                </tr>`;
        }).join('');
    } catch (e) {
        console.error('loadApprovalsPending error:', e);
        tbody.innerHTML = `<tr><td colspan="7" style="padding:20px;text-align:center;color:var(--danger)">Hata: ${_esc(e.message)}</td></tr>`;
    }
}

async function loadApprovalsHistory() {
    const tbody = document.getElementById('approvals-history-tbody');
    if (!tbody) return;
    try {
        const r = await fetch('/api/approvals/history?limit=100');
        const d = r.ok ? await r.json() : { rows: [] };
        const rows = d.rows || [];
        if (!rows.length) {
            tbody.innerHTML = '<tr><td colspan="7" style="padding:20px;text-align:center;color:var(--text-muted)">Kayit yok</td></tr>';
            return;
        }
        tbody.innerHTML = rows.map(row => {
            const statusColor = {
                pending: 'var(--warning)', approved: 'var(--accent)',
                rejected: 'var(--danger)', expired: 'var(--text-muted)',
                executed: 'var(--success)',
            }[row.status] || 'var(--text-muted)';
            return `
                <tr>
                    <td style="padding:10px;border-bottom:1px solid var(--border)"><code>${row.id}</code></td>
                    <td style="padding:10px;border-bottom:1px solid var(--border)"><code>${_esc(row.operation_type)}</code></td>
                    <td style="padding:10px;border-bottom:1px solid var(--border);color:${statusColor};font-weight:600">${_esc(row.status)}</td>
                    <td style="padding:10px;border-bottom:1px solid var(--border)">${_esc(row.requested_by)}</td>
                    <td style="padding:10px;border-bottom:1px solid var(--border)">${_esc(row.approved_by || '-')}</td>
                    <td style="padding:10px;border-bottom:1px solid var(--border)">${_esc(row.rejected_by || '-')}</td>
                    <td style="padding:10px;border-bottom:1px solid var(--border);font-size:11px">${_esc(row.requested_at)}</td>
                </tr>`;
        }).join('');
    } catch (e) {
        console.error('loadApprovalsHistory error:', e);
        tbody.innerHTML = `<tr><td colspan="7" style="padding:20px;text-align:center;color:var(--danger)">Hata: ${_esc(e.message)}</td></tr>`;
    }
}

async function approveApproval(id, btn) {
    const me = _approvalsCurrentUser();
    if (!me) {
        if (!confirm('Kullanici adi girilmedi. Server tarafi kimlik cozumlemesi kullanilacak. Devam edilsin mi?')) return;
    }
    await withButtonLoading(btn, async (signal) => {
        const r = await fetch(`/api/approvals/${id}/approve`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(me ? { approved_by: me } : {}),
            signal,
        });
        const d = await r.json().catch(() => ({}));
        if (!r.ok) {
            notify('Onaylanamadi: ' + (d.detail || `HTTP ${r.status}`), 'error');
            return;
        }
        notify('Onaylandi: #' + id, 'success');
        await loadApprovalsAll();
    });
}

function openRejectModal(id) {
    const reason = prompt('Red gerekcesi:', '');
    if (reason === null) return;
    rejectApproval(id, reason);
}

async function rejectApproval(id, reason) {
    const me = _approvalsCurrentUser();
    const body = { reason: reason || '' };
    if (me) body.rejected_by = me;
    try {
        const r = await fetch(`/api/approvals/${id}/reject`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
        });
        const d = await r.json().catch(() => ({}));
        if (!r.ok) {
            notify('Reddedilemedi: ' + (d.detail || `HTTP ${r.status}`), 'error');
            return;
        }
        notify('Reddedildi: #' + id, 'success');
        loadApprovalsAll();
    } catch (e) {
        notify('Reddedilemedi: ' + e.message, 'error');
    }
}

async function executeApproval(id, btn) {
    if (!confirm('Onaylanmis islem calistirilsin mi? Bu islem geri alinamaz.')) return;
    await withButtonLoading(btn, async (signal) => {
        const r = await fetch(`/api/approvals/${id}/execute`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({}),
            signal,
        });
        const d = await r.json().catch(() => ({}));
        if (!r.ok) {
            notify('Calistirma basarisiz: ' + (d.detail || `HTTP ${r.status}`), 'error');
            return;
        }
        notify('Islem calistirildi: #' + id, 'success');
        await loadApprovalsAll();
    });
}

// ═══════════════════════════════════════════════════
// INIT
// ═══════════════════════════════════════════════════
(async function init() {
    // Issue #77 Phase 2 — auto-restore banner. If the dashboard
    // bootstrap salvaged the DB from a snapshot, surface a persistent
    // yellow banner so the operator knows BEFORE they look at any data.
    fetch('/api/system/last-restore')
        .then(r => r.ok ? r.json() : null)
        .then(d => {
            if (!d || !d.restored) return;
            const banner = document.getElementById('auto-restore-banner');
            const text = document.getElementById('auto-restore-banner-text');
            const detail = document.getElementById('auto-restore-banner-detail');
            const closeBtn = document.getElementById('auto-restore-banner-close');
            if (!banner || !text) return;
            const sid = d.snapshot_id || '?';
            const broken = d.broken_path || '?';
            text.textContent =
                'Veritabani otomatik geri yuklendi (snapshot: ' + sid +
                '). Bozuk DB: ' + broken + '. Lutfen kontrol edin.';
            banner.style.display = 'block';
            if (detail) detail.onclick = () => {
                alert(
                    'Otomatik geri yukleme detaylari:\n\n' +
                    'Snapshot ID: ' + sid + '\n' +
                    'Bozuk DB yolu: ' + broken + '\n' +
                    'Zaman: ' + (d.ts || '?') + '\n' +
                    'Audit event ID: ' + (d.audit_event_id || '-') + '\n\n' +
                    'Bozuk DB silinmedi — adli inceleme icin korunuyor.'
                );
            };
            if (closeBtn) closeBtn.onclick = () => {
                banner.style.display = 'none';
            };
        })
        .catch(() => {}); // Endpoint yoksa sessizce atla

    // Sidebar sürümünü dinamik cek (VERSION dosyasindan)
    fetch('/api/system/version')
        .then(r => r.ok ? r.json() : null)
        .then(d => {
            const el = document.getElementById('app-version');
            if (el && d && d.version) el.textContent = 'v' + d.version;
        })
        .catch(() => {
            const el = document.getElementById('app-version');
            if (el) el.textContent = '';
        });

    // GitHub'da yeni release var mi kontrol et - varsa sidebar'da banner goster
    fetch('/api/system/version-check')
        .then(r => r.ok ? r.json() : null)
        .then(d => {
            if (d && d.update_available) {
                const badge = document.getElementById('update-badge');
                const info = document.getElementById('update-badge-info');
                if (badge) badge.style.display = 'block';
                if (info) info.textContent = `v${d.remote} (mevcut: v${d.local})`;
                window._updateInfo = d;
            }
        })
        .catch(() => {}); // Ag yoksa sessizce atla

    // WAL dosyasi buyuduyse sidebar'da uyari banner'i goster
    fetch('/api/system/health')
        .then(r => r.ok ? r.json() : null)
        .then(d => {
            if (d && d.wal_warning) {
                const badge = document.getElementById('wal-warning-badge');
                const info = document.getElementById('wal-warning-info');
                if (badge) {
                    badge.style.display = 'block';
                    // Kritik durumda daha belirgin kirmizi
                    if (d.wal_warning.severity === 'critical') {
                        badge.style.background = '#991b1b';
                    }
                }
                if (info) info.textContent = `WAL: ${d.wal_warning.wal_size_formatted}`;
                window._walWarning = d.wal_warning;
            }
        })
        .catch(() => {});

    // Issue #59 — Legal hold sidebar badge. Polled every 5 minutes
    // (cheap endpoint, just two COUNTs against legal_holds + scanned_files).
    function refreshLegalHoldBadge() {
        fetch('/api/compliance/legal-holds/badge')
            .then(r => r.ok ? r.json() : null)
            .then(d => {
                if (!d) return;
                const badge = document.getElementById('legal-hold-badge');
                const title = document.getElementById('legal-hold-badge-title');
                const info = document.getElementById('legal-hold-badge-info');
                if (!badge) return;
                if ((d.active_count || 0) > 0) {
                    badge.style.display = 'block';
                    if (title) title.textContent = d.active_count + ' aktif legal hold';
                    if (info) info.textContent = (d.held_paths_count || 0) + ' dosya donduruldu';
                } else {
                    badge.style.display = 'none';
                }
            })
            .catch(() => {});
    }
    refreshLegalHoldBadge();
    setInterval(refreshLegalHoldBadge, 5 * 60 * 1000);

    // Dashboard aninda yuklensin - veri arka planda gelsin
    try {
        const initData = await api('/dashboard/init', {silent: true}).catch(() => null);
        if (initData && initData.sources) {
            sources = initData.sources;
            populateAllSourceSelects();
            renderSources();

            // Otomatik kaynak sec (tek kaynak varsa veya ilk kaynagi sec)
            const autoId = initData.auto_select || (sources.length ? sources[0].id : null);
            if (autoId) {
                // Tum source select'lere ayarla
                document.querySelectorAll('select[id$="-source"]').forEach(sel => {
                    if (sel.querySelector(`option[value="${autoId}"]`)) sel.value = autoId;
                });

                // Ozet bilgiyi aninda goster (ağır sorguları beklemeden)
                const summary = initData.summaries?.[autoId];
                if (summary?.has_data) {
                    document.getElementById('ov-files').textContent = formatNum(summary.file_count);
                    document.getElementById('ov-size').textContent = summary.total_size_formatted || formatSize(summary.total_size);
                    document.getElementById('ov-risk-text').textContent = '...';

                    // Yukleniyor gostergesi — loadOverview insightsPromise'i
                    // arka planda baslatacak, hazir olunca icerik gelecek.
                    const recEl = document.getElementById('ov-recommendations');
                    if (recEl) recEl.innerHTML = '<div style="text-align:center;padding:20px;color:var(--text-muted);font-size:12px">AI onerileri arka planda hazirlaniyor — dashboard kullanima hazir</div>';
                }

                // Agir verileri arka planda yukle
                loadOverview();
                loadAnomalies();
                // Issue #177 — fetch trend data for delta lines after initial render
                Promise.all(sources.map(s =>
                    fetch(`/api/trend/${s.id}`, { cache: 'no-store' })
                        .then(r => r.ok ? r.json() : null)
                        .catch(() => null)
                        .then(data => { if (data) _sourceTrends[s.id] = data; })
                )).then(() => renderSources()).catch(() => {});
            }
        } else {
            // Fallback: eski yontem
            await loadSources();
            loadOverview();
            loadAnomalies();
        }
    } catch(e) {
        console.error('Init error:', e);
        await loadSources();
        loadOverview();
        loadAnomalies();
    }

    // Issue #125: start global "su an ne oluyor" polling once. The
    // poller is page-agnostic and lives for the lifetime of the
    // dashboard tab; offline / 5xx responses are swallowed silently.
    try { startOperationsPolling(); } catch (_) { /* polling is non-critical */ }
})();

// ═══════════════════════════════════════════════════
// SQL QUERY PANEL (issue #48)
// localStorage namespace: file_activity.sqlq.saved -> {name: sql, ...}
// ═══════════════════════════════════════════════════
const SQLQ_STORE_KEY = 'file_activity.sqlq.saved';

function sqlqLoadStore() {
    try { return JSON.parse(localStorage.getItem(SQLQ_STORE_KEY) || '{}'); }
    catch { return {}; }
}
function sqlqSaveStore(obj) {
    localStorage.setItem(SQLQ_STORE_KEY, JSON.stringify(obj));
}
function sqlqRefreshDropdown() {
    const sel = document.getElementById('sqlq-saved');
    const store = sqlqLoadStore();
    const names = Object.keys(store).sort();
    sel.innerHTML = '<option value="">Kayitli sorgular...</option>' +
        names.map(n => `<option value="${n}">${n}</option>`).join('');
}
function sqlqInit() {
    sqlqRefreshDropdown();
    const ta = document.getElementById('sqlq-text');
    if (ta && !ta.value) ta.value = 'SELECT id, file_name, file_size FROM scanned_files ORDER BY file_size DESC LIMIT 50';
}
function sqlqLoadSaved(name) {
    if (!name) return;
    const store = sqlqLoadStore();
    if (store[name]) document.getElementById('sqlq-text').value = store[name];
}
function sqlqSaveCurrent() {
    const sql = document.getElementById('sqlq-text').value.trim();
    if (!sql) { notify('Bos sorgu kaydedilemez', 'warning'); return; }
    const name = prompt('Sorgu adi:');
    if (!name) return;
    const store = sqlqLoadStore();
    store[name] = sql;
    sqlqSaveStore(store);
    sqlqRefreshDropdown();
    document.getElementById('sqlq-saved').value = name;
    notify(`Kaydedildi: ${name}`, 'success');
}
function sqlqDeleteSaved() {
    const sel = document.getElementById('sqlq-saved');
    const name = sel.value;
    if (!name) { notify('Once bir kayit secin', 'warning'); return; }
    if (!confirm(`Sil: ${name}?`)) return;
    const store = sqlqLoadStore();
    delete store[name];
    sqlqSaveStore(store);
    sqlqRefreshDropdown();
    notify('Silindi', 'success');
}
function sqlqEscape(v) {
    if (v === null || v === undefined) return '<span style="color:var(--text-muted)">NULL</span>';
    const s = String(v);
    return s.replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}
async function sqlqRun() {
    const sql = document.getElementById('sqlq-text').value.trim();
    const max_rows = parseInt(document.getElementById('sqlq-max').value, 10) || 1000;
    const status = document.getElementById('sqlq-status');
    const tbl = document.getElementById('sqlq-result');
    if (!sql) { notify('Bos sorgu', 'warning'); return; }
    status.textContent = 'Calisiyor...';
    tbl.innerHTML = '';
    try {
        const r = await api('/analytics/query', {
            method: 'POST',
            body: JSON.stringify({ sql, max_rows }),
        });
        const cols = r.columns || [];
        const rows = r.rows || [];
        let html = '<thead><tr>' +
            cols.map(c => `<th style="position:sticky;top:0;background:var(--bg-secondary);padding:8px 10px;text-align:left;font-size:12px;border-bottom:1px solid var(--border-light)">${sqlqEscape(c)}</th>`).join('') +
            '</tr></thead><tbody>';
        if (rows.length === 0) {
            html += `<tr><td colspan="${Math.max(cols.length,1)}" style="padding:18px;text-align:center;color:var(--text-muted)">Sonuc bulunamadi</td></tr>`;
        } else {
            for (const row of rows) {
                html += '<tr>' + row.map(v => `<td style="padding:6px 10px;font-size:12px;border-bottom:1px solid var(--border);font-family:Consolas,monospace">${sqlqEscape(v)}</td>`).join('') + '</tr>';
            }
        }
        html += '</tbody>';
        tbl.innerHTML = html;
        const flag = r.truncated ? ' <span style="color:var(--warning)">(kesildi)</span>' : '';
        status.innerHTML = `${r.row_count} satir - ${r.elapsed_ms} ms${flag}`;
    } catch (e) {
        status.textContent = 'Hata';
    }
}

// ═══════════════════════════════════════════════════
// GUVENLIK > YETIM SID'LER / RANSOMWARE / ACL (issue #81)
// Reusable, lightweight entity-list renderer used by all three security
// pages. Mirrors the contract of the planned components/entity-list.js
// from PR #98 — vanilla JS, no dependencies. Each call replaces the
// container's contents.
// ═══════════════════════════════════════════════════

let _securityFlagsCache = null;
async function getSecurityFlags() {
    if (_securityFlagsCache) return _securityFlagsCache;
    try {
        _securityFlagsCache = await api('/security/feature-flags', { silent: true });
    } catch (e) {
        _securityFlagsCache = { ransomware: { enabled: false }, orphan_sid: { enabled: false }, acl: { enabled: true } };
    }
    return _securityFlagsCache;
}

// escapeHtml() canonical definition lives at the top of this script block
// (security audit 2026-04-28, H-1). Local duplicate removed — function
// declarations are hoisted within the same <script> block, so all callers
// here continue to resolve to the canonical version.

function renderEntityList(containerId, items, columns, opts = {}) {
    const c = document.getElementById(containerId);
    if (!c) return;
    if (!items || !items.length) {
        c.innerHTML = `<div style="padding:32px;text-align:center;color:var(--text-muted)">${opts.emptyHtml || 'Kayit bulunamadi'}</div>`;
        return;
    }
    let html = '<div class="table-wrap"><table style="width:100%;border-collapse:collapse">';
    html += '<thead><tr>' + columns.map(col =>
        `<th style="text-align:left;padding:8px 10px;border-bottom:1px solid var(--border);font-size:12px;color:var(--text-secondary)">${escapeHtml(col.label)}</th>`
    ).join('') + '</tr></thead><tbody>';
    for (const item of items) {
        html += '<tr>' + columns.map(col => {
            const raw = typeof col.field === 'function' ? col.field(item) : item[col.field];
            const cell = col.render ? col.render(item, raw) : escapeHtml(raw == null ? '' : raw);
            return `<td style="padding:8px 10px;border-bottom:1px solid var(--border);font-size:12px">${cell}</td>`;
        }).join('') + '</tr>';
    }
    html += '</tbody></table></div>';
    c.innerHTML = html;
}

// ─── ORPHAN SIDS ───────────────────────────────────────

async function loadOrphanSids() {
    const flags = await getSecurityFlags();
    const banner = document.getElementById('orphan-sids-banner');
    if (!flags.orphan_sid.enabled) {
        banner.style.display = 'block';
        banner.textContent = 'Bu ozellik kapali — config.yaml > security.orphan_sid.enabled: true ile aciliyor.';
    } else {
        banner.style.display = 'none';
    }

    const sourceId = document.getElementById('orphan-sids-source')?.value;
    const summary = document.getElementById('orphan-sids-summary');
    const xlsxBtn = document.getElementById('orphan-sids-xlsx-btn');
    const csvBtn = document.getElementById('orphan-sids-csv-btn');
    const reBtn = document.getElementById('orphan-sids-reassign-btn');
    if (!sourceId) {
        summary.innerHTML = '';
        renderEntityList('orphan-sids-container', [], [], { emptyHtml: 'Lutfen bir kaynak secin' });
        if (xlsxBtn) xlsxBtn.style.display = 'none';
        if (csvBtn) csvBtn.style.display = 'none';
        if (reBtn) reBtn.style.display = 'none';
        return;
    }
    if (xlsxBtn) xlsxBtn.style.display = '';
    if (csvBtn) csvBtn.style.display = '';
    if (reBtn) reBtn.style.display = '';

    try {
        const data = await api(`/security/orphan-sids/${sourceId}`);
        summary.innerHTML = `
            <div class="card accent"><div class="card-label">Toplam Dosya</div><div class="card-value">${formatNum(data.total_files || 0)}</div></div>
            <div class="card warning"><div class="card-label">Yetim Dosya</div><div class="card-value">${formatNum(data.total_orphan_files || 0)}</div></div>
            <div class="card danger"><div class="card-label">Yetim SID</div><div class="card-value">${formatNum((data.orphan_sids || []).length)}</div></div>
            <div class="card success"><div class="card-label">Sure</div><div class="card-value">${data.elapsed_seconds || 0}s</div></div>
        `;
        const items = data.orphan_sids || [];
        renderEntityList('orphan-sids-container', items, [
            { field: 'sid', label: 'SID / Sahip' },
            { field: 'sid', label: 'Etiket', render: (i, v) => `<span style="color:var(--text-muted)">${escapeHtml(v.includes('\\\\') ? v.split('\\\\').pop() : v)}</span>` },
            { field: 'file_count', label: 'Dosya Sayisi', render: (i, v) => formatNum(v) },
            { field: 'total_size', label: 'Toplam Boyut', render: (i, v) => formatSize(v || 0) },
            { field: 'sample_paths', label: 'Ornekler', render: (i, v) => `<span style="color:var(--text-muted);font-size:11px">${escapeHtml((v || []).slice(0,2).join(' | '))}</span>` },
            {
                field: 'sid', label: 'Islem',
                render: (item) => `<button class="btn btn-sm btn-outline" onclick="prefillOrphanReassign(${JSON.stringify(item.sid).replace(/"/g, '&quot;')})">Yeniden Ata</button>`,
            },
        ], { emptyHtml: '<span style="color:var(--success)">Yetim SID bulunamadi — tum sahipler AD\'de cozulebiliyor</span>' });
    } catch (e) {
        notify('Yetim SID raporu alinamadi: ' + (e.message || e), 'error');
    }
}

async function exportOrphanSidsXlsx(ev) {
    const sourceId = document.getElementById('orphan-sids-source')?.value;
    if (!sourceId) { notify('Once kaynak secin', 'warning'); return; }
    await withButtonLoading(ev.target, async (signal) => {
        await fetchAndDownload(`${API}/security/orphan-sids/${sourceId}/export.xlsx`, signal, `orphan_sids_${sourceId}.xlsx`);
    });
}

async function exportOrphanSidsCsv(ev) {
    const sourceId = document.getElementById('orphan-sids-source')?.value;
    if (!sourceId) { notify('Once kaynak secin', 'warning'); return; }
    await withButtonLoading(ev.target, async (signal) => {
        await fetchAndDownload(`${API}/security/orphan-sids/${sourceId}/export.csv`, signal, `orphan_sids_${sourceId}.csv`);
    });
}

function openOrphanReassignModal() {
    document.getElementById('orphan-reassign-sid').value = '';
    document.getElementById('orphan-reassign-new-owner').value = '';
    document.getElementById('orphan-reassign-dryrun').checked = true;
    document.getElementById('orphan-reassign-confirm').checked = false;
    document.getElementById('modal-orphan-reassign').classList.add('active');
}

function prefillOrphanReassign(sid) {
    openOrphanReassignModal();
    document.getElementById('orphan-reassign-sid').value = sid;
}

async function submitOrphanReassign() {
    const sourceId = document.getElementById('orphan-sids-source')?.value;
    if (!sourceId) { notify('Once kaynak secin', 'warning'); return; }
    const sid = document.getElementById('orphan-reassign-sid').value.trim();
    const newOwner = document.getElementById('orphan-reassign-new-owner').value.trim();
    const dryRun = document.getElementById('orphan-reassign-dryrun').checked;
    const confirmFlag = document.getElementById('orphan-reassign-confirm').checked;
    if (!sid || !newOwner) { notify('SID ve yeni sahip zorunlu', 'warning'); return; }
    if (!confirmFlag) { notify('Onay kutusunu isaretleyin', 'warning'); return; }
    const flags = await getSecurityFlags();
    if (!dryRun && flags.orphan_sid.require_dual_approval_for_reassign) {
        notify('Ikinci onay gerekli — canli devir icin ikinci onaylayici eklenmeli (issue #83)', 'warning');
        return;
    }
    try {
        const r = await api('/security/orphan-sids/reassign', {
            method: 'POST',
            body: JSON.stringify({ source_id: parseInt(sourceId), sid, new_owner: newOwner, dry_run: dryRun }),
        });
        const count = dryRun ? (r.scanned || 0) : (r.changed || 0);
        notify(`Yeniden atama ${dryRun ? '(dry-run) ' : ''}tamamlandi: ${count} dosya`, 'success');
        closeModal('modal-orphan-reassign');
        loadOrphanSids();
    } catch (e) {
        notify('Yeniden atama hatasi: ' + (e.message || e), 'error');
    }
}

// ─── RANSOMWARE ALERTS ────────────────────────────────

async function loadRansomwareAlerts() {
    const flags = await getSecurityFlags();
    const banner = document.getElementById('ransomware-banner');
    if (!flags.ransomware.enabled) {
        banner.style.display = 'block';
        banner.textContent = 'Bu ozellik kapali — config.yaml > security.ransomware.enabled: true ile aciliyor.';
    } else {
        banner.style.display = 'none';
    }
    const win = parseInt(document.getElementById('ransomware-window').value || '1440', 10);
    const summary = document.getElementById('ransomware-summary');
    try {
        const alerts = await api(`/security/ransomware/alerts?since_minutes=${win}`);
        const total = alerts.length;
        const ackd = alerts.filter(a => a.acknowledged_at).length;
        const critical = alerts.filter(a => (a.severity || '').toLowerCase() === 'critical').length;
        summary.innerHTML = `
            <div class="card ${total ? 'danger' : 'success'}"><div class="card-label">Toplam Uyari</div><div class="card-value">${formatNum(total)}</div><div class="card-sub">${win} dk pencere</div></div>
            <div class="card warning"><div class="card-label">Kritik</div><div class="card-value">${formatNum(critical)}</div></div>
            <div class="card success"><div class="card-label">Onaylanan</div><div class="card-value">${formatNum(ackd)}</div></div>
            <div class="card accent"><div class="card-label">Bekleyen</div><div class="card-value">${formatNum(total - ackd)}</div></div>
        `;
        const sevColors = { critical: 'var(--danger)', high: 'var(--danger)', warning: 'var(--warning)', info: 'var(--info)' };
        renderEntityList('ransomware-container', alerts, [
            { field: 'triggered_at', label: 'Zaman', render: (i, v) => `<span style="font-family:Consolas,monospace;font-size:11px">${escapeHtml(v || '')}</span>` },
            { field: 'rule_name', label: 'Kural' },
            {
                field: 'severity', label: 'Ciddiyet',
                render: (i, v) => {
                    const c = sevColors[(v || '').toLowerCase()] || 'var(--text-muted)';
                    return `<span style="display:inline-block;padding:2px 8px;border-radius:10px;font-size:10px;font-weight:700;background:${c}22;color:${c}">${escapeHtml((v||'').toUpperCase())}</span>`;
                }
            },
            { field: (a) => (a.sample_paths && a.sample_paths[0]) || '', label: 'Dosya', render: (i, v) => `<span style="font-family:Consolas,monospace;font-size:11px;color:var(--text-muted)">${escapeHtml(v)}</span>` },
            { field: 'username', label: 'Kullanici' },
            {
                field: 'acknowledged_at', label: 'Durum',
                render: (i, v) => v ? `<span style="color:var(--success)">Onaylandi (${escapeHtml(i.acknowledged_by||'?')})</span>` : `<button class="btn btn-sm btn-warning" onclick="acknowledgeRansomwareAlert(${i.id})">Acknowledge</button>`
            },
        ], { emptyHtml: '<div style="font-size:36px;color:var(--success)">&#10004;</div><div style="margin-top:8px;color:var(--success);font-weight:600">Su an aktif uyari yok</div>' });
    } catch (e) {
        notify('Uyarilar alinamadi: ' + (e.message || e), 'error');
    }
}

async function acknowledgeRansomwareAlert(id) {
    try {
        await api(`/security/ransomware/alerts/${id}/acknowledge`, { method: 'POST' });
        notify('Uyari onaylandi', 'success');
        loadRansomwareAlerts();
    } catch (e) {
        notify('Onay hatasi: ' + (e.message || e), 'error');
    }
}

async function acknowledgeAllRansomware(ev) {
    if (!confirm('Bu pencerede ki tum uyarilari onaylamak istiyor musunuz?')) return;
    const win = parseInt(document.getElementById('ransomware-window').value || '1440', 10);
    await withButtonLoading(ev.target, async () => {
        const r = await api(`/security/ransomware/alerts/acknowledge-all?since_minutes=${win}`, { method: 'POST' });
        notify(`${r.rows_updated || 0} uyari onaylandi`, 'success');
        loadRansomwareAlerts();
    });
}

async function exportRansomwareXlsx(ev) {
    const win = parseInt(document.getElementById('ransomware-window').value || '1440', 10);
    await withButtonLoading(ev.target, async (signal) => {
        await fetchAndDownload(`${API}/security/ransomware/alerts/export.xlsx?since_minutes=${win}`, signal, 'ransomware_alerts.xlsx');
    });
}

// ─── ACL ANALYZER ─────────────────────────────────────

let _aclMode = 'sprawl'; // 'sprawl' | 'trustee'
let _aclTrustee = null;

async function loadAclAnalyzer() {
    _aclMode = 'sprawl';
    _aclTrustee = null;
    document.getElementById('acl-mode-indicator').textContent = '';
    document.getElementById('acl-panel-title').textContent = 'Sprawl Tespiti (en cok yetkilendirilmis trustee\'lar)';
    const sourceId = document.getElementById('acl-source')?.value || '';

    let url = '/security/acl/sprawl';
    if (sourceId) {
        // sprawl endpoint accepts scan_id, not source_id — derive most recent.
        try {
            const sources = await api('/sources', { silent: true });
            const s = (sources || []).find(x => String(x.id) === String(sourceId));
            if (s && s.last_scanned_at) {
                // No public scan_id by-source endpoint; rely on sprawl with no scan_id
                // and surface ALL trustees. This matches the analyzer default.
            }
        } catch {}
    }
    try {
        const data = await api(url);
        const trustees = data.trustees || [];
        renderEntityList('acl-container', trustees, [
            { field: 'trustee_sid', label: 'Trustee SID', render: (i, v) => `<span style="font-family:Consolas,monospace;font-size:11px">${escapeHtml(v)}</span>` },
            { field: 'trustee_name', label: 'Etiket', render: (i, v) => escapeHtml(v || '(cozulemedi)') },
            { field: 'file_count', label: 'Dosya Sayisi', render: (i, v) => formatNum(v) },
            { field: 'sample_permission_name', label: 'Ornek Izin', render: (i, v) => escapeHtml(v || '') },
            {
                field: 'trustee_sid', label: 'Islem',
                render: (item) => `<button class="btn btn-sm btn-outline" onclick="loadAclTrusteePaths(${JSON.stringify(item.trustee_sid).replace(/"/g, '&quot;')})">Yollari Gor</button>`
            },
        ], { emptyHtml: 'Sprawl tespiti icin once bir ACL snapshot calistirilmis olmali (POST /api/security/acl/scan/{source_id})' });
    } catch (e) {
        notify('ACL sprawl alinamadi: ' + (e.message || e), 'error');
    }
}

async function searchAclTrustee() {
    const sid = (document.getElementById('acl-trustee-search').value || '').trim();
    if (!sid) { notify('Trustee SID girin', 'warning'); return; }
    await loadAclTrusteePaths(sid);
}

async function loadAclTrusteePaths(sid) {
    _aclMode = 'trustee';
    _aclTrustee = sid;
    document.getElementById('acl-mode-indicator').innerHTML = `<span style="color:var(--accent)">Trustee modu:</span> ${escapeHtml(sid)} <a href="#" onclick="event.preventDefault(); loadAclAnalyzer()" style="color:var(--accent-light);margin-left:8px">[sprawl gorunume don]</a>`;
    document.getElementById('acl-panel-title').textContent = 'Trustee Erisim Yollari';
    try {
        const data = await api(`/security/acl/trustee/${encodeURIComponent(sid)}/paths?limit=100`);
        const paths = data.paths || [];
        renderEntityList('acl-container', paths, [
            { field: 'file_path', label: 'Dosya Yolu', render: (i, v) => `<span style="font-family:Consolas,monospace;font-size:11px">${escapeHtml(v)}</span>` },
            { field: 'permission_name', label: 'Izin' },
            { field: 'ace_type', label: 'ACE Turu', render: (i, v) => {
                const c = v === 'DENY' ? 'var(--danger)' : 'var(--success)';
                return `<span style="color:${c}">${escapeHtml(v || '')}</span>`;
            } },
            { field: 'permissions_mask', label: 'Mask' },
            { field: 'is_inherited', label: 'Devralinmis', render: (i, v) => v ? 'Evet' : 'Hayir' },
        ], { emptyHtml: 'Bu trustee icin kayit bulunamadi' });
    } catch (e) {
        notify('Trustee yollari alinamadi: ' + (e.message || e), 'error');
    }
}

async function exportAclXlsx(ev) {
    await withButtonLoading(ev.target, async (signal) => {
        if (_aclMode === 'trustee' && _aclTrustee) {
            await fetchAndDownload(`${API}/security/acl/trustee/${encodeURIComponent(_aclTrustee)}/paths/export.xlsx?limit=10000`, signal, 'acl_trustee_paths.xlsx');
        } else {
            await fetchAndDownload(`${API}/security/acl/sprawl/export.xlsx`, signal, 'acl_sprawl.xlsx');
        }
    });
}

// ─── EXTENSION ANOMALIES (issue #144 Phase 1) ─────────
// Wrong-extension detection (Czkawka pattern). Lists files whose
// declared extension doesn't match the libmagic-detected MIME type.
// Highlights "executable disguised as document" as critical.

let _extAnomalySelection = new Set();   // selected file_paths
let _extAnomalyRows = [];               // last fetched rows

async function loadExtensionAnomalies() {
    const flags = await getSecurityFlags();
    const banner = document.getElementById('ext-anomaly-banner');
    if (!flags.extension_anomalies || !flags.extension_anomalies.enabled) {
        banner.style.display = 'block';
        banner.textContent = 'Bu ozellik kapali — config.yaml > scanner.detect_wrong_extensions: true ile aciliyor (libmagic gerektirir).';
    } else {
        banner.style.display = 'none';
    }

    const sourceId = document.getElementById('ext-anomaly-source')?.value;
    const severity = document.getElementById('ext-anomaly-severity')?.value || '';
    const summary = document.getElementById('ext-anomaly-summary');
    const xlsxBtn = document.getElementById('ext-anomaly-xlsx-btn');
    if (!sourceId) {
        summary.innerHTML = '';
        renderEntityList('ext-anomaly-container', [], [], { emptyHtml: 'Lutfen bir kaynak secin' });
        if (xlsxBtn) xlsxBtn.style.display = 'none';
        return;
    }
    if (xlsxBtn) xlsxBtn.style.display = '';

    // Issue #181 Track B2: try partial summary first while scan is running.
    const ps = await _fetchPartialSummaryV2(sourceId);
    const wasPartial = !!_psv2PrevWasPartial['extension-anomalies'];
    if (ps && ps.scan_state && ps.scan_state !== 'completed') {
        _psv2PrevWasPartial['extension-anomalies'] = true;
        _psv2ShowBanner('ext-anomaly-partial-banner', ps.scan_state, ps.progress);
        const extCount = (ps.summary && ps.summary.anomalies_so_far && ps.summary.anomalies_so_far.extension) || 0;
        summary.innerHTML = `
            <div class="card warning"><div class="card-label">Tahmini Uzanti Anomalisi</div><div class="card-value">${formatNum(extCount)}</div><div class="card-sub">Kismi veri</div></div>
            <div class="card accent"><div class="card-label">Detay</div><div class="card-value" style="font-size:14px">Tarama devam ediyor</div><div class="card-sub">Tarama bitince detay goruntulenecek</div></div>
        `;
        renderEntityList('ext-anomaly-container', [], [], { emptyHtml: 'Tarama tamamlandiktan sonra detayli liste goruntulenecek' });
        _psv2StartPoll(sourceId, 'extension-anomalies', loadExtensionAnomalies);
        return;
    }

    if (wasPartial && ps && ps.scan_state === 'completed') {
        notify('Tarama tamamlandi, Yanlis Uzantili Dosyalar guncellendi', 'success');
    }
    _psv2PrevWasPartial['extension-anomalies'] = false;
    _psv2HideBanner('ext-anomaly-partial-banner');
    _psv2StopPoll();

    try {
        const qs = new URLSearchParams({ source_id: sourceId, limit: '500' });
        if (severity) qs.set('severity', severity);
        const data = await api(`/security/extension-anomalies?${qs.toString()}`);
        const bs = data.by_severity || {};
        summary.innerHTML = `
            <div class="card danger"><div class="card-label">Kritik</div><div class="card-value">${formatNum(bs.critical || 0)}</div></div>
            <div class="card warning"><div class="card-label">Yuksek</div><div class="card-value">${formatNum(bs.high || 0)}</div></div>
            <div class="card accent"><div class="card-label">Orta</div><div class="card-value">${formatNum(bs.medium || 0)}</div></div>
            <div class="card success"><div class="card-label">Dusuk</div><div class="card-value">${formatNum(bs.low || 0)}</div></div>
        `;
        const items = data.items || [];
        _extAnomalyRows = items;
        _extAnomalySelection = new Set();
        _extAnomalyUpdateSelCount();

        const sevColors = { critical: 'var(--danger)', high: 'var(--danger)', medium: 'var(--warning)', low: 'var(--info)' };
        renderEntityList('ext-anomaly-container', items, [
            {
                field: 'id',
                label: '<input type="checkbox" onclick="extAnomalyToggleAll(this)">',
                render: (item) => `<input type="checkbox" class="ext-anomaly-row-chk" data-path="${escapeHtml(item.file_path)}" onchange="extAnomalyToggleRow(this)">`,
            },
            {
                field: 'file_path', label: 'Dosya Yolu',
                render: (i, v) => `<span style="font-family:Consolas,monospace;font-size:11px">${escapeHtml(v || '')}</span>`,
            },
            { field: 'declared_ext', label: 'Beyan Edilen' },
            {
                field: 'detected_mime', label: 'Tespit Edilen MIME',
                render: (i, v) => `<span style="font-family:Consolas,monospace;font-size:11px;color:var(--text-muted)">${escapeHtml(v || '')}</span>`,
            },
            {
                field: 'severity', label: 'Ciddiyet',
                render: (i, v) => {
                    const c = sevColors[(v || '').toLowerCase()] || 'var(--text-muted)';
                    return `<span style="display:inline-block;padding:2px 8px;border-radius:10px;font-size:10px;font-weight:700;background:${c}22;color:${c}">${escapeHtml((v || '').toUpperCase())}</span>`;
                },
            },
            {
                field: 'detected_at', label: 'Tespit',
                render: (i, v) => `<span style="font-family:Consolas,monospace;font-size:11px">${escapeHtml(v || '')}</span>`,
            },
        ], { emptyHtml: '<span style="color:var(--success)">Anomali bulunamadi — tum uzantilar MIME ile uyumlu</span>' });
    } catch (e) {
        notify('Uzanti anomalileri alinamadi: ' + (e.message || e), 'error');
    }
}

function extAnomalyToggleRow(chk) {
    const p = chk.getAttribute('data-path');
    if (!p) return;
    if (chk.checked) _extAnomalySelection.add(p);
    else _extAnomalySelection.delete(p);
    _extAnomalyUpdateSelCount();
}

function extAnomalyToggleAll(headerChk) {
    const boxes = document.querySelectorAll('.ext-anomaly-row-chk');
    _extAnomalySelection = new Set();
    boxes.forEach(c => {
        c.checked = headerChk.checked;
        if (headerChk.checked) {
            const p = c.getAttribute('data-path');
            if (p) _extAnomalySelection.add(p);
        }
    });
    _extAnomalyUpdateSelCount();
}

function _extAnomalyUpdateSelCount() {
    const el = document.getElementById('ext-anomaly-selcount');
    if (el) el.textContent = _extAnomalySelection.size + ' secili';
}

function extAnomalyOpenSelected() {
    const sel = Array.from(_extAnomalySelection);
    if (!sel.length) { notify('Once en az bir dosya secin', 'warning'); return; }
    if (sel.length > 5) {
        notify('Hedefe Git en fazla 5 dosya icin acilabilir', 'warning');
        return;
    }
    // Open the parent folder in the browser via /api/open-folder if it
    // exists; otherwise just copy to clipboard with a notify hint.
    sel.forEach(p => {
        // Best-effort: try a generic explorer open endpoint
        api('/system/open-folder', { method: 'POST', body: JSON.stringify({ path: p }), silent: true })
            .catch(() => {
                // Fallback — just log; the path is in the table for manual copy
                console.log('[ext-anomaly] hedef:', p);
            });
    });
    notify(`${sel.length} dosyanin klasoru acilmaya calisildi`, 'info');
}

async function exportExtensionAnomaliesXlsx(ev) {
    const sourceId = document.getElementById('ext-anomaly-source')?.value;
    if (!sourceId) { notify('Once kaynak secin', 'warning'); return; }
    const severity = document.getElementById('ext-anomaly-severity')?.value || '';
    await withButtonLoading(ev.target, async (signal) => {
        const qs = severity ? `?severity=${encodeURIComponent(severity)}` : '';
        await fetchAndDownload(
            `${API}/security/extension-anomalies/${sourceId}/export.xlsx${qs}`,
            signal,
            `extension_anomalies_${sourceId}.xlsx`,
        );
    });
}

// ═══════════════════════════════════════════════════
// CHARGEBACK / COST CENTER (issue #111)
// ═══════════════════════════════════════════════════
let _cbReport = null;          // last computed report payload
let _cbCenters = [];           // cached center list (with owner_patterns)

function _attachChargebackViewToggle() {
    const page = document.getElementById('page-chargeback');
    if (!page || typeof attachViewToggle !== 'function') return;
    attachViewToggle(page, {
        pageKey: 'chargeback',
        renderVisual: _renderChargebackVisual,
        renderGrid: _renderChargebackGrid,
        defaultMode: 'visual',
    });
}

function _renderChargebackVisual() {
    const v = document.getElementById('cb-visual-host');
    const g = document.getElementById('cb-grid-host');
    if (v) v.style.display = '';
    if (g) g.style.display = 'none';
    if (_cbReport) _renderChargebackCharts(_cbReport);
}

function _renderChargebackGrid() {
    const v = document.getElementById('cb-visual-host');
    const g = document.getElementById('cb-grid-host');
    if (v) v.style.display = 'none';
    if (g) g.style.display = '';
    if (_cbReport) _renderChargebackEntityList(_cbReport);
}

async function loadChargeback() {
    _attachChargebackViewToggle();
    // Always refresh the centers list so the management table reflects the
    // server-side state, even when the user has not yet selected a source.
    await _loadChargebackCenters();

    const sid = document.getElementById('cb-source').value;
    if (!sid) {
        document.getElementById('cb-summary-cards').innerHTML = '';
        document.getElementById('cb-unmapped-list').innerHTML =
            '<div style="padding:20px;color:var(--text-muted)">Once bir kaynak secin</div>';
        const xb = document.getElementById('cb-xlsx-btn');
        if (xb) xb.style.display = 'none';
        return;
    }

    let data;
    try {
        data = await api(`/chargeback/${sid}`);
    } catch (e) {
        // 404 = no completed scan yet for this source
        document.getElementById('cb-summary-cards').innerHTML =
            '<div style="padding:20px;color:var(--text-muted)">Bu kaynak icin tamamlanmis bir tarama yok.</div>';
        return;
    }
    _cbReport = data;
    document.getElementById('cb-xlsx-btn').style.display = '';

    // Summary cards
    const fmtUSD = n => '$' + Number(n || 0).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2});
    document.getElementById('cb-summary-cards').innerHTML = `
        <div class="card" style="border-top:3px solid var(--accent)">
            <div class="card-label">Toplam Cost Center</div>
            <div class="card-value">${(data.centers || []).length}</div>
            <div class="card-sub">${(data.unmapped_owners || []).length} eslesmeyen owner</div>
        </div>
        <div class="card" style="border-top:3px solid var(--info)">
            <div class="card-label">Toplam Veri</div>
            <div class="card-value">${Number(data.total_gb || 0).toFixed(2)} GB</div>
            <div class="card-sub">${formatNum(data.total_file_count || 0)} dosya</div>
        </div>
        <div class="card" style="border-top:3px solid var(--success)">
            <div class="card-label">Aylik Toplam Maliyet</div>
            <div class="card-value">${fmtUSD(data.total_monthly_cost)}</div>
            <div class="card-sub">cost_per_gb_month × GB</div>
        </div>
    `;

    // Unmapped owners
    const u = data.unmapped_owners || [];
    if (u.length === 0) {
        document.getElementById('cb-unmapped-list').innerHTML =
            '<div style="padding:20px;color:var(--success)">Tum owner\'lar bir cost-center\'a atanmis.</div>';
    } else {
        document.getElementById('cb-unmapped-list').innerHTML = `
            <div class="table-wrap"><table>
                <thead><tr><th>Owner</th><th>Dosya</th><th>Toplam GB</th><th>Aksiyon</th></tr></thead>
                <tbody>${u.slice(0, 50).map(r => `
                    <tr>
                        <td><code>${_cbEsc(r.owner)}</code></td>
                        <td>${formatNum(r.file_count)}</td>
                        <td>${Number(r.total_gb || 0).toFixed(3)}</td>
                        <td>
                            <select onchange="cbAssignUnmapped('${_cbEsc(r.owner).replace(/'/g, "\\'")}', this.value, this)" style="font-size:11px">
                                <option value="">Cost center sec...</option>
                                ${(_cbCenters || []).map(c =>
                                    `<option value="${c.id}">${_cbEsc(c.name)}</option>`
                                ).join('')}
                            </select>
                        </td>
                    </tr>
                `).join('')}</tbody>
            </table></div>
            ${u.length > 50 ? `<div style="font-size:11px;color:var(--text-muted);margin-top:8px">${u.length - 50} owner daha gosterilmedi.</div>` : ''}
        `;
    }

    // Charts + tables
    _renderChargebackCharts(data);
    _renderChargebackEntityList(data);
}

function _cbEsc(s) {
    return String(s == null ? '' : s)
        .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

async function _loadChargebackCenters() {
    try {
        const data = await api('/chargeback/centers');
        _cbCenters = data.centers || [];
    } catch (e) {
        _cbCenters = [];
    }
    const host = document.getElementById('cb-centers-list');
    if (!host) return;
    if (_cbCenters.length === 0) {
        host.innerHTML = '<div style="padding:20px;color:var(--text-muted)">Henuz cost-center tanimlanmamis. Sag ustteki "+ Yeni Cost Center" butonu ile ekleyin.</div>';
        return;
    }
    host.innerHTML = `
        <div class="table-wrap"><table>
            <thead><tr><th>Ad</th><th>Aciklama</th><th>$/GB/ay</th><th>Owner Pattern Sayisi</th><th>Pattern'ler</th><th>Aksiyon</th></tr></thead>
            <tbody>${_cbCenters.map(c => `
                <tr>
                    <td><strong>${_cbEsc(c.name)}</strong></td>
                    <td>${_cbEsc(c.description || '')}</td>
                    <td>$${Number(c.cost_per_gb_month || 0).toFixed(4)}</td>
                    <td>${(c.owner_patterns || []).length}</td>
                    <td style="font-size:11px;font-family:monospace;color:var(--text-secondary)">
                        ${(c.owner_patterns || []).slice(0, 5).map(p =>
                            `<span style="display:inline-block;padding:2px 6px;background:var(--bg-secondary);border-radius:3px;margin:1px">${_cbEsc(p)} <a href="#" onclick="cbRemoveOwner(${c.id}, '${_cbEsc(p).replace(/'/g, "\\'")}', event)" style="color:var(--danger);text-decoration:none;margin-left:3px">×</a></span>`
                        ).join(' ')}
                        ${(c.owner_patterns || []).length > 5 ? `+${(c.owner_patterns || []).length - 5} daha` : ''}
                    </td>
                    <td>
                        <button class="btn btn-sm btn-outline" onclick="openChargebackOwnerModal(${c.id}, '${_cbEsc(c.name).replace(/'/g, "\\'")}')">+ Owner</button>
                        <button class="btn btn-sm btn-outline" style="color:var(--danger)" onclick="cbRemoveCenter(${c.id})">Sil</button>
                    </td>
                </tr>
            `).join('')}</tbody>
        </table></div>
    `;
}

function _renderChargebackCharts(data) {
    const centers = (data.centers || []).filter(c => (c.total_gb || 0) > 0);
    const COLORS_CB = ['#3b82f6','#10b981','#f59e0b','#ef4444','#8b5cf6','#ec4899','#14b8a6','#f97316','#84cc16','#06b6d4'];

    destroyChart('cb-pie-chart');
    const pieEl = document.getElementById('cb-pie-chart');
    if (pieEl && centers.length) {
        chartInstances['cb-pie-chart'] = new Chart(pieEl, {
            type: 'doughnut',
            data: {
                labels: centers.map(c => c.name),
                datasets: [{
                    data: centers.map(c => c.total_gb),
                    backgroundColor: COLORS_CB,
                    borderWidth: 0,
                }],
            },
            options: {
                responsive: true, maintainAspectRatio: false, cutout: '55%',
                plugins: {
                    legend: { position: 'right', labels: { boxWidth: 10, padding: 6, font: { size: 11 } } },
                    tooltip: {
                        callbacks: {
                            label: (ctx) => `${ctx.label}: ${Number(ctx.parsed).toFixed(2)} GB`,
                        },
                    },
                },
            },
        });
    }

    destroyChart('cb-bar-chart');
    const barEl = document.getElementById('cb-bar-chart');
    if (barEl && centers.length) {
        chartInstances['cb-bar-chart'] = new Chart(barEl, {
            type: 'bar',
            data: {
                labels: centers.map(c => c.name),
                datasets: [{
                    label: 'Aylik Maliyet ($)',
                    data: centers.map(c => c.monthly_cost || 0),
                    backgroundColor: COLORS_CB.map(c => c + '99'),
                    borderColor: COLORS_CB,
                    borderWidth: 1,
                    borderRadius: 4,
                }],
            },
            options: {
                indexAxis: 'y',
                responsive: true, maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                scales: { x: { beginAtZero: true, grid: { color: '#1e293b' } }, y: { grid: { display: false } } },
            },
        });
    }
}

function _renderChargebackEntityList(data) {
    const container = document.getElementById('cb-entity-container');
    if (!container || typeof renderEntityList !== 'function') return;

    const rows = (data.centers || []).map(c => ({
        id: c.id,
        name: c.name,
        total_gb: Number(c.total_gb || 0).toFixed(3),
        file_count: c.file_count || 0,
        monthly_cost: '$' + Number(c.monthly_cost || 0).toFixed(2),
        top_owner: (c.top_owners && c.top_owners[0]) ? c.top_owners[0].owner : '',
    }));

    renderEntityList(container, {
        rows: rows,
        rowKey: 'id',
        pageSize: 50,
        searchKeys: ['name', 'top_owner'],
        columns: [
            { key: 'name', label: 'Cost Center' },
            { key: 'total_gb', label: 'Toplam GB' },
            { key: 'file_count', label: 'Dosya Sayisi' },
            { key: 'monthly_cost', label: 'Aylik Maliyet' },
            { key: 'top_owner', label: 'En Buyuk Owner' },
        ],
        toolbar: {},
        emptyMessage: 'Bu kaynakta esleyen cost-center yok',
    });
}

// ---- CRUD modals & actions ----------------------------------------------

function openChargebackCenterModal() {
    document.getElementById('cb-form-name').value = '';
    document.getElementById('cb-form-desc').value = '';
    document.getElementById('cb-form-rate').value = '';
    openModal('modal-chargeback-center');
}

async function submitChargebackCenterForm(btn) {
    const name = document.getElementById('cb-form-name').value.trim();
    const description = document.getElementById('cb-form-desc').value.trim();
    const rate = parseFloat(document.getElementById('cb-form-rate').value || '0') || 0;
    if (!name) { notify('Cost center adi gerekli', 'warning'); return; }
    btn.disabled = true;
    try {
        await api('/chargeback/centers', {
            method: 'POST',
            body: JSON.stringify({ name, description, cost_per_gb_month: rate }),
        });
        notify('Cost center olusturuldu', 'success');
        closeModal('modal-chargeback-center');
        loadChargeback();
    } catch (e) {
        // notify already fired
    } finally {
        btn.disabled = false;
    }
}

function openChargebackOwnerModal(centerId, centerName) {
    document.getElementById('cb-owner-target-name').textContent = centerName;
    document.getElementById('cb-owner-form-center-id').value = String(centerId);
    document.getElementById('cb-owner-form-pattern').value = '';
    openModal('modal-chargeback-owner');
}

async function submitChargebackOwnerForm(btn) {
    const cid = parseInt(document.getElementById('cb-owner-form-center-id').value, 10);
    const pat = document.getElementById('cb-owner-form-pattern').value.trim();
    if (!cid || !pat) { notify('Pattern gerekli', 'warning'); return; }
    btn.disabled = true;
    try {
        await api(`/chargeback/centers/${cid}/owners`, {
            method: 'POST',
            body: JSON.stringify({ owner_pattern: pat }),
        });
        notify('Owner pattern eklendi', 'success');
        closeModal('modal-chargeback-owner');
        loadChargeback();
    } catch (e) {
        // notify fired
    } finally {
        btn.disabled = false;
    }
}

async function cbRemoveCenter(centerId) {
    if (!window.confirm('Bu cost center silinsin mi? Tum owner pattern\'leri de silinecek.')) return;
    try {
        await api(`/chargeback/centers/${centerId}`, { method: 'DELETE' });
        notify('Cost center silindi', 'success');
        loadChargeback();
    } catch (e) { /* notify fired */ }
}

async function cbRemoveOwner(centerId, pattern, ev) {
    if (ev) ev.preventDefault();
    try {
        await api(
            `/chargeback/centers/${centerId}/owners/${encodeURIComponent(pattern)}`,
            { method: 'DELETE' }
        );
        loadChargeback();
    } catch (e) { /* notify fired */ }
}

async function cbAssignUnmapped(owner, centerId, selectEl) {
    if (!centerId) return;
    try {
        await api(`/chargeback/centers/${centerId}/owners`, {
            method: 'POST',
            body: JSON.stringify({ owner_pattern: owner }),
        });
        notify('Owner ' + owner + ' atandi', 'success');
        loadChargeback();
    } catch (e) {
        if (selectEl) selectEl.value = '';
    }
}

async function exportChargebackXlsx(ev) {
    const sid = document.getElementById('cb-source').value;
    if (!sid) { notify('Once kaynak secin', 'warning'); return; }
    await withButtonLoading(ev.target, async (signal) => {
        await fetchAndDownload(
            `${API}/chargeback/${sid}/export.xlsx`,
            signal,
            `chargeback_source${sid}.xlsx`
        );
    });
}

// ═══════════════════════════════════════════════════
// CAPACITY FORECAST (issue #113)
// ═══════════════════════════════════════════════════
let _fcLastResult = null;        // last forecast response — for client-side recompute

function _attachForecastViewToggle() {
    const page = document.getElementById('page-forecast');
    if (!page || typeof attachViewToggle !== 'function') return;
    attachViewToggle(page, {
        pageKey: 'forecast',
        renderVisual: _renderForecastVisual,
        renderGrid: _renderForecastGrid,
        defaultMode: 'visual',
    });
}

function _renderForecastVisual() {
    const v = document.getElementById('fc-visual-host');
    const g = document.getElementById('fc-grid-host');
    if (v) v.style.display = '';
    if (g) g.style.display = 'none';
    if (_fcLastResult) _renderForecastChart(_fcLastResult);
}

function _renderForecastGrid() {
    const v = document.getElementById('fc-visual-host');
    const g = document.getElementById('fc-grid-host');
    if (v) v.style.display = 'none';
    if (g) g.style.display = '';
    if (_fcLastResult) _renderForecastEntityList(_fcLastResult);
}

async function loadForecast() {
    _attachForecastViewToggle();
    const sid = document.getElementById('fc-source').value;
    const horizon = parseInt(document.getElementById('fc-horizon').value || '180', 10);
    const xb = document.getElementById('fc-xlsx-btn');
    if (!sid) {
        document.getElementById('fc-kpi-cards').innerHTML = '';
        document.getElementById('fc-meta').innerHTML =
            '<div style="padding:20px;color:var(--text-muted)">Once bir kaynak secin</div>';
        if (xb) xb.style.display = 'none';
        return;
    }

    let data;
    try {
        data = await api(`/forecast/${sid}?horizon_days=${horizon}&model=linear`);
    } catch (e) {
        document.getElementById('fc-kpi-cards').innerHTML =
            '<div style="padding:20px;color:var(--text-muted)">Tahmin alinamadi.</div>';
        return;
    }
    _fcLastResult = data;
    if (xb) xb.style.display = '';

    // Pull threshold pct from server response if present, otherwise UI default.
    const pctInput = document.getElementById('fc-threshold-pct');
    if (data.capacity_threshold_pct && pctInput && !pctInput.dataset.userEdited) {
        pctInput.value = data.capacity_threshold_pct;
    }

    _renderForecastKPIs(data);
    _renderForecastChart(data);
    _renderForecastEntityList(data);
}

function _renderForecastKPIs(data) {
    const samples = data.samples_used || 0;
    const r2 = (data.r_squared || 0).toFixed(3);
    const slope = data.slope_bytes_per_day || 0;
    const slopePerDay = formatSize(Math.abs(slope));
    const slopeSign = slope < 0 ? '-' : '+';
    const horizon = data.horizon_days || 0;
    const predicted = formatSize(data.predicted_bytes || 0);

    let alarmHtml;
    let alarmColor = 'var(--success)';
    if (samples < 3) {
        alarmHtml = '<div class="card-value" style="font-size:18px">Yetersiz veri</div>'
            + '<div class="card-sub">3+ tarama gecmisi gerekli</div>';
        alarmColor = 'var(--warning)';
    } else if (data.capacity_alarm_at) {
        const today = new Date();
        const alarm = new Date(data.capacity_alarm_at + 'T00:00:00');
        const days = Math.round((alarm - today) / 86400000);
        if (days <= 0) {
            alarmHtml = `<div class="card-value" style="font-size:20px;color:var(--danger)">ALARM</div>`
                + `<div class="card-sub">Esik bugun veya gecmis: <code>${data.capacity_alarm_at}</code></div>`;
            alarmColor = 'var(--danger)';
        } else {
            alarmHtml = `<div class="card-value" style="font-size:20px;color:var(--warning)">${days} gun</div>`
                + `<div class="card-sub">Bu hizla %${data.capacity_threshold_pct || '?'} doluluga <code>${data.capacity_alarm_at}</code> ulasilir</div>`;
            alarmColor = 'var(--warning)';
        }
    } else {
        alarmHtml = '<div class="card-value" style="font-size:20px">Capacity alarm yok</div>'
            + '<div class="card-sub">Ufka kadar esige ulasilmiyor</div>';
    }

    document.getElementById('fc-kpi-cards').innerHTML = `
        <div class="card" style="border-top:3px solid var(--accent)">
            <div class="card-label">Ornek Sayisi</div>
            <div class="card-value">${samples}</div>
            <div class="card-sub">tamamlanmis scan_runs</div>
        </div>
        <div class="card" style="border-top:3px solid var(--info)">
            <div class="card-label">${horizon} Gunluk Tahmin</div>
            <div class="card-value" style="font-size:22px">${predicted}</div>
            <div class="card-sub">95% CI: ${formatSize(data.ci_low_bytes||0)} – ${formatSize(data.ci_high_bytes||0)}</div>
        </div>
        <div class="card" style="border-top:3px solid var(--success)">
            <div class="card-label">Buyume Hizi</div>
            <div class="card-value" style="font-size:22px">${slopeSign}${slopePerDay}/gun</div>
            <div class="card-sub">R² = ${r2}</div>
        </div>
        <div class="card" style="border-top:3px solid ${alarmColor}">
            <div class="card-label">Kapasite Alarmi</div>
            ${alarmHtml}
        </div>
    `;

    const meta = document.getElementById('fc-meta');
    if (meta) {
        const thr = data.threshold_bytes
            ? `${formatSize(data.threshold_bytes)} (%${data.capacity_threshold_pct || '?'} disk)`
            : '(belirsiz)';
        meta.innerHTML =
            `Esik: ${thr} · Disk toplami: ${data.disk_total_bytes ? formatSize(data.disk_total_bytes) : '-'} · ` +
            `Slope: ${slope.toLocaleString('tr-TR')} bayt/gun · R²: ${r2}`;
    }
}

function _renderForecastChart(data) {
    destroyChart('fc-line-chart');
    const el = document.getElementById('fc-line-chart');
    if (!el) return;

    const history = data.history || [];
    if (history.length === 0) {
        el.parentElement.innerHTML = '<div style="padding:40px;text-align:center;color:var(--text-muted)">Bu kaynakta tarama gecmisi yok.</div>';
        return;
    }

    // Build the projection line: from last historical point -> horizon endpoint.
    const lastTs = history[history.length - 1].ts;
    const lastBytes = history[history.length - 1].bytes;
    const horizonDate = new Date(new Date(lastTs).getTime() + (data.horizon_days || 180) * 86400000);
    const horizonIso = horizonDate.toISOString();

    const histPoints = history.map(h => ({ x: h.ts, y: h.bytes }));
    const projection = (data.samples_used || 0) >= 3
        ? [{ x: lastTs, y: lastBytes }, { x: horizonIso, y: data.predicted_bytes }]
        : [];
    const ciLow = (data.samples_used || 0) >= 3
        ? [{ x: lastTs, y: lastBytes }, { x: horizonIso, y: data.ci_low_bytes }]
        : [];
    const ciHigh = (data.samples_used || 0) >= 3
        ? [{ x: lastTs, y: lastBytes }, { x: horizonIso, y: data.ci_high_bytes }]
        : [];

    // Threshold line (horizontal) — anchored to history start so the user
    // sees where the projected line will cross it.
    const thr = data.threshold_bytes;
    const thrLine = thr
        ? [{ x: history[0].ts, y: thr }, { x: horizonIso, y: thr }]
        : [];

    chartInstances['fc-line-chart'] = new Chart(el, {
        type: 'line',
        data: {
            datasets: [
                {
                    label: 'Gecmis (bytes)',
                    data: histPoints,
                    borderColor: '#3b82f6',
                    backgroundColor: '#3b82f622',
                    borderWidth: 2,
                    pointRadius: 3,
                    fill: false,
                    tension: 0.1,
                },
                {
                    label: 'Projeksiyon',
                    data: projection,
                    borderColor: '#8b5cf6',
                    borderDash: [6, 4],
                    borderWidth: 2,
                    pointRadius: 0,
                    fill: false,
                },
                {
                    label: 'CI 95% ust',
                    data: ciHigh,
                    borderColor: '#8b5cf655',
                    borderWidth: 1,
                    pointRadius: 0,
                    fill: '+1',
                    backgroundColor: '#8b5cf61a',
                },
                {
                    label: 'CI 95% alt',
                    data: ciLow,
                    borderColor: '#8b5cf655',
                    borderWidth: 1,
                    pointRadius: 0,
                    fill: false,
                },
                ...(thrLine.length ? [{
                    label: 'Esik',
                    data: thrLine,
                    borderColor: '#ef4444',
                    borderDash: [3, 3],
                    borderWidth: 1.5,
                    pointRadius: 0,
                    fill: false,
                }] : []),
            ],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            parsing: false,
            scales: {
                x: {
                    type: 'time',
                    time: { unit: 'day' },
                    grid: { color: '#1e293b' },
                },
                y: {
                    beginAtZero: false,
                    grid: { color: '#1e293b' },
                    ticks: {
                        callback: (v) => formatSize(v),
                    },
                },
            },
            plugins: {
                legend: { position: 'top', labels: { boxWidth: 12, padding: 8 } },
                tooltip: {
                    callbacks: {
                        label: (ctx) => `${ctx.dataset.label}: ${formatSize(ctx.parsed.y)}`,
                    },
                },
            },
        },
    });
}

function _renderForecastEntityList(data) {
    const container = document.getElementById('fc-entity-container');
    if (!container || typeof renderEntityList !== 'function') return;
    if ((data.samples_used || 0) < 3) {
        container.innerHTML = '<div style="padding:30px;text-align:center;color:var(--text-muted)">3+ tarama olunca aylik donum noktalari hesaplanir.</div>';
        return;
    }

    // Project monthly milestones across the horizon
    const slope = data.slope_bytes_per_day || 0;
    const lastTs = data.last_ts;
    const lastBytes = data.last_bytes || 0;
    const horizonDays = data.horizon_days || 180;
    const monthsOut = Math.max(1, Math.floor(horizonDays / 30));
    const rows = [];
    const thr = data.threshold_bytes;
    const startDate = lastTs ? new Date(lastTs) : new Date();

    for (let m = 1; m <= monthsOut; m++) {
        const days = m * 30;
        const dt = new Date(startDate.getTime() + days * 86400000);
        const projected = lastBytes + slope * days;
        const pctOfThr = thr ? Math.round((projected / thr) * 100) : null;
        rows.push({
            id: m,
            month: m + ' ay sonra',
            date: dt.toISOString().slice(0, 10),
            projected_bytes: Math.round(projected),
            projected_human: formatSize(projected),
            pct_of_threshold: pctOfThr === null ? '-' : pctOfThr + '%',
            over_threshold: thr && projected >= thr ? 'EVET' : '-',
        });
    }

    renderEntityList(container, {
        rows: rows,
        rowKey: 'id',
        pageSize: 50,
        searchKeys: ['month', 'date'],
        columns: [
            { key: 'month', label: 'Ufuk' },
            { key: 'date', label: 'Tarih' },
            { key: 'projected_human', label: 'Tahmini Boyut' },
            { key: 'pct_of_threshold', label: '% Esik' },
            { key: 'over_threshold', label: 'Esik Asildi' },
        ],
        toolbar: {},
        emptyMessage: 'Donum noktasi yok',
    });
}

function recomputeForecastAlarm() {
    const pctInput = document.getElementById('fc-threshold-pct');
    if (pctInput) pctInput.dataset.userEdited = '1';
    if (!_fcLastResult) return;

    // Recompute alarm date client-side from slope/intercept + new threshold.
    const pct = parseFloat(pctInput.value || '85');
    const diskTotal = _fcLastResult.disk_total_bytes;
    if (!diskTotal || pct <= 0) return;
    const newThreshold = diskTotal * pct / 100;

    const slope = _fcLastResult.slope_bytes_per_day || 0;
    const intercept = _fcLastResult.intercept_bytes || 0;
    const lastTs = _fcLastResult.last_ts;
    const lastBytes = _fcLastResult.last_bytes || 0;

    let newAlarm = null;
    const todayIso = new Date().toISOString().slice(0, 10);
    if (lastBytes >= newThreshold) {
        newAlarm = todayIso;
    } else if (slope > 0 && _fcLastResult.samples_used >= 3) {
        // intercept + slope * t = newThreshold -> solve t (days from regression t0)
        const tCross = (newThreshold - intercept) / slope;
        // Map t back to a real date: t0 = first history sample.
        const firstTs = (_fcLastResult.history && _fcLastResult.history.length)
            ? _fcLastResult.history[0].ts : lastTs;
        if (firstTs) {
            const alarmDate = new Date(new Date(firstTs).getTime() + tCross * 86400000);
            if (alarmDate > new Date()) {
                newAlarm = alarmDate.toISOString().slice(0, 10);
            } else {
                newAlarm = todayIso;
            }
        }
    }

    // Patch the result + re-render KPIs (keep the chart's threshold line updated).
    _fcLastResult.capacity_alarm_at = newAlarm;
    _fcLastResult.capacity_threshold_pct = pct;
    _fcLastResult.threshold_bytes = newThreshold;
    _renderForecastKPIs(_fcLastResult);
    _renderForecastChart(_fcLastResult);
}

async function exportForecastXlsx(ev) {
    const sid = document.getElementById('fc-source').value;
    const horizon = parseInt(document.getElementById('fc-horizon').value || '180', 10);
    if (!sid) { notify('Once kaynak secin', 'warning'); return; }
    await withButtonLoading(ev.target, async (signal) => {
        await fetchAndDownload(
            `${API}/forecast/${sid}/export.xlsx?horizon_days=${horizon}`,
            signal,
            `forecast_source${sid}_h${horizon}d.xlsx`
        );
    });
}

// ═══════════════════════════════════════════════════
// SIDEBAR RESPONSIVE (issue #124)
// ═══════════════════════════════════════════════════
(function initSidebarResponsive() {
    // localStorage helpers — gracefully no-op in private mode / when blocked.
    function lsGet(k, fallback) {
        try { const v = localStorage.getItem(k); return v == null ? fallback : v; }
        catch (e) { return fallback; }
    }
    function lsSet(k, v) {
        try { localStorage.setItem(k, v); } catch (e) { /* private mode */ }
    }
    function lsGetJSON(k, fallback) {
        try { const v = localStorage.getItem(k); return v == null ? fallback : JSON.parse(v); }
        catch (e) { return fallback; }
    }
    function lsSetJSON(k, v) { lsSet(k, JSON.stringify(v)); }

    // Turkish ASCII fold for case-insensitive search.
    const TR_MAP = { 'ç':'c','Ç':'c','ğ':'g','Ğ':'g','ı':'i','İ':'i','ö':'o','Ö':'o','ş':'s','Ş':'s','ü':'u','Ü':'u' };
    function fold(s) {
        if (!s) return '';
        let out = '';
        for (const ch of s) out += TR_MAP[ch] || ch.toLowerCase();
        return out;
    }

    const KEY_COLLAPSED = 'sidebar.collapsed_groups';
    const KEY_NARROW = 'sidebar.narrow';
    // Default-collapsed groups for first-time visitors (less common ones).
    const DEFAULT_COLLAPSED = ['Entegrasyonlar', 'Sistem'];

    const sidebar = document.getElementById('sidebar');
    if (!sidebar) return;
    const navContent = document.getElementById('nav-content');
    const searchInput = document.getElementById('sidebar-search');
    const narrowToggle = document.getElementById('sidebar-narrow-toggle');
    const hamburger = document.getElementById('sidebar-hamburger');
    const backdrop = document.getElementById('sidebar-backdrop');

    // ---- Group collapse state ----
    function getStoredCollapsed() {
        const raw = lsGetJSON(KEY_COLLAPSED, null);
        if (Array.isArray(raw)) return new Set(raw);
        return new Set(DEFAULT_COLLAPSED);
    }
    let collapsedGroups = getStoredCollapsed();

    function saveCollapsed() {
        lsSetJSON(KEY_COLLAPSED, Array.from(collapsedGroups));
    }

    function applyCollapsedState() {
        document.querySelectorAll('.nav-section[data-group]').forEach(sec => {
            const g = sec.getAttribute('data-group');
            if (collapsedGroups.has(g)) sec.classList.add('collapsed');
            else sec.classList.remove('collapsed');
        });
    }

    function expandActiveGroup() {
        const activeItem = document.querySelector('.nav-item.active');
        if (!activeItem) return;
        const sec = activeItem.closest('.nav-section[data-group]');
        if (!sec) return;
        const g = sec.getAttribute('data-group');
        if (collapsedGroups.has(g)) {
            collapsedGroups.delete(g);
            saveCollapsed();
            sec.classList.remove('collapsed');
        }
    }

    // Click on nav-label toggles collapse.
    document.querySelectorAll('.nav-section[data-group] .nav-label').forEach(lbl => {
        lbl.addEventListener('click', () => {
            const sec = lbl.parentElement;
            const g = sec.getAttribute('data-group');
            if (sec.classList.toggle('collapsed')) collapsedGroups.add(g);
            else collapsedGroups.delete(g);
            saveCollapsed();
        });
    });

    applyCollapsedState();
    expandActiveGroup();

    // ---- Search ----
    function applySearch(query) {
        const q = fold(query.trim());
        const sections = document.querySelectorAll('.nav-section[data-group]');
        sections.forEach(sec => {
            let anyVisible = false;
            sec.querySelectorAll('.nav-item').forEach(item => {
                if (!q) {
                    item.classList.remove('search-hidden');
                    anyVisible = true;
                    return;
                }
                const txt = fold(item.textContent || '');
                if (txt.indexOf(q) !== -1) {
                    item.classList.remove('search-hidden');
                    anyVisible = true;
                } else {
                    item.classList.add('search-hidden');
                }
            });
            if (q && !anyVisible) sec.classList.add('search-empty');
            else sec.classList.remove('search-empty');
            // While searching, force-expand groups so matches show.
            if (q) sec.classList.remove('collapsed');
            else if (collapsedGroups.has(sec.getAttribute('data-group'))) sec.classList.add('collapsed');
        });
    }
    if (searchInput) {
        searchInput.addEventListener('input', e => applySearch(e.target.value));
        searchInput.addEventListener('keydown', e => {
            if (e.key === 'Escape') { searchInput.value = ''; applySearch(''); }
        });
    }

    // ---- Narrow mode ----
    function applyNarrow(on) {
        if (on) sidebar.classList.add('narrow');
        else sidebar.classList.remove('narrow');
    }
    applyNarrow(lsGet(KEY_NARROW, 'false') === 'true');
    if (narrowToggle) {
        narrowToggle.addEventListener('click', () => {
            const next = !sidebar.classList.contains('narrow');
            applyNarrow(next);
            lsSet(KEY_NARROW, String(next));
        });
    }

    // ---- Mobile hamburger ----
    function openMobile() {
        sidebar.classList.add('open');
        backdrop.classList.add('active');
        document.body.classList.add('sidebar-open');
    }
    function closeMobile() {
        sidebar.classList.remove('open');
        backdrop.classList.remove('active');
        document.body.classList.remove('sidebar-open');
    }
    if (hamburger) hamburger.addEventListener('click', openMobile);
    if (backdrop) backdrop.addEventListener('click', closeMobile);

    // Close on nav-item click in mobile mode (ignore label clicks).
    document.querySelectorAll('.nav-item').forEach(item => {
        item.addEventListener('click', () => {
            if (window.matchMedia('(max-width: 768px)').matches) closeMobile();
        });
    });

    // ---- Hook into showPage so the active group auto-expands on navigation ----
    if (typeof window.showPage === 'function') {
        const _origShowPage = window.showPage;
        window.showPage = function (name) {
            const r = _origShowPage.apply(this, arguments);
            try { expandActiveGroup(); } catch (e) {}
            return r;
        };
    }
})();

// ============================================================
// Gorsel Duplikasyonlar (issue #144 Phase 2)
// ============================================================
let _imgdupScanId = null;

async function loadImageDuplicates() {
    const page = document.getElementById('page-image-duplicates');
    if (!page) return;

    const sourceEl = document.getElementById('imgdup-source');
    const hashType = document.getElementById('imgdup-hash-type')?.value || 'phash';
    const threshold = parseInt(document.getElementById('imgdup-threshold')?.value || '5', 10);
    const container = document.getElementById('imgdup-container');
    const kpiCards = document.getElementById('imgdup-kpi-cards');
    const xlsxBtn = document.getElementById('imgdup-xlsx-btn');
    const featureBanner = document.getElementById('imgdup-feature-banner');

    // Populate source dropdown once
    if (sourceEl && sourceEl.options.length <= 1) {
        try {
            const srcs = await api('/sources');
            (srcs.sources || []).forEach(s => {
                const o = document.createElement('option');
                o.value = s.id;
                o.textContent = s.name;
                sourceEl.appendChild(o);
            });
        } catch (e) { /* ignore */ }
    }

    // Show feature banner based on flag
    if (featureBanner) {
        try {
            const flags = await api('/security/feature-flags');
            featureBanner.style.display = flags?.image_duplicates?.enabled ? 'none' : 'block';
        } catch (e) { featureBanner.style.display = 'none'; }
    }

    const sourceId = sourceEl?.value;

    // Resolve scan_id for selected source
    let scanId = null;
    if (sourceId) {
        try {
            const status = await api(`/reports/status/${sourceId}`);
            scanId = status?.latest_scan_id || null;
        } catch (e) { /* ignore */ }
    }
    _imgdupScanId = scanId;

    const qp = new URLSearchParams({ hash_type: hashType, max_distance: threshold });
    if (scanId) qp.set('scan_id', scanId);

    if (container) container.innerHTML = '<div style="color:var(--text-muted);padding:20px">Yukleniyor...</div>';

    try {
        const data = await api(`/security/image-duplicates?${qp}`);

        // KPI cards
        if (kpiCards) {
            kpiCards.innerHTML = `
                <div class="card"><div class="card-label">Taranan Gorsel</div><div class="card-value">${data.total_images ?? 0}</div></div>
                <div class="card"><div class="card-label">Duplikasyon Grubu</div><div class="card-value">${(data.groups || []).length}</div></div>
                <div class="card"><div class="card-label">Hash Turu</div><div class="card-value">${(data.hash_type || 'phash').toUpperCase()}</div></div>
                <div class="card"><div class="card-label">Esik (Hamming)</div><div class="card-value">${data.max_distance ?? 5}</div></div>
            `;
        }

        if (xlsxBtn) xlsxBtn.style.display = (data.groups || []).length > 0 ? '' : 'none';

        // Groups table
        if (container) {
            const groups = data.groups || [];
            if (!groups.length) {
                container.innerHTML = '<div style="color:var(--text-muted);padding:20px">Duplikasyon bulunamadi.</div>';
                return;
            }
            let html = '<table class="data-table"><thead><tr><th>Grup</th><th>Hash</th><th>Dosya Yolu</th><th>Boyut</th></tr></thead><tbody>';
            groups.forEach((g, gi) => {
                (g.files || []).forEach((f, fi) => {
                    const rowClass = gi % 2 === 0 ? '' : ' style="background:rgba(255,255,255,0.03)"';
                    html += `<tr${rowClass}>`;
                    if (fi === 0) html += `<td rowspan="${g.files.length}" style="vertical-align:top;font-weight:600">${gi + 1}</td>`;
                    html += `<td style="font-family:monospace;font-size:11px">${(g.hash || '').slice(0, 12)}…</td>`;
                    html += `<td style="font-size:12px;word-break:break-all">${_esc(f.file_path || '')}</td>`;
                    html += `<td style="white-space:nowrap">${_fmtSize(f.file_size || 0)}</td>`;
                    html += '</tr>';
                });
            });
            html += '</tbody></table>';
            container.innerHTML = html;
        }
    } catch (e) {
        if (container) container.innerHTML = `<div style="color:var(--danger);padding:20px">Hata: ${e.message || e}</div>`;
    }
}

async function exportImageDuplicatesXlsx(evt) {
    if (evt) evt.preventDefault();
    const hashType = document.getElementById('imgdup-hash-type')?.value || 'phash';
    const threshold = document.getElementById('imgdup-threshold')?.value || '5';
    const qp = new URLSearchParams({ hash_type: hashType, max_distance: threshold });
    if (_imgdupScanId) qp.set('scan_id', _imgdupScanId);
    const btn = document.getElementById('imgdup-xlsx-btn');
    const origText = btn?.textContent;
    if (btn) btn.textContent = 'Hazirlaniyor...';
    try {
        await fetchAndDownload(`/api/security/image-duplicates/export.xlsx?${qp}`, null, 'image_duplicates.xlsx');
    } finally {
        if (btn && origText) btn.textContent = origText;
    }
}

function _fmtSize(bytes) {
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1048576) return (bytes / 1024).toFixed(1) + ' KB';
    if (bytes < 1073741824) return (bytes / 1048576).toFixed(1) + ' MB';
    return (bytes / 1073741824).toFixed(1) + ' GB';
}


