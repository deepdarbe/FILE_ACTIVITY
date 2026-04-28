/**
 * Reusable entity-list component (issue #80).
 *
 * Vanilla JS, no framework. Renders a multi-select, sortable, paginated,
 * filterable table with toolbar (search + bulk actions + XLSX/CSV export).
 *
 * Public API:
 *   renderEntityList(container, options)
 *
 * options = {
 *   rows:        [...],                    // pre-fetched data
 *   columns:     [
 *     {key: 'file_path', label: 'Dosya', sort: true},
 *     {key: 'severity',  label: 'Onem',  render: (val, row) => '<span>...</span>'},
 *   ],
 *   searchKeys:  ['file_path', 'rule'],    // optional; defaults to all column keys
 *   pageSize:    50,                       // optional, default 50
 *   rowKey:      'id',                     // optional; for stable selection across re-renders
 *   toolbar: {
 *     xlsxExport:  {endpoint: '/api/.../export.xlsx', filenameBase: 'naming-1'},
 *     csvExport:   {endpoint: '/api/.../export.csv',  filenameBase: 'naming-1'},
 *     bulkActions: [
 *       {label: 'Hedefe Git', action: 'open-folder'},
 *       {label: 'Toplu Arsivle', action: 'bulk-archive', confirm: true,
 *        comingSoon: 'Yakinda eklenecek (#83)'},
 *     ],
 *   },
 *   onBulkAction: async (actionId, selectedRows) => {...},
 *   emptyMessage: 'Sonuc yok',             // optional
 * }
 *
 * The component does NOT hardcode any column / endpoint / action — callers
 * must supply them. Bulk-action handlers are dispatched via onBulkAction;
 * actions flagged comingSoon emit a toast and skip the handler entirely
 * (used for #83 destructive ops that aren't implemented yet).
 */
(function () {
    'use strict';

    // ---- helpers --------------------------------------------------------

    function _escape(v) {
        if (v === null || v === undefined) return '';
        return String(v)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    function _toast(msg, type) {
        // Prefer the host page's notify() if present; otherwise fallback.
        if (typeof window.notify === 'function') {
            window.notify(msg, type || 'info');
        } else {
            // Fallback: console + alert-on-error.
            if (type === 'error') console.error(msg);
            else console.log('[entity-list]', msg);
        }
    }

    function _downloadUrl(url, filename) {
        const a = document.createElement('a');
        a.href = url;
        if (filename) a.download = filename;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
    }

    function _formatBytes(v) {
        const n = Number(v);
        if (!Number.isFinite(n) || n <= 0) return '0 B';
        const units = ['B', 'KB', 'MB', 'GB', 'TB'];
        let i = 0, x = n;
        while (x >= 1024 && i < units.length - 1) { x /= 1024; i++; }
        return (i === 0 ? x.toFixed(0) : x.toFixed(2)) + ' ' + units[i];
    }

    function _formatDateTime(v) {
        if (v === null || v === undefined || v === '') return '';
        // Already a YYYY-MM-DD HH:MM:SS string? Pass through.
        return String(v);
    }

    const _FORMATTERS = {
        bytes: _formatBytes,
        datetime: _formatDateTime,
    };

    // Lightweight modal helper. Returns a `close` function. The modal is
    // built from a body-string + footer buttons; the caller is responsible
    // for escaping any dynamic content before passing the body in.
    function _modal(title, bodyHtml, buttons) {
        const overlay = document.createElement('div');
        overlay.className = 'modal-overlay active';
        overlay.style.cssText =
            'position:fixed;inset:0;background:rgba(0,0,0,0.55);' +
            'display:flex;align-items:center;justify-content:center;' +
            'z-index:9999';
        const dlg = document.createElement('div');
        dlg.className = 'modal';
        dlg.style.cssText =
            'background:var(--bg-card,#1e293b);color:var(--text-primary,#e2e8f0);' +
            'border:1px solid var(--border,#334155);border-radius:8px;' +
            'max-width:600px;width:95%;padding:20px;box-shadow:0 8px 32px rgba(0,0,0,0.4)';
        dlg.innerHTML =
            '<h3 style="margin:0 0 12px 0;font-size:16px">' +
            _escape(title) +
            '</h3>' +
            '<div class="el-modal-body" style="font-size:13px;line-height:1.5;' +
            'max-height:50vh;overflow-y:auto;margin-bottom:16px">' +
            (bodyHtml || '') +
            '</div>' +
            '<div class="el-modal-buttons" style="display:flex;gap:8px;' +
            'justify-content:flex-end;flex-wrap:wrap"></div>';
        overlay.appendChild(dlg);
        document.body.appendChild(overlay);

        const close = function () {
            if (overlay.parentNode) overlay.parentNode.removeChild(overlay);
        };

        const btnHost = dlg.querySelector('.el-modal-buttons');
        (buttons || []).forEach(function (b) {
            const el = document.createElement('button');
            el.type = 'button';
            el.className = 'btn btn-sm ' + (b.className || 'btn-outline');
            el.textContent = b.label;
            el.addEventListener('click', function () {
                if (typeof b.onClick === 'function') {
                    Promise.resolve()
                        .then(function () { return b.onClick(close); })
                        .catch(function (e) {
                            _toast(
                                'Islem hatasi: ' +
                                    (e && e.message ? e.message : e),
                                'error',
                            );
                        });
                } else {
                    close();
                }
            });
            btnHost.appendChild(el);
        });

        // Click-outside dismiss.
        overlay.addEventListener('click', function (e) {
            if (e.target === overlay) close();
        });
        return close;
    }

    function _postJson(endpoint, body) {
        return fetch(endpoint, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(body || {}),
        }).then(function (resp) {
            return resp.text().then(function (txt) {
                let parsed = null;
                try { parsed = txt ? JSON.parse(txt) : null; } catch (e) {}
                if (!resp.ok) {
                    const msg = (parsed && (parsed.detail || parsed.message))
                        || ('HTTP ' + resp.status);
                    const err = new Error(msg);
                    err.status = resp.status;
                    err.body = parsed;
                    throw err;
                }
                return parsed;
            });
        });
    }

    function _matchesSearch(row, query, keys) {
        if (!query) return true;
        const q = query.toLowerCase();
        for (const k of keys) {
            const v = row[k];
            if (v === undefined || v === null) continue;
            if (String(v).toLowerCase().indexOf(q) !== -1) return true;
        }
        return false;
    }

    function _compareValues(a, b) {
        if (a === b) return 0;
        if (a === null || a === undefined) return -1;
        if (b === null || b === undefined) return 1;
        const an = Number(a), bn = Number(b);
        if (!Number.isNaN(an) && !Number.isNaN(bn) && a !== '' && b !== '') {
            return an - bn;
        }
        return String(a).localeCompare(String(b));
    }

    // ---- main -----------------------------------------------------------

    function renderEntityList(container, options) {
        if (!container) throw new Error('renderEntityList: container is required');
        const opts = options || {};
        const columns = Array.isArray(opts.columns) ? opts.columns : [];
        const rows = Array.isArray(opts.rows) ? opts.rows : [];
        const pageSize = Number(opts.pageSize) || 50;
        const rowKey = opts.rowKey || null;
        const toolbar = opts.toolbar || {};
        const onBulkAction = typeof opts.onBulkAction === 'function'
            ? opts.onBulkAction : null;
        const emptyMessage = opts.emptyMessage || 'Sonuc yok';
        const searchKeys = Array.isArray(opts.searchKeys) && opts.searchKeys.length
            ? opts.searchKeys
            : columns.map(function (c) { return c.key; });

        const state = {
            search: '',
            sortKey: null,
            sortDir: null,        // 'asc' | 'desc' | null
            page: 1,
            selected: new Set(),  // rowKey value, or numeric index when no rowKey
        };

        // Generate stable IDs for elements within this container so multiple
        // entity-lists on the same page do not collide.
        const ns = 'el-' + Math.random().toString(36).slice(2, 9);

        function _idFor(row, idx) {
            return rowKey ? row[rowKey] : idx;
        }

        function _filteredRows() {
            let list = rows;
            if (state.search) {
                list = list.filter(function (r) {
                    return _matchesSearch(r, state.search, searchKeys);
                });
            }
            if (state.sortKey && state.sortDir) {
                const k = state.sortKey;
                const dir = state.sortDir === 'desc' ? -1 : 1;
                list = list.slice().sort(function (a, b) {
                    return dir * _compareValues(a[k], b[k]);
                });
            }
            return list;
        }

        function _pagedRows(filtered) {
            const start = (state.page - 1) * pageSize;
            return filtered.slice(start, start + pageSize);
        }

        function _selectedRowObjects() {
            const sel = [];
            rows.forEach(function (r, i) {
                if (state.selected.has(_idFor(r, i))) sel.push(r);
            });
            return sel;
        }

        // ---- toolbar HTML ----------------------------------------------

        function _renderToolbar() {
            const parts = [];
            parts.push('<div class="' + ns + '-toolbar" style="display:flex;flex-wrap:wrap;gap:8px;align-items:center;padding:10px;background:var(--bg-secondary);border-bottom:1px solid var(--border)">');

            // search
            parts.push(
                '<input type="text" class="' + ns + '-search" placeholder="Ara..." ' +
                'value="' + _escape(state.search) + '" ' +
                'style="flex:1;min-width:160px;padding:6px 10px;background:var(--bg-input);border:1px solid var(--border);border-radius:6px;color:var(--text-primary);font-size:12px">'
            );

            // select all / clear
            parts.push(
                '<button class="btn btn-sm btn-outline ' + ns + '-select-all" type="button">Tumu Sec</button>' +
                '<button class="btn btn-sm btn-outline ' + ns + '-clear-sel" type="button">Secimi Kaldir</button>'
            );

            // selected badge
            parts.push(
                '<span class="' + ns + '-sel-count" style="font-size:12px;color:var(--text-secondary);padding:4px 10px;background:var(--bg-card);border-radius:12px">' +
                state.selected.size + ' secili</span>'
            );

            // exports
            if (toolbar.xlsxExport && toolbar.xlsxExport.endpoint) {
                parts.push('<button class="btn btn-sm btn-outline ' + ns + '-xlsx" type="button" title="XLSX olarak indir">XLSX</button>');
            }
            if (toolbar.csvExport && toolbar.csvExport.endpoint) {
                parts.push('<button class="btn btn-sm btn-outline ' + ns + '-csv" type="button" title="CSV olarak indir">CSV</button>');
            }

            // bulk actions
            const bulk = Array.isArray(toolbar.bulkActions) ? toolbar.bulkActions : [];
            bulk.forEach(function (a, i) {
                const cls = a.danger ? 'btn-danger' : 'btn-outline';
                parts.push(
                    '<button class="btn btn-sm ' + cls + ' ' + ns + '-bulk" data-bulk-idx="' + i + '" type="button">' +
                    _escape(a.label) +
                    '</button>'
                );
            });

            parts.push('</div>');
            return parts.join('');
        }

        // ---- table HTML ------------------------------------------------

        function _renderTable() {
            const filtered = _filteredRows();
            const total = filtered.length;
            const pageRows = _pagedRows(filtered);
            const totalPages = Math.max(1, Math.ceil(total / pageSize));
            if (state.page > totalPages) state.page = totalPages;

            const parts = [];
            parts.push('<div class="' + ns + '-table-wrap" style="overflow-x:auto">');
            parts.push('<table style="width:100%;font-size:12px"><thead><tr>');

            // header checkbox
            parts.push(
                '<th style="width:32px"><input type="checkbox" class="' + ns + '-header-chk"></th>'
            );

            columns.forEach(function (c) {
                const sortable = c.sort !== false; // default true
                const isSorted = state.sortKey === c.key;
                const arrow = isSorted ? (state.sortDir === 'asc' ? ' ▲' : (state.sortDir === 'desc' ? ' ▼' : '')) : '';
                parts.push(
                    '<th data-col-key="' + _escape(c.key) + '"' +
                    (sortable ? ' style="cursor:pointer;user-select:none" class="' + ns + '-sort-th"' : '') +
                    '>' + _escape(c.label || c.key) + arrow + '</th>'
                );
            });

            parts.push('</tr></thead><tbody>');

            if (pageRows.length === 0) {
                parts.push(
                    '<tr><td colspan="' + (columns.length + 1) + '" ' +
                    'style="text-align:center;padding:30px;color:var(--text-muted)">' +
                    _escape(emptyMessage) + '</td></tr>'
                );
            } else {
                pageRows.forEach(function (row) {
                    const idx = rows.indexOf(row);
                    const id = _idFor(row, idx);
                    const checked = state.selected.has(id) ? ' checked' : '';
                    parts.push('<tr data-row-idx="' + idx + '">');
                    parts.push(
                        '<td><input type="checkbox" class="' + ns + '-row-chk"' + checked + '></td>'
                    );
                    columns.forEach(function (c) {
                        let cell;
                        if (typeof c.render === 'function') {
                            try {
                                cell = c.render(row[c.key], row);
                            } catch (e) {
                                cell = '';
                            }
                            // render() may return raw HTML — caller's responsibility.
                            if (cell === undefined || cell === null) cell = '';
                        } else if (c.formatter && _FORMATTERS[c.formatter]) {
                            // Built-in formatter ('bytes', 'datetime', ...).
                            cell = _escape(_FORMATTERS[c.formatter](row[c.key]));
                        } else {
                            cell = _escape(row[c.key]);
                        }
                        parts.push('<td>' + cell + '</td>');
                    });
                    parts.push('</tr>');
                });
            }

            parts.push('</tbody></table></div>');

            // footer / pagination
            const start = total === 0 ? 0 : (state.page - 1) * pageSize + 1;
            const end = Math.min(state.page * pageSize, total);
            parts.push(
                '<div class="' + ns + '-foot" style="display:flex;justify-content:space-between;align-items:center;padding:8px 12px;font-size:12px;color:var(--text-secondary);border-top:1px solid var(--border);background:var(--bg-secondary)">' +
                '<span>' + start + '-' + end + ' / ' + total + ' (toplam ' + rows.length + ')</span>' +
                '<span>' +
                '<button class="btn btn-sm btn-outline ' + ns + '-prev" type="button"' + (state.page <= 1 ? ' disabled' : '') + '>Onceki</button>' +
                ' <span style="margin:0 8px">Sayfa ' + state.page + ' / ' + totalPages + '</span>' +
                '<button class="btn btn-sm btn-outline ' + ns + '-next" type="button"' + (state.page >= totalPages ? ' disabled' : '') + '>Sonraki</button>' +
                '</span></div>'
            );

            return parts.join('');
        }

        // ---- mount + bind ---------------------------------------------

        // Security audit 2026-04-28, finding H-1: this innerHTML is safe
        // because every leaf value spliced into _renderToolbar() /
        // _renderTable() flows through _escape(). The one explicit escape
        // hatch is column ``render`` callbacks (line ~253), which are
        // documented as the caller's responsibility — keep them returning
        // pre-escaped HTML.
        function _render() {
            container.innerHTML =
                '<div class="' + ns + '-root" style="background:var(--bg-card);border:1px solid var(--border);border-radius:var(--radius);overflow:hidden">' +
                _renderToolbar() +
                _renderTable() +
                '</div>';
            _bind();
        }

        function _bind() {
            // search
            const searchEl = container.querySelector('.' + ns + '-search');
            if (searchEl) {
                let t;
                searchEl.addEventListener('input', function () {
                    clearTimeout(t);
                    const v = this.value;
                    t = setTimeout(function () {
                        state.search = v;
                        state.page = 1;
                        _render();
                    }, 150);
                });
            }

            // select all visible (current page) / clear
            const selAllBtn = container.querySelector('.' + ns + '-select-all');
            if (selAllBtn) {
                selAllBtn.addEventListener('click', function () {
                    _filteredRows().forEach(function (r) {
                        state.selected.add(_idFor(r, rows.indexOf(r)));
                    });
                    _render();
                });
            }
            const clearBtn = container.querySelector('.' + ns + '-clear-sel');
            if (clearBtn) {
                clearBtn.addEventListener('click', function () {
                    state.selected.clear();
                    _render();
                });
            }

            // header checkbox: toggle visible page rows
            const headerChk = container.querySelector('.' + ns + '-header-chk');
            if (headerChk) {
                const visible = _pagedRows(_filteredRows());
                const allSel = visible.length > 0 && visible.every(function (r) {
                    return state.selected.has(_idFor(r, rows.indexOf(r)));
                });
                headerChk.checked = allSel;
                headerChk.addEventListener('change', function () {
                    visible.forEach(function (r) {
                        const id = _idFor(r, rows.indexOf(r));
                        if (headerChk.checked) state.selected.add(id);
                        else state.selected.delete(id);
                    });
                    _render();
                });
            }

            // per-row checkboxes
            container.querySelectorAll('.' + ns + '-row-chk').forEach(function (chk) {
                chk.addEventListener('change', function () {
                    const tr = chk.closest('tr');
                    const idx = Number(tr.getAttribute('data-row-idx'));
                    const id = _idFor(rows[idx], idx);
                    if (chk.checked) state.selected.add(id);
                    else state.selected.delete(id);
                    // light update without full re-render to keep inputs focused
                    const counter = container.querySelector('.' + ns + '-sel-count');
                    if (counter) counter.textContent = state.selected.size + ' secili';
                });
            });

            // sort headers
            container.querySelectorAll('.' + ns + '-sort-th').forEach(function (th) {
                th.addEventListener('click', function () {
                    const k = th.getAttribute('data-col-key');
                    if (state.sortKey !== k) {
                        state.sortKey = k;
                        state.sortDir = 'asc';
                    } else if (state.sortDir === 'asc') {
                        state.sortDir = 'desc';
                    } else if (state.sortDir === 'desc') {
                        state.sortKey = null;
                        state.sortDir = null;
                    } else {
                        state.sortDir = 'asc';
                    }
                    _render();
                });
            });

            // pagination
            const prev = container.querySelector('.' + ns + '-prev');
            if (prev) prev.addEventListener('click', function () {
                if (state.page > 1) { state.page--; _render(); }
            });
            const next = container.querySelector('.' + ns + '-next');
            if (next) next.addEventListener('click', function () {
                state.page++; _render();
            });

            // exports
            const xlsxBtn = container.querySelector('.' + ns + '-xlsx');
            if (xlsxBtn) xlsxBtn.addEventListener('click', function () {
                _doExport(toolbar.xlsxExport, 'xlsx');
            });
            const csvBtn = container.querySelector('.' + ns + '-csv');
            if (csvBtn) csvBtn.addEventListener('click', function () {
                _doExport(toolbar.csvExport, 'csv');
            });

            // bulk actions
            container.querySelectorAll('.' + ns + '-bulk').forEach(function (btn) {
                btn.addEventListener('click', function () {
                    const idx = Number(btn.getAttribute('data-bulk-idx'));
                    const action = (toolbar.bulkActions || [])[idx];
                    if (!action) return;
                    if (action.comingSoon) {
                        _toast(action.comingSoon, 'info');
                        return;
                    }
                    const selected = _selectedRowObjects();
                    if (selected.length === 0) {
                        _toast('Once en az bir satir secin', 'warning');
                        return;
                    }
                    // Issue #80: max-cap enforcement.
                    if (typeof action.max === 'number' && selected.length > action.max) {
                        _toast(
                            'Bu islem icin en fazla ' + action.max + ' satir secebilirsiniz ' +
                                '(secili: ' + selected.length + ')',
                            'warning'
                        );
                        return;
                    }
                    _runBulkAction(action, selected);
                });
            });
        }

        // Issue #80: dispatch a bulk action.
        //
        // Two flavours, picked by config:
        //   1. action.endpoint set      -> POST to that endpoint with the
        //      selected rows. If action.dryRun is true, we POST first with
        //      {dry_run: true} to fetch a preview, render the modal, then
        //      POST again with {dry_run: false, confirm: true} on confirm.
        //   2. action.endpoint missing  -> fall back to onBulkAction(actionId, rows).
        //
        // Every endpoint-driven action surfaces an audit-event banner so the
        // operator knows the call is logged.
        function _runBulkAction(action, selected) {
            const filePaths = selected
                .map(function (r) { return r.file_path || r.path; })
                .filter(function (p) { return !!p; });

            // Endpoint-driven action.
            if (action.endpoint) {
                const auditNote =
                    '<div style="background:var(--bg-secondary,#0f172a);border:1px solid var(--border,#334155);' +
                    'border-left:3px solid var(--info,#38bdf8);padding:8px 12px;border-radius:6px;' +
                    'font-size:12px;color:var(--text-secondary,#94a3b8);margin-bottom:12px">' +
                    'Bu islem audit log\'a yazilir.</div>';

                const _doConfirm = function (preview) {
                    let bodyHtml = auditNote;
                    if (preview && typeof preview === 'object') {
                        bodyHtml +=
                            '<p>' + _escape(action.label) + ': ' +
                            '<strong>' + selected.length + '</strong> satir secildi.</p>';
                        if (preview.matched != null) {
                            bodyHtml +=
                                '<div style="font-size:12px;color:var(--text-secondary,#94a3b8)">' +
                                'Eslesti: <strong>' + Number(preview.matched) + '</strong>' +
                                (preview.missing ? ' &middot; eksik: ' + Number(preview.missing) : '') +
                                (preview.total_size_formatted
                                    ? ' &middot; toplam: ' + _escape(preview.total_size_formatted)
                                    : '') +
                                '</div>';
                        }
                    } else {
                        bodyHtml +=
                            '<p>' + _escape(action.label) + ': ' +
                            '<strong>' + selected.length + '</strong> satir icin ' +
                            'onayliyor musunuz?</p>';
                    }
                    _modal(
                        action.label + ' - Onay',
                        bodyHtml,
                        [
                            {label: 'Vazgec', className: 'btn-outline'},
                            {
                                label: 'Onayla',
                                className: action.danger ? 'btn-danger' : 'btn-primary',
                                onClick: function (close) {
                                    return _postJson(action.endpoint, {
                                        file_paths: filePaths,
                                        paths: filePaths,
                                        confirm: true,
                                        dry_run: false,
                                    }).then(function (resp) {
                                        close();
                                        _toast(
                                            action.label + ' tamamlandi' +
                                                (resp && resp.archived != null
                                                    ? ' (' + resp.archived + ' dosya)' : ''),
                                            'success'
                                        );
                                        state.selected.clear();
                                        _render();
                                    });
                                },
                            },
                        ]
                    );
                };

                if (action.dryRun) {
                    // Stage 1: fetch preview.
                    _postJson(action.endpoint, {
                        file_paths: filePaths,
                        paths: filePaths,
                        dry_run: true,
                    }).then(function (preview) {
                        _doConfirm(preview);
                    }).catch(function (e) {
                        _toast(
                            'On izleme hatasi: ' + (e && e.message ? e.message : e),
                            'error'
                        );
                    });
                } else if (action.confirmRequired) {
                    _doConfirm(null);
                } else {
                    // Fire and forget (still POSTs with confirm=true so the
                    // server's confirm gate is satisfied).
                    _postJson(action.endpoint, {
                        file_paths: filePaths,
                        paths: filePaths,
                        confirm: true,
                        dry_run: false,
                    }).then(function () {
                        _toast(action.label + ' tamamlandi', 'success');
                        state.selected.clear();
                        _render();
                    }).catch(function (e) {
                        _toast(
                            'Toplu islem hatasi: ' +
                                (e && e.message ? e.message : e),
                            'error'
                        );
                    });
                }
                return;
            }

            // Legacy path: defer to onBulkAction.
            if (action.confirm) {
                const msg = typeof action.confirm === 'string'
                    ? action.confirm
                    : (action.label + ': ' + selected.length + ' satir icin onayliyor musunuz?');
                if (!window.confirm(msg)) return;
            }
            if (onBulkAction) {
                Promise.resolve()
                    .then(function () { return onBulkAction(action.action || action.id, selected); })
                    .catch(function (e) {
                        _toast(
                            'Toplu islem hatasi: ' +
                                (e && e.message ? e.message : e),
                            'error'
                        );
                    });
            }
        }

        function _doExport(exportCfg, kind) {
            if (!exportCfg || !exportCfg.endpoint) return;
            const ids = [];
            if (state.selected.size > 0) {
                state.selected.forEach(function (v) { ids.push(v); });
            }
            let url = exportCfg.endpoint;
            if (ids.length > 0) {
                const sep = url.indexOf('?') === -1 ? '?' : '&';
                url = url + sep + 'ids=' + encodeURIComponent(ids.join(','));
            }
            const base = exportCfg.filenameBase || 'export';
            const ts = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
            const filename = base + '_' + ts + '.' + kind;
            _downloadUrl(url, filename);
            _toast(kind.toUpperCase() + ' indirme baslatildi', 'info');
        }

        _render();

        // Return a small handle so callers can re-render or inspect state.
        return {
            getSelected: _selectedRowObjects,
            clearSelection: function () { state.selected.clear(); _render(); },
            rerender: _render,
        };
    }

    // expose globally — index.html includes this file with a plain <script>
    window.renderEntityList = renderEntityList;
})();
