/**
 * View-mode toggle helper (issue #84).
 *
 * Renders a small two-button switch (Görsel | Profesyonel) in the top-right of
 * a dashboard page header. Persists the user's choice in localStorage under
 * `viewMode.{pageKey}` and dispatches to the matching renderer.
 *
 * Pure vanilla JS; no framework dependency. The component does NOT own the
 * page's data — callers supply `renderVisual` / `renderGrid` callbacks that
 * take care of the actual DOM mutations for each mode.
 *
 * Public API:
 *   attachViewToggle(pageContainer, options)
 *
 * options = {
 *   pageKey:       'duplicates',         // localStorage key suffix
 *   renderVisual:  () => {...},          // renders the chart / cards mode
 *   renderGrid:    () => {...},          // renders the entity-list mode
 *   defaultMode:   'visual',             // 'visual' | 'grid', default 'visual'
 *   labels:        {visual: 'Gorsel',    // optional localized labels
 *                   grid:   'Profesyonel'},
 * }
 *
 * Idempotent: calling attachViewToggle twice on the same page replaces the
 * previous toggle (no duplicate buttons accumulate when loadXxx() re-runs on
 * source-change). The toggle is anchored inside `.page-header .actions` if
 * present, otherwise it falls back to the first child of pageContainer.
 */
(function () {
    'use strict';

    var STORAGE_PREFIX = 'viewMode.';

    function _readMode(pageKey, defaultMode) {
        try {
            var v = window.localStorage.getItem(STORAGE_PREFIX + pageKey);
            if (v === 'visual' || v === 'grid') return v;
        } catch (_) { /* private mode / disabled storage */ }
        return defaultMode === 'grid' ? 'grid' : 'visual';
    }

    function _writeMode(pageKey, mode) {
        try {
            window.localStorage.setItem(STORAGE_PREFIX + pageKey, mode);
        } catch (_) { /* noop */ }
    }

    function _findAnchor(pageContainer) {
        // Preferred: page header's .actions block (right-aligned by default).
        var actions = pageContainer.querySelector('.page-header .actions');
        if (actions) return actions;
        // Fallback: page header itself, or the container's first child.
        var header = pageContainer.querySelector('.page-header');
        return header || pageContainer;
    }

    function attachViewToggle(pageContainer, options) {
        if (!pageContainer) {
            throw new Error('attachViewToggle: pageContainer is required');
        }
        var opts = options || {};
        var pageKey = opts.pageKey;
        if (!pageKey) {
            throw new Error('attachViewToggle: options.pageKey is required');
        }
        var renderVisual = typeof opts.renderVisual === 'function'
            ? opts.renderVisual : null;
        var renderGrid = typeof opts.renderGrid === 'function'
            ? opts.renderGrid : null;
        if (!renderVisual || !renderGrid) {
            throw new Error('attachViewToggle: renderVisual and renderGrid required');
        }
        var labels = opts.labels || {};
        var visualLabel = labels.visual || 'Gorsel';
        var gridLabel = labels.grid || 'Profesyonel';
        var defaultMode = opts.defaultMode === 'grid' ? 'grid' : 'visual';

        var anchor = _findAnchor(pageContainer);

        // Idempotent: drop any existing toggle for this pageKey first.
        var existing = pageContainer.querySelector(
            '.view-toggle[data-page-key="' + pageKey + '"]'
        );
        if (existing && existing.parentNode) {
            existing.parentNode.removeChild(existing);
        }

        var wrap = document.createElement('div');
        wrap.className = 'view-toggle';
        wrap.setAttribute('data-page-key', pageKey);
        wrap.setAttribute('role', 'group');
        wrap.setAttribute('aria-label', 'Goruntu modu');

        var btnVisual = document.createElement('button');
        btnVisual.type = 'button';
        btnVisual.className = 'vt-btn vt-visual';
        btnVisual.setAttribute('data-mode', 'visual');
        btnVisual.textContent = visualLabel;

        var btnGrid = document.createElement('button');
        btnGrid.type = 'button';
        btnGrid.className = 'vt-btn vt-grid';
        btnGrid.setAttribute('data-mode', 'grid');
        btnGrid.textContent = gridLabel;

        wrap.appendChild(btnVisual);
        wrap.appendChild(btnGrid);

        // Insert as the FIRST child of the actions block so the toggle is the
        // left-most element in the header's right-side cluster — visually
        // consistent across pages regardless of how many other buttons exist.
        if (anchor.firstChild) {
            anchor.insertBefore(wrap, anchor.firstChild);
        } else {
            anchor.appendChild(wrap);
        }

        var currentMode = _readMode(pageKey, defaultMode);

        function _apply(mode, persist) {
            currentMode = (mode === 'grid') ? 'grid' : 'visual';
            btnVisual.classList.toggle('active', currentMode === 'visual');
            btnGrid.classList.toggle('active', currentMode === 'grid');
            if (persist) _writeMode(pageKey, currentMode);
            try {
                if (currentMode === 'grid') renderGrid();
                else renderVisual();
            } catch (e) {
                // Surface the failure but don't break the rest of the page.
                if (typeof window.notify === 'function') {
                    window.notify('Goruntu modu degistirilemedi: ' +
                        (e && e.message ? e.message : e), 'error');
                } else {
                    console.error('[view-toggle] render failed', e);
                }
            }
        }

        btnVisual.addEventListener('click', function () {
            if (currentMode === 'visual') return;
            _apply('visual', true);
        });
        btnGrid.addEventListener('click', function () {
            if (currentMode === 'grid') return;
            _apply('grid', true);
        });

        // Initial render — do NOT persist on first paint; that way refreshing
        // a page without ever toggling keeps the user's previous choice (or
        // the page's default) untouched in storage.
        _apply(currentMode, false);

        return {
            getMode: function () { return currentMode; },
            setMode: function (mode) { _apply(mode, true); },
            destroy: function () {
                if (wrap.parentNode) wrap.parentNode.removeChild(wrap);
            },
        };
    }

    window.attachViewToggle = attachViewToggle;
})();
