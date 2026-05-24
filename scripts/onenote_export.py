"""Export Markdown session-flow notes to OneNote via Microsoft Graph.

Why this exists
---------------
We keep the session "flow" (handoff state, what shipped, what's pending) in
Markdown -- ``CLAUDE.md`` (the SESSION HANDOFF block), ``ROADMAP.md``, or a
dedicated per-session log. This script pushes any such Markdown file into a
OneNote notebook so the running record lives somewhere searchable outside the
repo.

It talks to the Microsoft Graph OneNote API directly over the stdlib
``urllib`` (no new runtime dependency). A page is created with::

    POST https://graph.microsoft.com/v1.0/me/onenote/pages?sectionName=<name>

Graph auto-creates the section if it doesn't already exist, so no
notebook/section ID lookup is needed for the common case. Target an explicit
section instead with ``--section-id`` (get IDs from ``--list``).

Authentication
--------------
OneNote page creation needs a *delegated* Graph access token carrying one of
``Notes.Create`` / ``Notes.ReadWrite`` / ``Notes.ReadWrite.All``. This script
does NOT mint tokens -- supply one you already have::

    export ONENOTE_GRAPH_TOKEN="eyJ0eXAi..."      # or pass --token

Quick ways to obtain a token for a manual run:
  * Graph Explorer (https://developer.microsoft.com/graph/graph-explorer):
    sign in, consent to ``Notes.Create``, copy the access token.
  * ``az account get-access-token --resource https://graph.microsoft.com``
    (when the signed-in az identity already has the scope).
  * An MSAL device-code flow in your own wrapper.

Tokens are short-lived (~1h). For unattended/scheduled use, wrap this script
in one that refreshes a token first (app-only/client-credentials needs
``Notes.ReadWrite.All`` + admin consent and the ``/users/{id}/onenote`` path
rather than ``/me``).

Usage
-----
    # Smoke-test the token + list notebooks/sections you can write to
    python scripts/onenote_export.py --list

    # Build the page locally and print it -- no token, no network write
    python scripts/onenote_export.py --file ROADMAP.md --dry-run

    # Create a page from a Markdown file in a (possibly new) section
    python scripts/onenote_export.py --file SESSION_LOG.md \\
        --section "FILE_ACTIVITY Sessions" --title "Session 2026-05-24"

    # Pipe arbitrary content in (verbatim, no Markdown parsing)
    git log --oneline -20 | python scripts/onenote_export.py --stdin \\
        --title "Last 20 commits" --format text

Markdown rendering uses the ``markdown`` package when installed (best
fidelity: ``pip install markdown``); otherwise a built-in converter handles
the common subset OneNote supports (headings, lists, tables, fenced code,
bold/italic, inline code, links). ``--format text`` emits the input verbatim
inside a <pre> block with no parsing.
"""
from __future__ import annotations

import argparse
import html
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from urllib.parse import quote
from typing import Iterable

DEFAULT_GRAPH_BASE = "https://graph.microsoft.com/v1.0"
DEFAULT_SECTION = "FILE_ACTIVITY Sessions"

# Characters Graph forbids in a OneNote section name (the sectionName route
# creates the section if missing, so we reject these up front for a clear
# error instead of a 400 from the service).
_BAD_SECTION_CHARS = set('?*\\/:<>|&#"%~')


# --------------------------------------------------------------------------
# Markdown -> OneNote-flavoured XHTML
# --------------------------------------------------------------------------
# OneNote accepts only a subset of HTML (h1-h6, p, ul/ol/li, table/tr/td,
# pre, b/i/u, a, br, hr, img). Input must be well-formed XHTML: container
# tags closed, void tags self-closed, attribute values quoted. We escape
# first, then re-introduce only the supported tags.

_CODE_SPAN = re.compile(r"`([^`]+)`")
_LINK = re.compile(r"\[([^\]]+)\]\((\S+?)(?:\s+\"[^\"]*\")?\)")
_HEADING = re.compile(r"^(#{1,6})\s+(.*?)\s*#*\s*$")
_HR = re.compile(r"^(-{3,}|\*{3,}|_{3,})$")
_LIST_ITEM = re.compile(r"^(\s*)([-*+]|\d+[.)])\s+(.*)$")
_TABLE_SEP = re.compile(r"^\s*\|?\s*:?-{1,}:?\s*(\|\s*:?-{1,}:?\s*)+\|?\s*$")


def _emph(text: str) -> str:
    """Apply bold/italic to already-escaped text."""
    text = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", text)
    text = re.sub(r"__(.+?)__", r"<b>\1</b>", text)
    text = re.sub(r"(?<![\*\w])\*(?!\s)(.+?)(?<!\s)\*(?!\*)", r"<i>\1</i>", text)
    text = re.sub(r"(?<![_\w])_(?!\s)(.+?)(?<!\s)_(?!\w)", r"<i>\1</i>", text)
    return text


def _inline(text: str) -> str:
    """Convert inline Markdown in one line/cell to OneNote XHTML."""
    code_spans: list[str] = []
    links: list[str] = []

    def _stash_code(m: re.Match) -> str:
        code_spans.append(m.group(1))
        return f"\x00C{len(code_spans) - 1}\x00"

    text = _CODE_SPAN.sub(_stash_code, text)
    text = html.escape(text, quote=True)

    def _stash_link(m: re.Match) -> str:
        label = _emph(m.group(1))
        href = m.group(2)
        links.append(f'<a href="{href}">{label}</a>')
        return f"\x00L{len(links) - 1}\x00"

    text = _LINK.sub(_stash_link, text)
    text = _emph(text)

    text = re.sub(
        r"\x00L(\d+)\x00", lambda m: links[int(m.group(1))], text
    )
    text = re.sub(
        r"\x00C(\d+)\x00",
        lambda m: '<span style="font-family:Consolas,monospace">'
        f"{html.escape(code_spans[int(m.group(1))], quote=True)}</span>",
        text,
    )
    return text


def _render_list(items: list[tuple[int, bool, str]]) -> str:
    """items: (indent, ordered, inline_html). Build nested ul/ol."""
    out: list[str] = []
    stack: list[tuple[int, str]] = []  # (indent, tag)
    for indent, ordered, text in items:
        tag = "ol" if ordered else "ul"
        while stack and indent < stack[-1][0]:
            out.append(f"</li></{stack.pop()[1]}>")
        if stack and indent == stack[-1][0]:
            out.append("</li>")
        elif stack and indent > stack[-1][0]:
            out.append(f"<{tag}>")
            stack.append((indent, tag))
        if not stack:
            out.append(f"<{tag}>")
            stack.append((indent, tag))
        out.append(f"<li>{text}")
    while stack:
        out.append(f"</li></{stack.pop()[1]}>")
    return "".join(out)


def _split_table_row(line: str) -> list[str]:
    line = line.strip()
    if line.startswith("|"):
        line = line[1:]
    if line.endswith("|"):
        line = line[:-1]
    return [c.strip() for c in line.split("|")]


def _md_to_html_builtin(md_text: str) -> str:
    lines = md_text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    out: list[str] = []
    para: list[str] = []
    i = 0
    n = len(lines)

    def flush_para() -> None:
        if para:
            out.append(f"<p>{_inline(' '.join(para))}</p>")
            para.clear()

    while i < n:
        line = lines[i]
        stripped = line.strip()

        # Fenced code block
        if stripped.startswith("```") or stripped.startswith("~~~"):
            flush_para()
            fence = stripped[:3]
            i += 1
            code: list[str] = []
            while i < n and lines[i].strip()[:3] != fence:
                code.append(lines[i])
                i += 1
            i += 1  # skip closing fence
            out.append(f"<pre>{html.escape(chr(10).join(code), quote=True)}</pre>")
            continue

        # Blank line -> paragraph break
        if not stripped:
            flush_para()
            i += 1
            continue

        # Heading
        m = _HEADING.match(stripped)
        if m:
            flush_para()
            level = len(m.group(1))
            out.append(f"<h{level}>{_inline(m.group(2))}</h{level}>")
            i += 1
            continue

        # Horizontal rule
        if _HR.match(stripped):
            flush_para()
            out.append("<hr/>")
            i += 1
            continue

        # Table (header row followed by a separator row)
        if "|" in line and i + 1 < n and _TABLE_SEP.match(lines[i + 1]):
            flush_para()
            header = _split_table_row(line)
            i += 2
            rows: list[list[str]] = []
            while i < n and "|" in lines[i] and lines[i].strip():
                rows.append(_split_table_row(lines[i]))
                i += 1
            cells = "".join(f"<td><b>{_inline(c)}</b></td>" for c in header)
            tbl = [f"<table border=\"1\"><tr>{cells}</tr>"]
            for row in rows:
                tbl.append(
                    "<tr>" + "".join(f"<td>{_inline(c)}</td>" for c in row) + "</tr>"
                )
            tbl.append("</table>")
            out.append("".join(tbl))
            continue

        # Blockquote
        if stripped.startswith(">"):
            flush_para()
            quoted: list[str] = []
            while i < n and lines[i].strip().startswith(">"):
                quoted.append(lines[i].strip()[1:].strip())
                i += 1
            out.append(
                f'<p style="margin-left:18px;color:#555555">'
                f"{_inline(' '.join(quoted))}</p>"
            )
            continue

        # List
        if _LIST_ITEM.match(line):
            flush_para()
            items: list[tuple[int, bool, str]] = []
            while i < n and _LIST_ITEM.match(lines[i]):
                lm = _LIST_ITEM.match(lines[i])
                indent = len(lm.group(1).replace("\t", "    "))
                ordered = lm.group(2)[0].isdigit()
                items.append((indent, ordered, _inline(lm.group(3))))
                i += 1
            out.append(_render_list(items))
            continue

        # Plain paragraph text
        para.append(stripped)
        i += 1

    flush_para()
    return "\n".join(out)


def _md_to_html(md_text: str, fmt: str) -> str:
    if fmt == "text":
        return f"<pre>{html.escape(md_text, quote=True)}</pre>"
    try:
        import markdown as _markdown  # type: ignore
    except ImportError:
        return _md_to_html_builtin(md_text)
    return _markdown.markdown(
        md_text, extensions=["tables", "fenced_code", "sane_lists"]
    )


def _resolve_title(
    md_text: str, explicit: str | None, file_path: str | None
) -> tuple[str, str]:
    """Return (title, body_markdown). If no explicit title, lift the first
    H1 and strip it from the body so it isn't duplicated."""
    if explicit:
        return explicit, md_text
    lines = md_text.splitlines()
    for idx, ln in enumerate(lines):
        if ln.strip():
            m = re.match(r"#\s+(.*)$", ln.strip())
            if m:
                del lines[idx]
                return m.group(1).strip(), "\n".join(lines)
            break
    stem = Path(file_path).stem if file_path else "session"
    return f"{stem} — {time.strftime('%Y-%m-%d %H:%M')}", md_text


def _build_page(title: str, body_html: str) -> str:
    created = time.strftime("%Y-%m-%dT%H:%M:%S%z") or time.strftime(
        "%Y-%m-%dT%H:%M:%S"
    )
    return (
        "<!DOCTYPE html>\n"
        "<html>\n"
        "  <head>\n"
        f"    <title>{html.escape(title, quote=True)}</title>\n"
        f'    <meta name="created" content="{created}" />\n'
        "  </head>\n"
        f"  <body>\n{body_html}\n  </body>\n"
        "</html>\n"
    )


# --------------------------------------------------------------------------
# Microsoft Graph
# --------------------------------------------------------------------------
def _graph_request(
    method: str,
    url: str,
    token: str,
    *,
    body: bytes | str | None = None,
    content_type: str | None = None,
    timeout: float = 60.0,
    max_attempts: int = 3,
) -> tuple[int, bytes]:
    """Issue a Graph request with backoff on 429/5xx and transient network
    errors. Returns (status_code, response_body)."""
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    if content_type:
        headers["Content-Type"] = content_type
    data = body.encode("utf-8") if isinstance(body, str) else body

    attempt = 0
    while True:
        attempt += 1
        req = urllib.request.Request(url, data=data, method=method, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.status, resp.read()
        except urllib.error.HTTPError as e:
            err_body = e.read()
            if e.code in (429, 500, 502, 503, 504) and attempt < max_attempts:
                retry_after = e.headers.get("Retry-After")
                wait = (
                    float(retry_after)
                    if retry_after and retry_after.isdigit()
                    else float(2 ** attempt)
                )
                print(
                    f"  Graph {e.code}; retry {attempt}/{max_attempts - 1} "
                    f"in {wait:.0f}s",
                    file=sys.stderr,
                )
                time.sleep(wait)
                continue
            return e.code, err_body
        except urllib.error.URLError as e:
            if attempt < max_attempts:
                wait = float(2 ** attempt)
                print(
                    f"  network error ({e.reason}); retry in {wait:.0f}s",
                    file=sys.stderr,
                )
                time.sleep(wait)
                continue
            raise


def _format_graph_error(status: int, body: bytes) -> str:
    try:
        payload = json.loads(body.decode("utf-8"))
        err = payload.get("error", {})
        return f"HTTP {status}: {err.get('code', '?')} — {err.get('message', body[:300])}"
    except Exception:
        return f"HTTP {status}: {body[:300].decode('utf-8', 'replace')}"


def _list_targets(graph_base: str, token: str) -> int:
    for kind, path in (
        ("Notebooks", "/me/onenote/notebooks?$select=id,displayName"),
        ("Sections", "/me/onenote/sections?$select=id,displayName"),
    ):
        status, body = _graph_request("GET", graph_base + path, token)
        print(f"\n{kind}:")
        if status != 200:
            print(f"  {_format_graph_error(status, body)}")
            if status in (401, 403):
                return 2
            continue
        items = json.loads(body.decode("utf-8")).get("value", [])
        if not items:
            print("  (none)")
        for it in items:
            print(f"  {it.get('displayName', '?'):<40} {it.get('id', '')}")
    print()
    return 0


def _create_page(
    graph_base: str,
    token: str,
    *,
    section: str | None,
    section_id: str | None,
    html_doc: str,
) -> int:
    if section_id:
        url = f"{graph_base}/me/onenote/sections/{quote(section_id, safe='')}/pages"
    else:
        url = f"{graph_base}/me/onenote/pages?sectionName={quote(section, safe='')}"
    status, body = _graph_request(
        "POST", url, token, body=html_doc, content_type="text/html; charset=utf-8"
    )
    if status == 201:
        payload = json.loads(body.decode("utf-8"))
        links = payload.get("links", {})
        web = links.get("oneNoteWebUrl", {}).get("href", "")
        client = links.get("oneNoteClientUrl", {}).get("href", "")
        print(f"Created page: {payload.get('title', '(untitled)')}")
        print(f"  id:     {payload.get('id', '')}")
        if web:
            print(f"  web:    {web}")
        if client:
            print(f"  client: {client}")
        return 0
    print(_format_graph_error(status, body), file=sys.stderr)
    return 1


def main(argv: Iterable[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    src = ap.add_mutually_exclusive_group()
    src.add_argument("--file", help="Markdown file to export as a OneNote page.")
    src.add_argument(
        "--stdin", action="store_true", help="Read page content from stdin."
    )
    ap.add_argument("--title", help="Page title (default: first H1, else file+date).")
    ap.add_argument(
        "--section",
        default=DEFAULT_SECTION,
        help=f'Target section in the default notebook; auto-created if missing '
        f'(default: "{DEFAULT_SECTION}").',
    )
    ap.add_argument(
        "--section-id",
        help="Target an existing section by ID (overrides --section). See --list.",
    )
    ap.add_argument(
        "--format",
        choices=["markdown", "text"],
        default="markdown",
        help="markdown = parse to OneNote HTML; text = verbatim <pre> (default: markdown).",
    )
    ap.add_argument(
        "--token",
        help="Graph access token. Falls back to $ONENOTE_GRAPH_TOKEN.",
    )
    ap.add_argument(
        "--graph-base",
        default=DEFAULT_GRAPH_BASE,
        help=f"Graph API base URL (default: {DEFAULT_GRAPH_BASE}).",
    )
    ap.add_argument(
        "--list",
        action="store_true",
        help="List notebooks + sections you can write to, then exit (auth smoke test).",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Build and print the page HTML without calling Graph (no token needed).",
    )
    args = ap.parse_args(list(argv) if argv else None)

    token = args.token or os.environ.get("ONENOTE_GRAPH_TOKEN", "").strip()
    if token.lower().startswith("bearer "):
        token = token[7:].strip()

    graph_base = args.graph_base.rstrip("/")

    if args.list:
        if not token:
            print(
                "error: --list needs a token (--token or $ONENOTE_GRAPH_TOKEN).",
                file=sys.stderr,
            )
            return 2
        return _list_targets(graph_base, token)

    # Validate section name early (sectionName route only).
    if not args.section_id:
        bad = _BAD_SECTION_CHARS & set(args.section)
        if bad:
            print(
                f"error: section name contains characters Graph forbids: "
                f"{''.join(sorted(bad))}",
                file=sys.stderr,
            )
            return 2

    # Gather content.
    if args.stdin:
        md_text = sys.stdin.read()
        file_path = None
    elif args.file:
        file_path = args.file
        try:
            md_text = Path(file_path).read_text(encoding="utf-8")
        except OSError as e:
            print(f"error: cannot read {file_path}: {e}", file=sys.stderr)
            return 2
    else:
        print(
            "error: provide --file PATH or --stdin (or --list).", file=sys.stderr
        )
        return 2

    if not md_text.strip():
        print("error: no content to export.", file=sys.stderr)
        return 2

    title, body_md = _resolve_title(md_text, args.title, file_path)
    body_html = _md_to_html(body_md, args.format)
    html_doc = _build_page(title, body_html)

    if args.dry_run:
        target = (
            f"section-id={args.section_id}"
            if args.section_id
            else f'section="{args.section}"'
        )
        print(f"--- DRY RUN ---  title={title!r}  {target}", file=sys.stderr)
        print(html_doc)
        return 0

    if not token:
        print(
            "error: no token. Set $ONENOTE_GRAPH_TOKEN or pass --token "
            "(needs Notes.Create / Notes.ReadWrite). See --dry-run to test offline.",
            file=sys.stderr,
        )
        return 2

    return _create_page(
        graph_base,
        token,
        section=args.section,
        section_id=args.section_id,
        html_doc=html_doc,
    )


if __name__ == "__main__":
    sys.exit(main())
