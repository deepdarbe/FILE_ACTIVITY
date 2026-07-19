"""Background worker that archives the FULL set of files matching an AI insight.

The interactive "Uygula" path caps at 10 000 files per click (``LIMIT 10000``).
This worker drains the entire matching set in batches for one ``archive_jobs``
row, moving files from the source share to the archive dest via
``ArchiveEngine.archive_files`` (Copy-Verify-Delete). It is designed around the
data-safety guardrails an adversarial review surfaced for a 1.7M-file / 11.3 TB
run:

- **No infinite loop / starvation (P0-#1):** progress is a *keyset cursor* on
  ``scanned_files.id`` (``fetch_insight_batch(after_id=...)``), so a file that
  fails to archive is stepped over, never re-selected forever.
- **Stall guard (P0-#1/#3):** a real batch that archives 0 with failures stops
  the job (poison file / disk-full) instead of looping.
- **Verified copy required (P0-#2):** real runs refuse to start unless
  ``archiving.verify_checksum`` is true.
- **Disk / reachability check (P0-#3):** real runs verify the dest has room for
  ``total_size`` before moving anything.
- **One snapshot per job (P1-#6):** taken once here; each batch passes
  ``skip_snapshot=True``.
- **WAL-safe (P0-#5):** the batch read cursor is drained + closed before
  archiving; the checkpointer is nudged between batches.
- **Fast cancel (P1-#7):** a cancel flag is honoured between batches (batch size
  is kept small in config).

The worker honours ``archiving.dry_run`` — a dry-run job is a safe no-op that
still exercises the whole pipeline (nothing is moved, ``archived_files`` stays
untouched), which is how the feature is validated before it is armed for real.
"""

from __future__ import annotations

import logging
import shutil
import threading
import time

logger = logging.getLogger("file_activity.archiver.job_worker")

# job_id -> (Thread, stop Event)
_JOB_THREADS: dict[int, tuple] = {}
_JOB_LOCK = threading.Lock()


def run_archive_job(db, config: dict, job_id: int, stop_event=None) -> None:
    """Drain one ``archive_jobs`` row to completion (or failure/cancel).

    Directly callable (no thread) for deterministic tests. ``config`` is the
    FULL app config (not ``db.config``, which is only the ``database:`` sub-dict)
    because the worker needs ``archiving`` / ``backup``.
    """
    job = db.get_archive_job(job_id)
    if not job or job["status"] in ("completed", "cancelled", "failed"):
        return

    arch_cfg = config.get("archiving") or {}
    bg = arch_cfg.get("background_jobs") or {}
    batch_size = int(bg.get("batch_size", 1000))
    sleep_s = float(bg.get("sleep_between_batches_seconds", 1.0))
    max_batches = int(bg.get("max_batches", 100000))
    min_free_margin = float(bg.get("min_free_margin", 1.05))
    dry_run = bool(job["dry_run"])

    from src.archiver.archive_engine import ArchiveEngine
    from src.archiver.insight_queries import insight_where

    try:
        where_frag, extra = insight_where(job["insight_type"])
    except ValueError as e:
        db.set_archive_job_status(job_id, "failed", error=str(e))
        return

    src = db.get_source_by_id(job["source_id"])
    if not src:
        db.set_archive_job_status(job_id, "failed", error="Kaynak bulunamadi")
        return

    # P0-#2: a real archive deletes the source after copy — never without a
    # checksum verification.
    if not dry_run and not arch_cfg.get("verify_checksum", True):
        db.set_archive_job_status(
            job_id, "failed",
            error="archiving.verify_checksum=true bulk gercek arsiv icin zorunlu")
        return

    engine = ArchiveEngine(db, config)
    db.set_archive_job_status(job_id, "running")

    # P1-#6: ONE snapshot for the whole job (real runs only). Failure never
    # aborts the job — same policy as the engine's per-call snapshot.
    if not dry_run:
        try:
            engine._maybe_pre_apply_snapshot(reason=f"pre-archive-job:{job_id}")
        except Exception as e:  # pragma: no cover - defensive
            logger.warning("job %s: snapshot failed (continuing): %s", job_id, e)

    # P0-#3: dest reachable + enough free space BEFORE moving 11 TB.
    if not dry_run:
        try:
            free = shutil.disk_usage(job["archive_dest"]).free
            need = int((job["total_size"] or 0) * min_free_margin)
            if free < need:
                db.set_archive_job_status(
                    job_id, "failed",
                    error=(f"Hedefte yer yok ({job['archive_dest']}): "
                           f"bos={free} gereken>={need}"))
                return
        except OSError as e:
            db.set_archive_job_status(
                job_id, "failed", error=f"Arsiv hedefi erisilemiyor: {e}")
            return

    after_id = int(job["last_file_id"] or 0)   # resume cursor (0 on a fresh job)
    batches = 0
    while True:
        # P1-#7: honour cancel between batches.
        if (stop_event is not None and stop_event.is_set()) \
                or db.is_archive_job_cancelled(job_id):
            db.set_archive_job_status(job_id, "cancelled")
            return

        rows = db.fetch_insight_batch(
            job["source_id"], job["scan_id"], where_frag, extra,
            after_id, batch_size)
        if not rows:                       # empty batch => the whole set is done
            db.set_archive_job_status(job_id, "completed")
            return

        try:
            result = engine.archive_files(
                rows, job["archive_dest"], src.unc_path, src.id,
                archived_by=f"ai_insight_all:{job['insight_type']}",
                dry_run=dry_run, trigger_type="ai_insight_all",
                trigger_detail=str(job_id), skip_snapshot=True)
        except Exception as e:
            logger.exception("job %s: batch raised", job_id)
            db.set_archive_job_status(
                job_id, "failed", error=f"Arsiv partisi hata verdi: {e}")
            return

        max_id = max(r["id"] for r in rows)   # keyset ALWAYS advances
        archived = int(result.get("archived", 0))
        failed = int(result.get("failed", 0))
        db.update_archive_job_progress(
            job_id, archived_delta=archived, failed_delta=failed,
            size_delta=int(result.get("total_size", 0)), last_file_id=max_id)
        after_id = max_id

        # P0-#1/#3: a real, non-empty batch that made zero progress means a
        # poison file at the head or a full/unreachable dest — stop, don't spin.
        if not dry_run and archived == 0 and failed > 0:
            db.set_archive_job_status(
                job_id, "failed",
                error=(f"Parti hic ilerlemedi ({failed} hata) — "
                       f"dongu onlemek icin durduruldu (zehirli dosya / disk dolu?)"))
            return

        batches += 1
        if batches >= max_batches:
            db.set_archive_job_status(
                job_id, "failed", error=f"max_batches ({max_batches}) asildi")
            return

        # P0-#5: nudge the manual checkpointer between batches (no open txn now)
        # so the WAL stays bounded across a multi-hour run.
        cp = getattr(db, "checkpointer", None)
        if cp is not None:
            try:
                cp.request()
            except Exception:  # pragma: no cover - defensive
                pass

        # politeness: back off while a live scan is writing (P2-#11)
        eff = sleep_s
        try:
            if batches % 10 == 0 and db._live_scan_running():
                eff = max(sleep_s, 5.0)
        except Exception:  # pragma: no cover - defensive
            pass
        if stop_event is not None:
            if stop_event.wait(eff):
                db.set_archive_job_status(job_id, "cancelled")
                return
        elif eff:
            time.sleep(eff)


def _guarded_run(db, config, job_id, stop_event) -> None:
    """Thread target: never leave a zombie 'running' row on a crash."""
    try:
        run_archive_job(db, config, job_id, stop_event=stop_event)
    except Exception as e:  # pragma: no cover - defensive
        logger.exception("archive job %s crashed", job_id)
        try:
            db.set_archive_job_status(job_id, "failed", error=f"worker crashed: {e}")
        except Exception:
            pass
    finally:
        with _JOB_LOCK:
            _JOB_THREADS.pop(job_id, None)


def spawn_archive_job(db, config: dict, job_id: int):
    """Start a daemon thread draining ``job_id``. Returns the Thread."""
    ev = threading.Event()
    t = threading.Thread(
        target=_guarded_run, args=(db, config, job_id, ev),
        name=f"file_activity.archive_job.{job_id}", daemon=True)
    with _JOB_LOCK:
        _JOB_THREADS[job_id] = (t, ev)
    t.start()
    return t


def cancel_all_threads(timeout: float = 10.0) -> None:
    """Signal every in-process job to stop and join briefly (used on shutdown)."""
    with _JOB_LOCK:
        items = list(_JOB_THREADS.values())
    for t, ev in items:
        ev.set()
    for t, ev in items:
        t.join(timeout=timeout)


def resume_running_archive_jobs(db, config: dict) -> int:
    """Startup re-attach: respawn any job left ``queued``/``running`` by a
    restart. Safe because ``fetch_insight_batch``'s ``NOT EXISTS`` skips
    already-archived files and the keyset restarts at ``last_file_id``. Gated by
    ``archiving.background_jobs.resume_on_startup`` (default true)."""
    bg = (config.get("archiving") or {}).get("background_jobs") or {}
    if not bg.get("resume_on_startup", True):
        return 0
    try:
        jobs = db.list_resumable_archive_jobs()
    except Exception:  # pragma: no cover - defensive (table may predate a job)
        return 0
    resumed = 0
    for job in jobs:
        spawn_archive_job(db, config, job["id"])
        resumed += 1
    if resumed:
        logger.info("resumed %d interrupted archive job(s)", resumed)
    return resumed
