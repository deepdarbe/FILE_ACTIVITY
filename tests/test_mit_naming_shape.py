"""#366 audit follow-up: MITNamingAnalyzer.get_report() must return ONE
canonical shape — including the empty (total==0) path, which previously emitted
a short dict ({total, compliance_score, requirements, best_practices}) missing
total_files_analyzed / *_compliance / all_* / summary. A non-defensive consumer
would read those as undefined.
"""

from __future__ import annotations

from src.scanner.file_scanner import MITNamingAnalyzer

CANONICAL_KEYS = {
    "total_files_analyzed", "compliance_score", "requirement_compliance",
    "full_compliance", "fully_compliant_count", "req_compliant_count",
    "requirements", "best_practices", "all_requirements", "all_best_practices",
    "summary",
}


def test_empty_report_has_canonical_shape():
    rep = MITNamingAnalyzer().get_report()   # no files processed -> total==0 path
    assert CANONICAL_KEYS <= set(rep), f"missing: {CANONICAL_KEYS - set(rep)}"
    assert rep["total_files_analyzed"] == 0
    assert rep["requirements"] == [] and rep["best_practices"] == []
    assert set(rep["summary"]) == {
        "total_requirement_violations", "total_bp_violations", "top_issue"}
    assert rep["summary"]["total_requirement_violations"] == 0
    assert rep["summary"]["top_issue"] is None
