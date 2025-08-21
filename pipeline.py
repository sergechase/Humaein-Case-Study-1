"""
Claim Resubmission Pipeline
- Ingests from multiple EMR formats (CSV + JSON)
- Normalizes to a unified schema
- Applies eligibility logic
- Output:  output/local ~ resubmission.json + rejections_log.json

Usage example:
  python pipeline.py data/sample.csv data/sample.json
"""

from __future__ import annotations
import sys, json, logging, argparse
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Iterable, List, Tuple, Optional

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger("pipeline")


UNIFIED_FIELDS = [
    "claim_id", "patient_id", "procedure_code",
    "denial_reason", "status", "submitted_at", "source_system"
]

# Basic rules 
RETRYABLE = {s.lower() for s in ["Missing modifier", "Incorrect NPI", "Prior auth required"]}
NON_RETRYABLE = {s.lower() for s in ["Authorization expired", "Incorrect provider type"]}

# Ambiguous classification examples 
AMBIGUOUS_HINTS = {s.lower() for s in ["incorrect procedure", "form incomplete", "not billable", ""]}

# Fixed "today" per but should be dynamic
TODAY = datetime(2025, 7, 30, tzinfo=timezone.utc)

def to_iso_utc(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()

def parse_date_guess(date_str: str) -> Optional[datetime]:
    if not date_str or str(date_str).strip().lower() in {"none", "null", "nan"}:
        return None
    # Trying several formats
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%m/%d/%Y", "%Y/%m/%d"):
        try:
            d = datetime.strptime(str(date_str).strip(), fmt).replace(tzinfo=timezone.utc)
            return d
        except ValueError:
            continue
    return None

# Normalize data
def normalize_alpha(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Input columns:
      claim_id,patient_id,procedure_code,denial_reason,submitted_at,status
    """
    out: List[Dict[str, Any]] = []
    for _, r in df.iterrows():
        submitted = parse_date_guess(r.get("submitted_at"))
        out.append({
            "claim_id": str(r.get("claim_id") or "").strip() or None,
            "patient_id": str(r.get("patient_id") or "").strip() or None,
            "procedure_code": str(r.get("procedure_code") or "").strip() or None,
            "denial_reason": (str(r.get("denial_reason")).strip() if pd.notna(r.get("denial_reason")) else None) or None,
            "status": (str(r.get("status") or "").strip().lower() in {"denied", "approved"} and str(r.get("status")).strip().lower()) or None,
            "submitted_at": to_iso_utc(submitted) if submitted else None,
            "source_system": "alpha",
        })
    return out

def normalize_beta(records: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    JSON list with keys:
      id, member, code, error_msg, date, status
    """
    out: List[Dict[str, Any]] = []
    for r in records:
        submitted = parse_date_guess(r.get("date"))
        reason = r.get("error_msg")
        reason = (str(reason).strip() if reason is not None else None) or None
        status = (str(r.get("status") or "").strip().lower() in {"denied", "approved"} and str(r.get("status")).strip().lower()) or None
        out.append({
            "claim_id": str(r.get("id") or "").strip() or None,
            "patient_id": str(r.get("member") or "").strip() or None,
            "procedure_code": str(r.get("code") or "").strip() or None,
            "denial_reason": reason,
            "status": status,
            "submitted_at": to_iso_utc(submitted) if submitted else None,
            "source_system": "beta",
        })
    return out

# Reason classifier
def mock_reason_classifier(reason: Optional[str]) -> bool:
    """
    Returns True if reason is inferred retryable, False if inferred not retryable.
    Logic:
      - If contains "incorrect" but not "provider type" → retryable
      - If contains "missing" or "modifier" → retryable
      - If contains "expired" → not retryable
      - Null/empty → not retryable (conservative)
    """
    if not reason:
        return False
    text = reason.strip().lower()
    if "expired" in text:
        return False
    if "missing" in text or "modifier" in text:
        return True
    if "incorrect" in text and "provider type" not in text:
        return True
    # Treat unknowns as not retryable
    return False

# Eligibility 
def is_eligible(claim: Dict[str, Any]) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Returns (eligible, resubmission_reason, recommended_changes)
    """
    if claim.get("status") != "denied":
        return False, None, "Excluded: status not denied"

    if not claim.get("patient_id"):
        return False, None, "Excluded: missing patient_id"

    # Submittion time > 7 days ago
    submitted_at = claim.get("submitted_at")
    if not submitted_at:
        return False, None, "Excluded: missing submitted_at"
    try:
        submitted_dt = datetime.fromisoformat(submitted_at.replace("Z", "+00:00"))
    except Exception:
        return False, None, "Excluded: invalid submitted_at"

    if TODAY - submitted_dt <= timedelta(days=7):
        return False, None, "Excluded: submitted within 7 days"

    # Denial reason 
    denial = (claim.get("denial_reason") or "").strip()
    denial_lc = denial.lower()

    if denial_lc in RETRYABLE:
        return True, denial or "Retryable", "Review required field(s) and resubmit"

    if denial_lc in NON_RETRYABLE:
        return False, None, f"Excluded: non-retryable - {denial or 'N/A'}"

    # Ambiguous classifier
    infer_retryable = mock_reason_classifier(denial if denial else "")
    if infer_retryable:
        return True, denial or "Inferred retryable", "Review suspected fields and resubmit"
    else:
        return False, None, f"Excluded: ambiguous not retryable - {denial or 'null'}"

# Helpers
def load_source(path: Path) -> List[Dict[str, Any]]:
    if path.suffix.lower() == ".csv":
        df = pd.read_csv(path)
        return normalize_alpha(df)
    elif path.suffix.lower() == ".json":
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, list):
            raise ValueError("JSON root must be a list")
        return normalize_beta(data)
    else:
        raise ValueError(f"Unsupported file type: {path.suffix}")


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser = argparse.ArgumentParser()
    parser.add_argument("inputs", nargs="+", help="Paths to EMR files (.csv or .json)")
    parser.add_argument("--out", default="output/local/resubmission.json")
    parser.add_argument("--rejects", default="output/local/rejections_log.json")
    args = parser.parse_args(argv)

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.rejects).parent.mkdir(parents=True, exist_ok=True)

    all_claims: List[Dict[str, Any]] = []
    metrics = {
        "total_claims": 0,
        "by_source": {},
        "flagged_for_resubmission": 0,
        "excluded": 0,
        "exclusion_reasons": {}
    }

    for raw in args.inputs:
        p = Path(raw)
        try:
            normalized = load_source(p)
        except Exception as e:
            log.exception(f"Failed to load {p}: {e}")
            continue

        all_claims.extend(normalized)
        src = normalized[0]["source_system"] if normalized else "unknown"
        metrics["by_source"].setdefault(src, 0)
        metrics["by_source"][src] += len(normalized)

    cleaned: List[Dict[str, Any]] = []
    for c in all_claims:
        row = {k: (c.get(k) if c.get(k) is not None else None) for k in UNIFIED_FIELDS}
        cleaned.append(row)

    metrics["total_claims"] = len(cleaned)

    candidates: List[Dict[str, Any]] = []
    rejects: List[Dict[str, Any]] = []

    for c in cleaned:
        eligible, reason, note = is_eligible(c)
        if eligible:
            metrics["flagged_for_resubmission"] += 1
            candidates.append({
                "claim_id": c["claim_id"],
                "resubmission_reason": reason,
                "source_system": c["source_system"],
                "recommended_changes": (
                    "Review NPI number and resubmit" if (reason or "").lower() == "incorrect npi" else
                    "Add or correct modifier and resubmit" if "modifier" in (reason or "").lower() else
                    "Obtain/attach prior authorization and resubmit" if "prior auth" in (reason or "").lower() else
                    "Review required field(s) and resubmit"
                )
            })
        else:
            metrics["excluded"] += 1
            reason_key = (note or "Excluded: unknown").split(":")[0].strip()
            metrics["exclusion_reasons"][reason_key] = metrics["exclusion_reasons"].get(reason_key, 0) + 1
            rejects.append({"record": c, "why": note})

    # Output
    Path(args.out).write_text(json.dumps(candidates, indent=2), encoding="utf-8")
    Path(args.rejects).write_text(json.dumps(rejects, indent=2), encoding="utf-8")

    # Logging metrics
    log.info("METRICS: %s", json.dumps(metrics, indent=2))
    log.info("Wrote %d candidates → %s", len(candidates), args.out)
    log.info("Wrote %d rejects → %s", len(rejects), args.rejects)

if __name__ == "__main__":
    main()
