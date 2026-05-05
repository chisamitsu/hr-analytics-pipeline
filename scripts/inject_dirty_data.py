"""
inject_dirty_data.py
====================
Talent Marketplace — Realistic Data Quality Issue Injector

PURPOSE
-------
Takes the clean synthetic CSVs and injects realistic, documented data quality
issues that mirror what you encounter in real HR Tech / SFTP pipelines.

Each issue is:
  - Documented with a CATEGORY and REAL-WORLD SOURCE explanation
  - Injected at a controlled rate (1–5% of affected rows)
  - Logged to an injection_manifest.csv for traceability
  - Detectable and fixable by the ETL notebook

CATEGORIES OF ISSUES INJECTED
-------------------------------
  [ENC]  Encoding / character set problems  (vendor exports, locale mismatches)
  [NULL] Unexpected nulls / blank fields    (incomplete form submissions, ETL gaps)
  [DUP]  Duplicate rows                     (double-sends from SFTP, reprocessing)
  [DATE] Temporal inconsistencies           (data entry errors, timezone issues)
  [REF]  Referential integrity breaks       (record deletions, orphaned FKs)
  [RANGE] Out-of-range / implausible values (bad dropdowns, free-text fields)
  [LOGIC] Business rule violations          (self-referencing, contradictory flags)
  [NOISE] System / bot noise               (scheduled jobs, API retry artefacts)

USAGE
-----
  python inject_dirty_data.py --input ./data/clean --output ./data/dirty --seed 42

SEED: 42 (for reproducibility)
"""

import os
import csv
import json
import random
import shutil
import argparse
import unicodedata
from datetime import date, datetime, timedelta
from pathlib import Path
from collections import defaultdict

# ── Configuration ─────────────────────────────────────────────────────────────

SEED = 42
random.seed(SEED)

INJECTION_RATES = {
    # (issue_key): rate as fraction of eligible rows
    "emp_encoding"           : 0.025,   # 2.5% of employees get name encoding errors
    "emp_null_termination"   : 0.030,   # 3.0% of terminated employees lose their term date
    "emp_future_hire"        : 0.008,   # 0.8% get a future hire date
    "emp_duplicate_email"    : 0.015,   # 1.5% get duplicate emails (raw, pre-dedup state)
    "emp_missing_status"     : 0.010,   # 1.0% get null employment_status
    "ja_open_with_enddate"   : 0.020,   # 2.0% of current assignments get a spurious end_date
    "ja_self_manager"        : 0.005,   # 0.5% become their own manager
    "ja_null_manager"        : 0.050,   # 5.0% lose manager_id (very common in real data)
    "ja_level_downgrade"     : 0.015,   # 1.5% of promotions get reversed (noise)
    "ja_date_overlap"        : 0.010,   # 1.0% get overlapping assignment periods
    "gig_zero_duration"      : 0.020,   # 2.0% of gigs have duration_weeks = 0
    "gig_null_hours"         : 0.025,   # 2.5% of gigs have null hours_per_week
    "gig_orphan_owner"       : 0.010,   # 1.0% of gig owners don't exist (churned, deleted)
    "app_early_date"         : 0.015,   # 1.5% of applications predate the gig posting
    "app_selected_no_start"  : 0.020,   # 2.0% selected apps missing assignment_start_date
    "app_end_before_start"   : 0.010,   # 1.0% of assigned gigs end before they start
    "app_zero_hours"         : 0.015,   # 1.5% of assignments have hours = 0
    "app_duplicate_apply"    : 0.020,   # 2.0% of gigs get a second application from same person
    "app_approval_before_app": 0.010,   # 1.0% approval timestamps precede application
    "skills_out_of_range"    : 0.020,   # 2.0% get skill_level 0 or 6
    "skills_duplicate"       : 0.025,   # 2.5% get a duplicate employee+skill row
    "skills_early_date"      : 0.015,   # 1.5% added_date before hire_date
    "skills_null_source"     : 0.030,   # 3.0% have null skill_source
    "training_zero_hours"    : 0.020,   # 2.0% training records have hours = 0
    "training_extreme_hours" : 0.010,   # 1.0% have hours = 999 (data entry error)
    "training_future_date"   : 0.015,   # 1.5% completion_date in the future
    "training_duplicate"     : 0.020,   # 2.0% duplicated completion records
    "training_pre_hire"      : 0.010,   # 1.0% completed before employee was hired
    "log_bot_events"         : 0.008,   # 0.8% of events at 3–4am (batch job noise)
    "log_duplicate_event_id" : 0.010,   # 1.0% duplicate event_ids
    "log_unknown_event_type" : 0.005,   # 0.5% unknown/deprecated event types
    "log_null_gig_apply"     : 0.015,   # 1.5% ApplyGig events with null gig_id
    "gig_skills_duplicate"   : 0.030,   # 3.0% duplicate gig+skill combinations
    "train_skills_duplicate" : 0.030,   # 3.0% duplicate training+skill combinations
}

MANIFEST = []  # injection log


def log(file, issue_key, category, description, rows_affected, example=None):
    MANIFEST.append({
        "file"          : file,
        "issue_key"     : issue_key,
        "category"      : category,
        "description"   : description,
        "rows_affected" : rows_affected,
        "example_value" : str(example) if example else "",
    })
    print(f"  [{category}] {issue_key}: {rows_affected} rows → {description}")


def sample_indices(n_total, rate, min_count=1):
    """Return a set of random row indices to inject issues into."""
    count = max(min_count, int(n_total * rate))
    count = min(count, n_total)
    return set(random.sample(range(n_total), count))


def add_encoding_noise(name):
    """Simulate a character encoding round-trip error (UTF-8 → Latin-1 → UTF-8)."""
    replacements = {
        "á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u",
        "ä": "ae", "ö": "oe", "ü": "ue", "ß": "ss",
        "ñ": "n", "ç": "c", "è": "e", "ê": "e", "â": "a",
        "à": "a", "ï": "i", "ô": "o", "ù": "u", "ő": "o",
        "ź": "z", "ż": "z", "ś": "s", "ł": "l", "ą": "a",
        "ę": "e", "ć": "c", "ń": "n",
        "Á": "A", "É": "E", "Ó": "O", "Ö": "Oe", "Ü": "Ue",
        "Ñ": "N", "Ç": "C",
    }
    # Also occasionally produce the real encoding garbage
    garbage_map = {
        "ü": "ü", "ö": "ö", "ä": "ä",
        "é": "é", "è": "è", "ê": "ê",
        "ñ": "ñ", "ç": "ç",
    }
    result = name
    if random.random() < 0.5:
        for src, dst in replacements.items():
            result = result.replace(src, dst)
    else:
        for src, dst in garbage_map.items():
            result = result.replace(src, dst)
    return result


# ── File loaders ──────────────────────────────────────────────────────────────

def read_csv(path):
    with open(path, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = reader.fieldnames
    return rows, fieldnames


def write_csv(path, fieldnames, rows):
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


# ─────────────────────────────────────────────────────────────────────────────
# ISSUE INJECTORS — one function per file
# ─────────────────────────────────────────────────────────────────────────────

def inject_employee_master(rows, fieldnames):
    fname = "raw_employee_master.csv"
    n = len(rows)

    # Build hire_date and termination_date lookups
    hire_dates = {}
    for r in rows:
        hire_dates[r["employee_id"]] = r["hire_date"]

    # [ENC] Name encoding errors — simulates vendor SFTP with wrong locale setting
    # Real source: SAP SuccessFactors CSV exports default to Latin-1 on some configs
    enc_idx = sample_indices(n, INJECTION_RATES["emp_encoding"])
    enc_examples = []
    for i in enc_idx:
        original = rows[i]["first_name"] + " " + rows[i]["last_name"]
        rows[i]["first_name"] = add_encoding_noise(rows[i]["first_name"])
        rows[i]["last_name"]  = add_encoding_noise(rows[i]["last_name"])
        enc_examples.append(f"{original} → {rows[i]['first_name']} {rows[i]['last_name']}")
    log(fname, "emp_encoding", "ENC",
        "First/last name encoding corruption (UTF-8 → Latin-1 round-trip from vendor SFTP)",
        len(enc_idx), enc_examples[0] if enc_examples else None)

    # [NULL] Terminated employees missing termination_date
    # Real source: HR admin forgot to enter the date; system marks status but field left blank
    terminated_idx = [i for i, r in enumerate(rows) if r["employment_status"] == "Terminated"]
    null_term_idx = sample_indices(len(terminated_idx), INJECTION_RATES["emp_null_termination"])
    actual_null_idx = [terminated_idx[i] for i in null_term_idx]
    for i in actual_null_idx:
        rows[i]["termination_date"] = ""
    log(fname, "emp_null_termination", "NULL",
        "Terminated employees with blank termination_date (HR admin data entry gap)",
        len(actual_null_idx))

    # [DATE] Future hire dates — data entry error or pre-loading for upcoming hires
    # Real source: HR pre-loads new starters before their actual start date
    future_idx = sample_indices(n, INJECTION_RATES["emp_future_hire"])
    for i in future_idx:
        future_days = random.randint(30, 180)
        future_date = (date(2025, 12, 31) + timedelta(days=future_days)).isoformat()
        rows[i]["hire_date"] = future_date
        rows[i]["termination_date"] = ""
        rows[i]["employment_status"] = "Active"
    log(fname, "emp_future_hire", "DATE",
        "Hire date set in the future (HR pre-loading upcoming starters)",
        len(future_idx), f"hire_date: {future_date}")

    # [DUP] Duplicate email addresses (raw, before deduplication)
    # Real source: person changed name, HR created new record without removing old email
    dup_email_idx = sample_indices(n, INJECTION_RATES["emp_duplicate_email"])
    all_emails = [r["company_email"] for r in rows]
    for i in dup_email_idx:
        # Assign an email that already exists
        steal_from = random.choice([j for j in range(n) if j != i])
        rows[i]["company_email"] = rows[steal_from]["company_email"]
    log(fname, "emp_duplicate_email", "DUP",
        "Duplicate company_email values (name change → new record, old email reused)",
        len(dup_email_idx))

    # [NULL] Missing employment_status
    # Real source: middleware integration dropped the field on partial API response
    status_null_idx = sample_indices(n, INJECTION_RATES["emp_missing_status"])
    for i in status_null_idx:
        rows[i]["employment_status"] = ""
    log(fname, "emp_missing_status", "NULL",
        "Null employment_status (middleware API partial response — field silently dropped)",
        len(status_null_idx))

    return rows, fieldnames


def inject_job_assignment(rows, fieldnames, emp_rows):
    fname = "raw_employee_job_assignment.csv"
    n = len(rows)

    emp_ids = {r["employee_id"] for r in emp_rows}

    # Build current assignments (end_date is empty)
    current_idx = [i for i, r in enumerate(rows) if not r["end_date"]]

    # [LOGIC] Open (current) assignment with spurious end_date
    # Real source: batch migration script incorrectly closed open records
    open_end_idx = sample_indices(len(current_idx), INJECTION_RATES["ja_open_with_enddate"])
    actual_open_end_idx = [current_idx[i] for i in open_end_idx]
    for i in actual_open_end_idx:
        start = rows[i]["start_date"]
        try:
            start_dt = date.fromisoformat(start)
            spurious_end = (start_dt + timedelta(days=random.randint(30, 300))).isoformat()
        except Exception:
            spurious_end = "2025-06-15"
        rows[i]["end_date"] = spurious_end
    log(fname, "ja_open_with_enddate", "LOGIC",
        "Current (open) assignments incorrectly closed by migration script",
        len(actual_open_end_idx))

    # [LOGIC] Employee is their own manager
    # Real source: top-level executives / org chart root nodes often self-reference
    self_mgr_idx = sample_indices(n, INJECTION_RATES["ja_self_manager"])
    for i in self_mgr_idx:
        rows[i]["manager_id"] = rows[i]["employee_id"]
    log(fname, "ja_self_manager", "LOGIC",
        "manager_id = employee_id (self-referencing, common for C-suite in org chart exports)",
        len(self_mgr_idx))

    # [NULL] Missing manager_id
    # Real source: matrix org, interim roles, or SuccessFactors relationship not mapped
    null_mgr_idx = sample_indices(n, INJECTION_RATES["ja_null_manager"])
    for i in null_mgr_idx:
        rows[i]["manager_id"] = ""
    log(fname, "ja_null_manager", "NULL",
        "Blank manager_id (matrix org / interim role / SF relationship not maintained)",
        len(null_mgr_idx))

    # [LOGIC] Level downgrade (noise in promotion sequence)
    # Real source: retroactive correction applied without updating the SCD2 chain
    multi_assign_emp = defaultdict(list)
    for i, r in enumerate(rows):
        multi_assign_emp[r["employee_id"]].append(i)
    candidates = [idxs for idxs in multi_assign_emp.values() if len(idxs) >= 2]
    n_downgrade = max(1, int(len(candidates) * INJECTION_RATES["ja_level_downgrade"]))
    downgrade_targets = random.sample(candidates, min(n_downgrade, len(candidates)))
    LEVELS = ["L1", "L2", "L3", "L4", "L5", "L6", "L7"]
    actual_downgrade = 0
    for idxs in downgrade_targets:
        idxs_sorted = sorted(idxs, key=lambda i: rows[i]["start_date"])
        last_idx = idxs_sorted[-1]
        current_level = rows[last_idx]["job_level"]
        if current_level in LEVELS and LEVELS.index(current_level) > 0:
            rows[last_idx]["job_level"] = LEVELS[LEVELS.index(current_level) - 1]
            actual_downgrade += 1
    log(fname, "ja_level_downgrade", "LOGIC",
        "Job level decreased in latest assignment (retroactive correction applied without SCD2 update)",
        actual_downgrade)

    # [DATE] Overlapping assignment date ranges for same employee
    # Real source: two records created during a system migration; both marked active
    overlap_idx = sample_indices(len(candidates), INJECTION_RATES["ja_date_overlap"])
    overlap_targets = [candidates[i] for i in overlap_idx if i < len(candidates)]
    actual_overlap = 0
    for idxs in overlap_targets:
        if len(idxs) < 2:
            continue
        idxs_sorted = sorted(idxs, key=lambda i: rows[i]["start_date"])
        # Push the start_date of the second record back to overlap with the first
        first_end = rows[idxs_sorted[0]].get("end_date", "")
        if first_end:
            try:
                first_end_dt = date.fromisoformat(first_end)
                rows[idxs_sorted[1]]["start_date"] = (
                    first_end_dt - timedelta(days=random.randint(10, 30))
                ).isoformat()
                actual_overlap += 1
            except Exception:
                pass
    log(fname, "ja_date_overlap", "DATE",
        "Overlapping start/end dates between consecutive assignments (system migration duplicate)",
        actual_overlap)

    return rows, fieldnames


def inject_gig_master(rows, fieldnames, emp_ids_set):
    fname = "raw_gig_master.csv"
    n = len(rows)

    # [RANGE] Duration = 0 weeks
    # Real source: vendor API default value when field left blank during gig creation
    zero_dur_idx = sample_indices(n, INJECTION_RATES["gig_zero_duration"])
    for i in zero_dur_idx:
        rows[i]["duration_weeks_planned"] = "0"
    log(fname, "gig_zero_duration", "RANGE",
        "duration_weeks_planned = 0 (vendor API default when field left blank)",
        len(zero_dur_idx))

    # [NULL] Null hours_per_week_planned
    # Real source: optional field in gig creation form, skipped by owners in a hurry
    null_hours_idx = sample_indices(n, INJECTION_RATES["gig_null_hours"])
    for i in null_hours_idx:
        rows[i]["hours_per_week_planned"] = ""
    log(fname, "gig_null_hours", "NULL",
        "Null hours_per_week_planned (optional form field skipped during gig creation)",
        len(null_hours_idx))

    # [REF] Orphaned owner — owner employee no longer exists
    # Real source: employee terminated, record deleted from HR system, gig record orphaned
    all_owner_idx = list(range(n))
    orphan_owner_idx = sample_indices(n, INJECTION_RATES["gig_orphan_owner"])
    fake_emp_ids = [f"EMP-DEAD{i:04X}" for i in range(500)]
    for i in orphan_owner_idx:
        rows[i]["owner_employee_id"] = random.choice(fake_emp_ids)
        rows[i]["created_by_employee_id"] = rows[i]["owner_employee_id"]
    log(fname, "gig_orphan_owner", "REF",
        "owner_employee_id references non-existent employee (terminated, record deleted from HRIS)",
        len(orphan_owner_idx), fake_emp_ids[0])

    return rows, fieldnames


def inject_applications(rows, fieldnames):
    fname = "raw_gig_applications_and_assignments.csv"
    n = len(rows)

    # [DATE] Application date before gig posting date
    # Real source: timezone mismatch (gig posted UTC+1, application logged UTC)
    early_idx = sample_indices(n, INJECTION_RATES["app_early_date"])
    for i in early_idx:
        try:
            posted = rows[i]["posted_date"] if "posted_date" in rows[i] else None
            app_dt = rows[i]["application_date"][:10]
            app_date = date.fromisoformat(app_dt)
            rows[i]["application_date"] = (
                app_date - timedelta(days=random.randint(1, 5))
            ).isoformat() + " " + rows[i]["application_date"][11:]
        except Exception:
            pass
    log(fname, "app_early_date", "DATE",
        "application_date before gig posted_date (UTC vs UTC+1 timezone mismatch from vendor)",
        len(early_idx))

    # [NULL] Selected applications missing assignment_start_date
    # Real source: status updated to Selected but assignment not yet formally confirmed
    selected_idx = [i for i, r in enumerate(rows) if r["application_status"] == "Selected"]
    null_start_idx = sample_indices(len(selected_idx), INJECTION_RATES["app_selected_no_start"])
    actual_null_start = [selected_idx[i] for i in null_start_idx]
    for i in actual_null_start:
        rows[i]["assignment_start_date"] = ""
        rows[i]["assignment_end_date"]   = ""
        rows[i]["assigned_hours_per_week"] = ""
    log(fname, "app_selected_no_start", "NULL",
        "Selected applications with no assignment dates (status updated before formal confirmation)",
        len(actual_null_start))

    # [DATE] Assignment end_date before start_date
    # Real source: date fields swapped during bulk import from spreadsheet
    assigned_idx = [i for i, r in enumerate(rows)
                    if r["assignment_start_date"] and r["assignment_end_date"]]
    end_before_start_idx = sample_indices(len(assigned_idx), INJECTION_RATES["app_end_before_start"])
    actual_ebs = [assigned_idx[i] for i in end_before_start_idx]
    for i in actual_ebs:
        # Swap the dates
        rows[i]["assignment_start_date"], rows[i]["assignment_end_date"] = (
            rows[i]["assignment_end_date"], rows[i]["assignment_start_date"]
        )
    log(fname, "app_end_before_start", "DATE",
        "assignment_end_date before assignment_start_date (date columns swapped in bulk import)",
        len(actual_ebs))

    # [RANGE] Assigned hours = 0
    # Real source: hours_per_week field defaulted to 0 in old form version
    assigned_hrs_idx = [i for i, r in enumerate(rows) if r["assigned_hours_per_week"]]
    zero_hrs_idx = sample_indices(len(assigned_hrs_idx), INJECTION_RATES["app_zero_hours"])
    actual_zero_hrs = [assigned_hrs_idx[i] for i in zero_hrs_idx]
    for i in actual_zero_hrs:
        rows[i]["assigned_hours_per_week"] = "0"
    log(fname, "app_zero_hours", "RANGE",
        "assigned_hours_per_week = 0 (old form version defaulted numeric field to 0)",
        len(actual_zero_hrs))

    # [DUP] Same employee applying twice to same gig
    # Real source: employee clicked Apply twice; or Withdraw + re-Apply on same session
    gig_applicants = defaultdict(list)
    for i, r in enumerate(rows):
        gig_applicants[r["gig_id"]].append((i, r["employee_id"]))
    dup_apply_count = 0
    dup_app_ids = set()
    for gig_id, apps_list in gig_applicants.items():
        if random.random() < INJECTION_RATES["app_duplicate_apply"] and len(apps_list) > 1:
            # Pick one applicant and duplicate their row with a new app_id
            idx, emp_id = random.choice(apps_list)
            new_row = dict(rows[idx])
            new_row["application_id"] = f"APP-DUPL{dup_apply_count:04X}"
            new_row["application_status"] = "Applied"  # second attempt lands as Applied
            rows.append(new_row)
            dup_apply_count += 1
    log(fname, "app_duplicate_apply", "DUP",
        "Same employee applied twice to same gig (double-click or Withdraw + re-Apply same session)",
        dup_apply_count)

    # [DATE] Manager approval timestamp before application timestamp
    # Real source: manager pre-approved via email; HR admin backdated the approval incorrectly
    approval_idx = [i for i, r in enumerate(rows)
                    if r["manager_approval_date"] and r["application_date"]]
    approval_early_idx = sample_indices(len(approval_idx), INJECTION_RATES["app_approval_before_app"])
    actual_approval_early = [approval_idx[i] for i in approval_early_idx]
    for i in actual_approval_early:
        try:
            app_dt = date.fromisoformat(rows[i]["application_date"][:10])
            rows[i]["manager_approval_date"] = (
                app_dt - timedelta(days=random.randint(1, 3))
            ).isoformat() + " 09:00:00"
        except Exception:
            pass
    log(fname, "app_approval_before_app", "DATE",
        "manager_approval_date before application_date (pre-approval backdated by HR admin)",
        len(actual_approval_early))

    return rows, fieldnames


def inject_employee_skills(rows, fieldnames, emp_rows):
    fname = "raw_employee_skills.csv"
    n = len(rows)

    hire_map = {r["employee_id"]: r["hire_date"] for r in emp_rows}

    # [RANGE] skill_level = 0 or 6 (out of 1-5 range)
    # Real source: import script mapped a 0-based index incorrectly; or free-text field
    range_idx = sample_indices(n, INJECTION_RATES["skills_out_of_range"])
    for i in range_idx:
        rows[i]["skill_level"] = str(random.choice([0, 6]))
    log(fname, "skills_out_of_range", "RANGE",
        "skill_level = 0 or 6 (valid range is 1–5; import mapped 0-based index incorrectly)",
        len(range_idx))

    # [DUP] Duplicate employee+skill combinations
    # Real source: file sent twice from vendor (re-transmission after timeout)
    dup_idx = sample_indices(n, INJECTION_RATES["skills_duplicate"])
    for i in dup_idx:
        dup_row = dict(rows[i])
        rows.append(dup_row)
    log(fname, "skills_duplicate", "DUP",
        "Duplicate employee+skill rows (vendor SFTP file re-transmitted after connection timeout)",
        len(dup_idx))

    # [DATE] added_date before employee hire_date
    # Real source: skill imported from a previous employer record during onboarding
    early_date_idx = sample_indices(n, INJECTION_RATES["skills_early_date"])
    for i in early_date_idx:
        emp_id = rows[i]["employee_id"]
        hire_str = hire_map.get(emp_id, "2020-01-01")
        try:
            hire_dt = date.fromisoformat(hire_str)
            rows[i]["added_date"] = (
                hire_dt - timedelta(days=random.randint(30, 365))
            ).isoformat()
        except Exception:
            pass
    log(fname, "skills_early_date", "DATE",
        "added_date before employee hire_date (skill imported from previous employer profile)",
        len(early_date_idx))

    # [NULL] Null skill_source
    # Real source: source field added later; backfill job left some records blank
    null_src_idx = sample_indices(n, INJECTION_RATES["skills_null_source"])
    for i in null_src_idx:
        rows[i]["skill_source"] = ""
    log(fname, "skills_null_source", "NULL",
        "Null skill_source (field added retroactively; backfill job did not cover all records)",
        len(null_src_idx))

    return rows, fieldnames


def inject_training_records(rows, fieldnames, emp_rows):
    fname = "raw_training_records.csv"
    n = len(rows)

    hire_map = {r["employee_id"]: r["hire_date"] for r in emp_rows}

    # [RANGE] hours = 0
    # Real source: e-learning system reported 0 for completions under 1 minute (browser closed early)
    zero_hr_idx = sample_indices(n, INJECTION_RATES["training_zero_hours"])
    for i in zero_hr_idx:
        rows[i]["hours"] = "0"
    log(fname, "training_zero_hours", "RANGE",
        "training hours = 0 (e-learning platform logged completion but no time spent — browser closed)",
        len(zero_hr_idx))

    # [RANGE] hours = 999 (extreme outlier)
    # Real source: free-text field; someone typed 999 instead of 9.9
    extreme_hr_idx = sample_indices(n, INJECTION_RATES["training_extreme_hours"])
    for i in extreme_hr_idx:
        rows[i]["hours"] = str(random.choice([999, 9999, 888]))
    log(fname, "training_extreme_hours", "RANGE",
        "training hours = 999/9999 (free-text entry error: 9.9 typed as 999)",
        len(extreme_hr_idx))

    # [DATE] Completion date in the future
    # Real source: scheduled/planned trainings pre-loaded with target completion date
    future_idx = sample_indices(n, INJECTION_RATES["training_future_date"])
    for i in future_idx:
        future_dt = (date(2025, 12, 31) + timedelta(days=random.randint(10, 180))).isoformat()
        rows[i]["completion_date"] = future_dt
    log(fname, "training_future_date", "DATE",
        "completion_date in the future (planned trainings pre-loaded with target date)",
        len(future_idx), future_dt)

    # [DUP] Duplicate completion records
    # Real source: LMS sent completion webhook twice; ETL processed both
    dup_idx = sample_indices(n, INJECTION_RATES["training_duplicate"])
    for i in dup_idx:
        dup_row = dict(rows[i])
        rows.append(dup_row)
    log(fname, "training_duplicate", "DUP",
        "Duplicate training completion rows (LMS sent completion webhook twice, ETL processed both)",
        len(dup_idx))

    # [DATE] Completion before hire date
    # Real source: training from previous company imported during onboarding/LMS migration
    pre_hire_idx = sample_indices(n, INJECTION_RATES["training_pre_hire"])
    for i in pre_hire_idx:
        emp_id = rows[i]["employee_id"]
        hire_str = hire_map.get(emp_id, "2020-01-01")
        try:
            hire_dt = date.fromisoformat(hire_str)
            rows[i]["completion_date"] = (
                hire_dt - timedelta(days=random.randint(30, 730))
            ).isoformat()
        except Exception:
            pass
    log(fname, "training_pre_hire", "DATE",
        "completion_date before hire_date (training imported from previous employer during LMS migration)",
        len(pre_hire_idx))

    return rows, fieldnames


def inject_activity_log(rows, fieldnames):
    fname = "raw_user_activity_log.csv"
    n = len(rows)

    KNOWN_EVENT_TYPES = [
        "Login", "ViewGig", "SearchGig", "ApplyGig",
        "WithdrawApplication", "UpdateProfile", "AddSkill", "CompleteGig"
    ]
    DEPRECATED_EVENT_TYPES = [
        "ViewProfile", "ShareGig", "BookmarkGig",
        "RateGig", "ReportIssue", "InviteColleague"
    ]

    # [NOISE] Events at 3-4am (scheduled batch job noise)
    # Real source: nightly sync job logs "Login" events for SSO token refresh
    bot_idx = sample_indices(n, INJECTION_RATES["log_bot_events"])
    for i in bot_idx:
        ts = rows[i]["event_timestamp"]
        if len(ts) >= 10:
            rows[i]["event_timestamp"] = ts[:10] + f" 0{random.randint(3,4)}:{random.randint(0,59):02d}:{random.randint(0,59):02d}"
            rows[i]["event_type"] = "Login"
            rows[i]["metadata"] = json.dumps({"source": "nightly_sso_refresh", "auto": True})
    log(fname, "log_bot_events", "NOISE",
        "Login events at 3–4am (nightly SSO token refresh job logs synthetic Login events)",
        len(bot_idx))

    # [DUP] Duplicate event_ids
    # Real source: API retry on network timeout sent the same event twice
    dup_evt_idx = sample_indices(n, INJECTION_RATES["log_duplicate_event_id"])
    for i in dup_evt_idx:
        dup_row = dict(rows[i])
        # Same event_id, slightly different timestamp
        ts = rows[i]["event_timestamp"]
        try:
            dt = datetime.fromisoformat(ts)
            dup_row["event_timestamp"] = (dt + timedelta(seconds=random.randint(1, 5))).isoformat(sep=" ")
        except Exception:
            pass
        rows.append(dup_row)
    log(fname, "log_duplicate_event_id", "DUP",
        "Duplicate event_id values (API retry on network timeout delivered same event twice)",
        len(dup_evt_idx))

    # [NOISE] Unknown/deprecated event_type values
    # Real source: older platform version used different event names before schema change
    unknown_idx = sample_indices(n, INJECTION_RATES["log_unknown_event_type"])
    for i in unknown_idx:
        rows[i]["event_type"] = random.choice(DEPRECATED_EVENT_TYPES)
    log(fname, "log_unknown_event_type", "NOISE",
        "Deprecated event_type values (old platform version before schema standardisation v2.3)",
        len(unknown_idx), DEPRECATED_EVENT_TYPES[0])

    # [NULL] ApplyGig events with null gig_id
    # Real source: frontend sent the event before gig_id was resolved (race condition)
    apply_idx = [i for i, r in enumerate(rows) if r["event_type"] == "ApplyGig"]
    null_gig_idx = sample_indices(len(apply_idx), INJECTION_RATES["log_null_gig_apply"])
    actual_null_gig = [apply_idx[i] for i in null_gig_idx]
    for i in actual_null_gig:
        rows[i]["gig_id"] = ""
        rows[i]["metadata"] = json.dumps({"error": "gig_id_not_resolved", "race_condition": True})
    log(fname, "log_null_gig_apply", "NULL",
        "ApplyGig events with null gig_id (frontend race condition: event fired before gig_id resolved)",
        len(actual_null_gig))

    return rows, fieldnames


def inject_gig_required_skills(rows, fieldnames):
    fname = "raw_gig_required_skills.csv"
    n = len(rows)

    # [DUP] Duplicate gig+skill combinations
    # Real source: gig updated, skills re-saved without clearing previous records first
    dup_idx = sample_indices(n, INJECTION_RATES["gig_skills_duplicate"])
    for i in dup_idx:
        rows.append(dict(rows[i]))
    log(fname, "gig_skills_duplicate", "DUP",
        "Duplicate gig+skill rows (gig updated, skill list re-saved without deleting previous rows)",
        len(dup_idx))

    return rows, fieldnames


def inject_training_skills(rows, fieldnames):
    fname = "raw_training_skills.csv"
    n = len(rows)

    # [DUP] Duplicate training+skill combinations
    # Real source: same cause as gig_skills — update without delete
    dup_idx = sample_indices(n, INJECTION_RATES["train_skills_duplicate"])
    for i in dup_idx:
        rows.append(dict(rows[i]))
    log(fname, "train_skills_duplicate", "DUP",
        "Duplicate training+skill rows (training catalogue update re-inserted all skills)",
        len(dup_idx))

    return rows, fieldnames


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main(input_dir: Path, output_dir: Path):
    output_dir.mkdir(parents=True, exist_ok=True)

    print("\n" + "═" * 65)
    print("  TALENT MARKETPLACE — DATA QUALITY INJECTOR")
    print(f"  Seed: {SEED}  |  Input: {input_dir}  |  Output: {output_dir}")
    print("═" * 65)

    FILES = {
        "employee_master"    : "raw_employee_master.csv",
        "job_assignment"     : "raw_employee_job_assignment.csv",
        "skill_dim"          : "dim_skill.csv",
        "employee_skills"    : "raw_employee_skills.csv",
        "gig_master"         : "raw_gig_master.csv",
        "gig_req_skills"     : "raw_gig_required_skills.csv",
        "applications"       : "raw_gig_applications_and_assignments.csv",
        "activity_log"       : "raw_user_activity_log.csv",
        "training_master"    : "raw_training_master.csv",
        "training_skills"    : "raw_training_skills.csv",
        "training_records"   : "raw_training_records.csv",
    }

    print("\n[1/11] Loading source files...")
    data = {}
    fnames = {}
    for key, filename in FILES.items():
        src = input_dir / filename
        if not src.exists():
            print(f"  ⚠️  Missing: {filename} — skipping")
            continue
        data[key], fnames[key] = read_csv(src)
        print(f"  ✓  {filename}: {len(data[key]):,} rows")

    emp_rows = data.get("employee_master", [])
    emp_ids_set = {r["employee_id"] for r in emp_rows}

    print("\n[2/11] Injecting issues into employee_master...")
    data["employee_master"], fnames["employee_master"] = inject_employee_master(
        data["employee_master"], fnames["employee_master"]
    )

    print("\n[3/11] Injecting issues into job_assignment...")
    data["job_assignment"], fnames["job_assignment"] = inject_job_assignment(
        data["job_assignment"], fnames["job_assignment"], emp_rows
    )

    print("\n[4/11] Injecting issues into gig_master...")
    data["gig_master"], fnames["gig_master"] = inject_gig_master(
        data["gig_master"], fnames["gig_master"], emp_ids_set
    )

    print("\n[5/11] Injecting issues into applications...")
    data["applications"], fnames["applications"] = inject_applications(
        data["applications"], fnames["applications"]
    )

    print("\n[6/11] Injecting issues into employee_skills...")
    data["employee_skills"], fnames["employee_skills"] = inject_employee_skills(
        data["employee_skills"], fnames["employee_skills"], emp_rows
    )

    print("\n[7/11] Injecting issues into training_records...")
    data["training_records"], fnames["training_records"] = inject_training_records(
        data["training_records"], fnames["training_records"], emp_rows
    )

    print("\n[8/11] Injecting issues into activity_log...")
    data["activity_log"], fnames["activity_log"] = inject_activity_log(
        data["activity_log"], fnames["activity_log"]
    )

    print("\n[9/11] Injecting issues into gig_required_skills...")
    data["gig_req_skills"], fnames["gig_req_skills"] = inject_gig_required_skills(
        data["gig_req_skills"], fnames["gig_req_skills"]
    )

    print("\n[10/11] Injecting issues into training_skills...")
    data["training_skills"], fnames["training_skills"] = inject_training_skills(
        data["training_skills"], fnames["training_skills"]
    )

    # dim_skill and training_master: copy unchanged (reference tables, clean by design)
    for key in ["skill_dim", "training_master"]:
        if key in data:
            print(f"\n  (no injection needed for {FILES[key]} — reference/dimension table)")

    print("\n[11/11] Writing dirty output files...")
    for key, filename in FILES.items():
        if key not in data:
            continue
        dst = output_dir / filename
        rows = data[key]
        # Shuffle rows slightly to make the dirty data less obvious
        if key not in ["skill_dim", "training_master"]:
            random.shuffle(rows)
        write_csv(dst, fnames[key], rows)
        original_n = len(read_csv(input_dir / filename)[0]) if (input_dir / filename).exists() else "?"
        print(f"  ✓  {filename}: {len(rows):,} rows (was {original_n})")

    # Write injection manifest
    manifest_path = output_dir / "injection_manifest.csv"
    manifest_fields = ["file", "issue_key", "category", "description", "rows_affected", "example_value"]
    write_csv(manifest_path, manifest_fields, MANIFEST)
    print(f"\n  📋  Injection manifest: {manifest_path} ({len(MANIFEST)} issues logged)")

    # Summary
    print("\n" + "═" * 65)
    print("  INJECTION SUMMARY")
    print("═" * 65)
    by_category = defaultdict(int)
    total_rows = 0
    for m in MANIFEST:
        by_category[m["category"]] += m["rows_affected"]
        total_rows += m["rows_affected"]
    for cat, count in sorted(by_category.items()):
        labels = {
            "ENC": "Encoding errors",
            "NULL": "Unexpected nulls",
            "DUP": "Duplicate rows",
            "DATE": "Temporal issues",
            "REF": "Referential integrity",
            "RANGE": "Out-of-range values",
            "LOGIC": "Business rule violations",
            "NOISE": "System/bot noise",
        }
        print(f"  [{cat}] {labels.get(cat, cat):<28s}: {count:,} rows")
    print(f"  {'─'*50}")
    print(f"  Total rows affected: {total_rows:,}")
    print("═" * 65)
    print(f"\n✅  Done. Dirty data written to: {output_dir.resolve()}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inject realistic data quality issues into clean CSVs")
    parser.add_argument("--input",  default="./data/clean",  help="Directory with clean CSVs")
    parser.add_argument("--output", default="./data/dirty",  help="Directory to write dirty CSVs")
    parser.add_argument("--seed",   type=int, default=42,    help="Random seed")
    args = parser.parse_args()
    SEED = args.seed
    random.seed(SEED)
    main(Path(args.input), Path(args.output))
