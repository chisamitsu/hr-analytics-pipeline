# Data Dictionary — Pharma Talent Marketplace Synthetic Dataset

**Version:** 1.0 | **Seed:** 12345 | **Generated:** 2025 | **Time Window:** 2023-01-01 to 2025-12-31

---

## 1. `raw_employee_master.csv` — 5,000 rows

Master record for each employee. One row per person.

| Column | Type | Format | Description |
|---|---|---|---|
| employee_id | string | EMP-XXXXXXXX | Unique employee identifier (8 hex uppercase chars) |
| first_name | string | — | Employee first name (region-appropriate) |
| last_name | string | — | Employee last name (country-appropriate) |
| company_email | string | first.last@example.com | Lowercase, diacritics removed; `.1/.2` suffix for duplicates |
| birth_date | date | YYYY-MM-DD | Date of birth; consistent with age distribution |
| gender | string | enum | Female / Male / Non-binary / Not disclosed |
| country_of_birth | string | — | One of 30 countries (Europe-weighted) |
| hire_date | date | YYYY-MM-DD | Employment start date (may pre-date 2023) |
| termination_date | date | YYYY-MM-DD or empty | Date of employment end; empty if still active |
| contract_type | string | enum | Permanent / Temporary / Contractor |
| employment_status | string | enum | Active / Terminated / Leave / Retired |
| job_level_at_hire | string | L1–L7 | Job grade at time of hiring |

---

## 2. `raw_employee_job_assignment.csv` — ~8,600 rows (SCD2)

Slowly-changing dimension tracking job assignment history. Each row is one assignment period.

| Column | Type | Format | Description |
|---|---|---|---|
| employee_id | string | EMP-XXXXXXXX | FK → raw_employee_master |
| job_assignment_id | string | JA-XXXXXXXX | Unique surrogate key for the assignment |
| job_id | string | JOB-XXXX | Dimensional job code (reused across employees) |
| job_title | string | — | Human-readable job title |
| job_level | string | L1–L7 | Job grade for this assignment period |
| function | string | enum | Business function (R&D, IT, Finance, etc.) |
| sub_function | string | — | Functional sub-team |
| business_unit | string | — | Organisational unit |
| manager_id | string | EMP-XXXXXXXX | FK → raw_employee_master (direct manager) |
| employment_country | string | — | Country where the role is based |
| start_date | date | YYYY-MM-DD | Start of this assignment period |
| end_date | date | YYYY-MM-DD or empty | End of period; empty = current assignment |

---

## 3. `dim_skill.csv` — 80 rows

Master catalogue of 80 skills across five categories.

| Column | Type | Format | Description |
|---|---|---|---|
| skill_id | string | SKILL-XXX | Unique skill identifier |
| skill_name | string | — | Descriptive skill name |
| skill_category | string | enum | Technical / Regulatory / Digital / Functional / Leadership |

---

## 4. `raw_employee_skills.csv` — ~92,000 rows

Each row records a skill possessed by an employee at a given proficiency level.

| Column | Type | Format | Description |
|---|---|---|---|
| employee_id | string | EMP-XXXXXXXX | FK → raw_employee_master |
| skill_id | string | SKILL-XXX | FK → dim_skill |
| skill_level | integer | 1–5 | Proficiency (1 = beginner, 5 = expert) |
| skill_source | string | enum | Self / Manager / Gig / Training |
| added_date | date | YYYY-MM-DD | Date skill was assessed or added |

**Note:** If the same employee-skill pair appears via multiple sources, the highest level is retained.

---

## 5. `raw_gig_master.csv` — 1,500 rows

One row per gig (500 per year × 3 years).

| Column | Type | Format | Description |
|---|---|---|---|
| gig_id | string | GIG-XXXXXX | Unique gig identifier |
| gig_title | string | — | Descriptive title |
| gig_type | string | enum | Project / Mentoring |
| owner_employee_id | string | EMP-XXXXXXXX | FK → employee who owns/sponsors the gig |
| hours_per_week_planned | integer | 2–20 | Planned weekly time commitment |
| duration_weeks_planned | integer | 1–26 | Planned duration in weeks |
| business_unit | string | — | Owning business unit |
| posted_date | date | YYYY-MM-DD | Date gig was published on the marketplace |
| created_by_employee_id | string | EMP-XXXXXXXX | FK → employee who created the posting |

---

## 6. `raw_gig_required_skills.csv` — ~6,000 rows

Many-to-many linking gigs to required skills.

| Column | Type | Format | Description |
|---|---|---|---|
| gig_id | string | GIG-XXXXXX | FK → raw_gig_master |
| skill_id | string | SKILL-XXX | FK → dim_skill |

Each gig requires 2–6 skills.

---

## 7. `raw_gig_applications_and_assignments.csv` — ~23,600 rows

One row per application. For selected applicants, assignment fields are populated.

| Column | Type | Format | Description |
|---|---|---|---|
| application_id | string | APP-XXXXXXXX | Unique application identifier |
| employee_id | string | EMP-XXXXXXXX | FK → applicant |
| gig_id | string | GIG-XXXXXX | FK → raw_gig_master |
| application_date | datetime | YYYY-MM-DD HH:MM:SS | Timestamp of application submission |
| application_status | string | enum | Applied / Shortlisted / Selected / Rejected / Withdrawn |
| manager_approval_flag | string | enum | Yes / No / Not required |
| manager_approval_date | datetime | YYYY-MM-DD HH:MM:SS or empty | When manager approved/rejected |
| assignment_start_date | date | YYYY-MM-DD or empty | Populated for Selected status |
| assignment_end_date | date | YYYY-MM-DD or empty | Populated for Selected status |
| assigned_hours_per_week | integer | or empty | Actual weekly hours (±20% of planned) |

---

## 8. `raw_user_activity_log.csv` — ~549,000 rows

Event-level activity on the talent marketplace platform.

| Column | Type | Format | Description |
|---|---|---|---|
| event_id | string | EVT-XXXXXXXX | Unique event identifier |
| employee_id | string | EMP-XXXXXXXX | FK → raw_employee_master |
| event_type | string | enum | Login / ViewGig / SearchGig / ApplyGig / WithdrawApplication / UpdateProfile / AddSkill / CompleteGig |
| event_timestamp | datetime | YYYY-MM-DD HH:MM:SS | When the event occurred |
| gig_id | string | GIG-XXXXXX or empty | Populated for gig-related events |
| metadata | string | JSON or empty | Optional context (search query, skill_id, etc.) |

---

## 9. `raw_training_master.csv` — 80 rows

Catalogue of available training courses.

| Column | Type | Format | Description |
|---|---|---|---|
| training_id | string | TRN-XXX | Unique training identifier |
| training_name | string | — | Name of the training |
| training_type | string | enum | Course / Workshop / E-learning / Certification |
| provider | string | — | Training provider organisation |

---

## 10. `raw_training_skills.csv` — ~220 rows

Maps each training to the skills it develops (1–5 skills per training).

| Column | Type | Format | Description |
|---|---|---|---|
| training_id | string | TRN-XXX | FK → raw_training_master |
| skill_id | string | SKILL-XXX | FK → dim_skill |

---

## 11. `raw_training_records.csv` — ~41,700 rows

One row per employee training completion.

| Column | Type | Format | Description |
|---|---|---|---|
| employee_id | string | EMP-XXXXXXXX | FK → raw_employee_master |
| training_id | string | TRN-XXX | FK → raw_training_master |
| completion_date | date | YYYY-MM-DD | Date training was completed |
| hours | numeric | — | Hours spent on the training |

---

## Business Logic Notes

- **Promotions** are inferred from `raw_employee_job_assignment` where `job_level` increases across consecutive rows. Gig participants have +2.5pp promotion probability.
- **Churn** is inferred from `termination_date` in `raw_employee_master`. Gig participants have −2.5pp annual churn probability.
- **Skill uplift** from gig completion: 50% chance per required skill for Projects, 35% for Mentoring.
- **Manager hierarchy**: `manager_id` in job assignments points to an employee with a higher job level in most cases.
