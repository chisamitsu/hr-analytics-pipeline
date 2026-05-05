#!/usr/bin/env python3
"""
Talent Marketplace — Synthetic Raw Data Generator
Seed: 12345
"""

import random
import csv
import os
import json
import zipfile
import hashlib
import unicodedata
from datetime import date, datetime, timedelta
from collections import defaultdict

SEED = 12345
random.seed(SEED)

OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "clean")
os.makedirs(OUT_DIR, exist_ok=True)

# ── Reference data ──────────────────────────────────────────────────────────

COUNTRY_WEIGHTS = {
    "Germany": 10.0, "United Kingdom": 8.0, "France": 7.0, "Italy": 6.0,
    "Spain": 6.0, "Netherlands": 4.0, "Switzerland": 3.5, "Sweden": 3.0,
    "Poland": 2.5, "Belgium": 2.0, "Ireland": 1.5, "Austria": 1.2,
    "Denmark": 1.0, "Norway": 0.8, "Finland": 0.5, "Czech Republic": 0.8,
    "Portugal": 0.7, "Romania": 0.6, "Hungary": 0.5,
    "India": 4.5, "China": 4.0, "Japan": 2.5, "South Korea": 1.5,
    "Singapore": 1.0, "United States": 6.0, "Canada": 1.5, "Brazil": 1.0,
    "Mexico": 0.8, "United Arab Emirates": 0.6, "Israel": 0.5,
}
COUNTRIES = list(COUNTRY_WEIGHTS.keys())
COUNTRY_PROBS = [COUNTRY_WEIGHTS[c]/100.0 for c in COUNTRIES]

FUNCTIONS = ["R&D", "Clinical", "Regulatory", "Quality Assurance", "Manufacturing",
             "Supply Chain", "IT", "Finance", "HR", "Procurement", "Marketing", "Sales"]

SUB_FUNCTIONS = {
    "R&D": ["Drug Discovery", "Formulation", "Preclinical Research", "Bioanalysis"],
    "Clinical": ["Clinical Operations", "Data Management", "Biostatistics", "Medical Writing"],
    "Regulatory": ["Regulatory Affairs", "CMC", "Labeling", "Submissions"],
    "Quality Assurance": ["QA Operations", "Auditing", "CAPA", "GxP Compliance"],
    "Manufacturing": ["Production", "Process Development", "Tech Transfer", "Validation"],
    "Supply Chain": ["Demand Planning", "Logistics", "Procurement Ops", "Distribution"],
    "IT": ["Application Dev", "Infrastructure", "Cybersecurity", "Data & Analytics"],
    "Finance": ["FP&A", "Accounting", "Tax", "Treasury"],
    "HR": ["Talent Acquisition", "L&D", "Compensation & Benefits", "HR Business Partner"],
    "Procurement": ["Category Management", "Sourcing", "Vendor Management", "Contracts"],
    "Marketing": ["Brand Management", "Digital Marketing", "Market Research", "Medical Affairs"],
    "Sales": ["Key Account Management", "Regional Sales", "Sales Operations", "Trade Marketing"],
}

BUSINESS_UNITS = ["Pharma Ops", "Global IT", "Commercial", "R&D Division",
                  "Corporate Functions", "Supply Chain & Mfg", "Clinical Development"]

FUNCTION_TO_BU = {
    "R&D": "R&D Division", "Clinical": "Clinical Development",
    "Regulatory": "Pharma Ops", "Quality Assurance": "Pharma Ops",
    "Manufacturing": "Supply Chain & Mfg", "Supply Chain": "Supply Chain & Mfg",
    "IT": "Global IT", "Finance": "Corporate Functions", "HR": "Corporate Functions",
    "Procurement": "Corporate Functions", "Marketing": "Commercial", "Sales": "Commercial",
}

CONTRACT_TYPES = ["Permanent", "Temporary", "Contractor"]
CONTRACT_PROBS = [0.82, 0.12, 0.06]

JOB_LEVEL_DIST = {
    "L1": 0.18, "L2": 0.22, "L3": 0.20, "L4": 0.18, "L5": 0.12, "L6": 0.07, "L7": 0.03,
}
LEVELS = ["L1", "L2", "L3", "L4", "L5", "L6", "L7"]
LEVEL_IDX = {l: i for i, l in enumerate(LEVELS)}

GENDER_DIST = {"Female": 0.53, "Male": 0.46, "Non-binary": 0.005, "Not disclosed": 0.005}

AGE_BUCKETS = [(18, 25, 0.08), (26, 35, 0.32), (36, 45, 0.30), (46, 55, 0.20), (56, 70, 0.10)]

# Name pools by region/gender
FIRST_NAMES_F = {
    "EUR": ["Anna", "Maria", "Julia", "Emma", "Sophie", "Laura", "Elena", "Sara",
            "Isabelle", "Clara", "Lea", "Charlotte", "Mia", "Lena", "Hannah",
            "Franziska", "Amélie", "Giulia", "Valentina", "Marta", "Ines",
            "Katarzyna", "Petra", "Eva", "Monika", "Jana", "Nina", "Silvia",
            "Annika", "Birgit", "Maike", "Sigrid", "Brigitte", "Helene", "Agnes"],
    "ASIA": ["Mei", "Yuki", "Priya", "Aisha", "Ling", "Sakura", "Nisha", "Ananya",
             "Yuna", "Jia", "Rin", "Wei", "Li", "Pei", "Soo", "Min", "Hana",
             "Divya", "Pooja", "Kavya", "Sunita", "Riya"],
    "AME": ["Jennifer", "Ashley", "Samantha", "Brittany", "Megan", "Melissa",
            "Nicole", "Stephanie", "Gabriela", "Valeria", "Camila", "Isabella",
            "Madison", "Olivia", "Ava", "Sofia"],
    "MEO": ["Fatima", "Layla", "Sara", "Nour", "Dina", "Rania", "Yasmin", "Leila",
            "Amal", "Hiba", "Miriam", "Rachel"],
}

FIRST_NAMES_M = {
    "EUR": ["Thomas", "Michael", "Andreas", "Stefan", "Christian", "Markus", "Oliver",
            "Sebastian", "Alexander", "Daniel", "Tobias", "Florian", "Johannes",
            "Nicolas", "Guillaume", "Marco", "Luca", "Alessandro", "Javier",
            "Carlos", "Miguel", "Pierre", "Antoine", "Piotr", "Marek", "Jan",
            "Hans", "Klaus", "Werner", "Rolf", "Henrik", "Lars", "Erik", "Sven"],
    "ASIA": ["Hiroshi", "Kenji", "Raj", "Vikram", "Wei", "Ming", "Jun", "Takeshi",
             "Sanjay", "Arjun", "Soo", "Jin", "Hyun", "Tao", "Lei", "Feng",
             "Patel", "Kumar", "Ravi", "Arun", "Suresh", "Mohan"],
    "AME": ["James", "Robert", "David", "William", "John", "Christopher",
            "Matthew", "Andrew", "Kevin", "Ryan", "Brandon", "Tyler", "Nathan",
            "Diego", "Carlos", "Juan", "Luis", "Miguel", "Eduardo"],
    "MEO": ["Mohammed", "Ahmed", "Omar", "Ali", "Hassan", "Khalid", "Youssef",
            "Nabil", "Tamir", "Oren", "Eitan", "Yaron"],
}

FIRST_NAMES_NB = ["Alex", "Jordan", "Morgan", "Taylor", "Casey", "Riley", "Skyler",
                  "Quinn", "Avery", "Cameron", "Sage", "River", "Phoenix"]

LAST_NAMES = {
    "Germany": ["Müller", "Schmidt", "Schneider", "Fischer", "Weber", "Meyer", "Wagner", "Becker", "Schulz", "Hoffmann"],
    "United Kingdom": ["Smith", "Jones", "Williams", "Taylor", "Brown", "Davies", "Evans", "Wilson", "Thomas", "Roberts"],
    "France": ["Martin", "Bernard", "Dubois", "Thomas", "Robert", "Richard", "Petit", "Durand", "Leroy", "Moreau"],
    "Italy": ["Rossi", "Ferrari", "Russo", "Colombo", "Brambilla", "Romano", "Greco", "Ricci", "Marino", "Bruno"],
    "Spain": ["García", "Martínez", "López", "Sánchez", "Pérez", "González", "Rodríguez", "Fernández", "Torres", "Díaz"],
    "Netherlands": ["de Jong", "Jansen", "de Vries", "van den Berg", "Bakker", "Janssen", "Visser", "Smit", "Meijer", "de Boer"],
    "Switzerland": ["Müller", "Meier", "Schmid", "Keller", "Weber", "Huber", "Steiner", "Moser", "Fischer", "Zimmermann"],
    "Sweden": ["Andersson", "Johansson", "Karlsson", "Nilsson", "Eriksson", "Larsson", "Olsson", "Persson", "Svensson", "Gustafsson"],
    "Poland": ["Kowalski", "Nowak", "Wiśniewski", "Wójcik", "Kowalczyk", "Kamiński", "Lewandowski", "Zieliński", "Woźniak", "Szymański"],
    "Belgium": ["Peeters", "Janssen", "Maes", "Jacobs", "Claes", "Declercq", "Willems", "Smeets", "Goossens", "Leclercq"],
    "Ireland": ["Murphy", "Kelly", "O'Brien", "Walsh", "Smith", "O'Sullivan", "McCarthy", "Byrne", "Ryan", "O'Connor"],
    "Austria": ["Huber", "Gruber", "Bauer", "Wagner", "Müller", "Pichler", "Steiner", "Moser", "Mayer", "Hofer"],
    "Denmark": ["Nielsen", "Jensen", "Hansen", "Pedersen", "Andersen", "Christensen", "Larsen", "Sørensen", "Rasmussen", "Jørgensen"],
    "Norway": ["Hansen", "Johansen", "Olsen", "Larsen", "Andersen", "Pedersen", "Nilsen", "Kristiansen", "Jensen", "Karlsen"],
    "Finland": ["Korhonen", "Virtanen", "Mäkinen", "Nieminen", "Mäkelä", "Hämäläinen", "Laine", "Heikkinen", "Koskinen", "Järvinen"],
    "Czech Republic": ["Novák", "Svoboda", "Novotný", "Dvořák", "Černý", "Procházka", "Krejčí", "Blažek", "Kovář", "Kratochvíl"],
    "Portugal": ["Silva", "Santos", "Ferreira", "Pereira", "Oliveira", "Costa", "Rodrigues", "Martins", "Jesus", "Sousa"],
    "Romania": ["Pop", "Ionescu", "Popa", "Gheorghe", "Stoica", "Ilie", "Dumitrescu", "Constantin", "Stan", "Mihai"],
    "Hungary": ["Nagy", "Kovács", "Tóth", "Szabó", "Horváth", "Varga", "Kiss", "Molnár", "Németh", "Farkas"],
    "India": ["Sharma", "Patel", "Singh", "Kumar", "Gupta", "Verma", "Joshi", "Rao", "Mehta", "Shah"],
    "China": ["Wang", "Li", "Zhang", "Liu", "Chen", "Yang", "Huang", "Zhao", "Wu", "Zhou"],
    "Japan": ["Sato", "Suzuki", "Takahashi", "Tanaka", "Watanabe", "Ito", "Yamamoto", "Nakamura", "Kobayashi", "Kato"],
    "South Korea": ["Kim", "Lee", "Park", "Choi", "Jung", "Kang", "Cho", "Yoon", "Lim", "Han"],
    "Singapore": ["Tan", "Lim", "Lee", "Ng", "Chan", "Wong", "Chen", "Goh", "Chua", "Koh"],
    "United States": ["Johnson", "Williams", "Brown", "Davis", "Miller", "Wilson", "Moore", "Anderson", "Jackson", "Harris"],
    "Canada": ["MacDonald", "Campbell", "Johnston", "Stewart", "MacLeod", "Morrison", "Fraser", "Murray", "Robertson", "Martin"],
    "Brazil": ["Silva", "Santos", "Oliveira", "Souza", "Rodrigues", "Almeida", "Nascimento", "Lima", "Araújo", "Ferreira"],
    "Mexico": ["González", "Hernández", "López", "Martínez", "Pérez", "Ramírez", "Torres", "Flores", "Rivera", "García"],
    "United Arab Emirates": ["Al Mansouri", "Al Hashimi", "Al Maktoum", "Al Nahyan", "Al Falasi", "Al Qasimi", "Al Rashidi", "Al Zaabi"],
    "Israel": ["Cohen", "Levy", "Mizrahi", "Peretz", "Friedman", "Shapiro", "Katz", "Goldberg", "Rosenberg", "Greenberg"],
}

COUNTRY_REGION = {
    "Germany": "EUR", "United Kingdom": "EUR", "France": "EUR", "Italy": "EUR",
    "Spain": "EUR", "Netherlands": "EUR", "Switzerland": "EUR", "Sweden": "EUR",
    "Poland": "EUR", "Belgium": "EUR", "Ireland": "EUR", "Austria": "EUR",
    "Denmark": "EUR", "Norway": "EUR", "Finland": "EUR", "Czech Republic": "EUR",
    "Portugal": "EUR", "Romania": "EUR", "Hungary": "EUR",
    "India": "ASIA", "China": "ASIA", "Japan": "ASIA", "South Korea": "ASIA", "Singapore": "ASIA",
    "United States": "AME", "Canada": "AME", "Brazil": "AME", "Mexico": "AME",
    "United Arab Emirates": "MEO", "Israel": "MEO",
}

# Job catalog
JOB_CATALOG = {}
_job_counter = 1
def make_job_id():
    global _job_counter
    jid = f"JOB-{_job_counter:04d}"
    _job_counter += 1
    return jid

FUNCTION_JOBS = {}
for fn in FUNCTIONS:
    FUNCTION_JOBS[fn] = []
    titles_by_level = {
        "L1": f"{fn} Analyst I", "L2": f"{fn} Analyst II",
        "L3": f"Senior {fn} Specialist", "L4": f"Lead {fn} Specialist",
        "L5": f"{fn} Manager", "L6": f"Senior {fn} Manager",
        "L7": f"{fn} Director",
    }
    for lvl, title in titles_by_level.items():
        jid = make_job_id()
        FUNCTION_JOBS[fn].append({"job_id": jid, "job_title": title, "job_level": lvl})

# Skills catalog
SKILLS_DATA = []
skill_categories = {
    "Technical": ["Analytical Chemistry", "Formulation Science", "Bioassay Development",
                  "PCR Techniques", "Mass Spectrometry", "HPLC Analysis", "Cell Culture",
                  "In Vivo Studies", "Pharmacokinetics", "Toxicology", "Stability Testing",
                  "Method Validation", "GMP Manufacturing", "Process Scale-up", "Aseptic Processing"],
    "Regulatory": ["FDA Submissions", "EMA Submissions", "ICH Guidelines", "GxP Compliance",
                   "Clinical Trial Regulations", "Labeling Requirements", "REMS Management",
                   "Post-Market Surveillance", "Risk Management", "Drug Safety Reporting"],
    "Digital": ["Python Programming", "R Statistical Analysis", "SQL", "Power BI", "Tableau",
                "Machine Learning", "NLP", "Cloud Computing", "API Integration",
                "Data Visualization", "RPA Tools", "Agile Methodology", "DevOps", "Cybersecurity Basics"],
    "Functional": ["Clinical Operations", "Supply Chain Management", "Financial Modeling",
                   "Budget Management", "Vendor Management", "Contract Negotiation",
                   "Medical Writing", "Market Research", "Sales Strategy", "KOL Management",
                   "Demand Planning", "Quality Systems", "Auditing", "CAPA Management",
                   "Change Management", "Project Management"],
    "Leadership": ["Strategic Thinking", "Stakeholder Management", "Cross-functional Leadership",
                   "Executive Communication", "Coaching & Mentoring", "Talent Development",
                   "Organizational Design", "Innovation Management", "Crisis Management",
                   "Diversity & Inclusion", "Negotiation Skills", "Conflict Resolution",
                   "People Management", "Performance Management", "Decision Making"],
}

skill_id_counter = 1
for cat, skill_list in skill_categories.items():
    for sname in skill_list:
        SKILLS_DATA.append({
            "skill_id": f"SKILL-{skill_id_counter:03d}",
            "skill_name": sname,
            "skill_category": cat,
        })
        skill_id_counter += 1
# Pad to 100 if needed
extra_skills = [
    ("Technical", "Biomarker Analysis"), ("Digital", "Blockchain in Pharma"),
    ("Functional", "Regulatory Intelligence"), ("Leadership", "Global Mindset"),
    ("Regulatory", "Health Economics"), ("Digital", "Digital Health"),
    ("Technical", "Gene Therapy Techniques"), ("Functional", "Patient Advocacy"),
    ("Leadership", "Cultural Intelligence"), ("Digital", "AI in Drug Discovery"),
]
for cat, sname in extra_skills:
    if skill_id_counter > 100:
        break
    SKILLS_DATA.append({"skill_id": f"SKILL-{skill_id_counter:03d}", "skill_name": sname, "skill_category": cat})
    skill_id_counter += 1

SKILLS_DATA = SKILLS_DATA[:100]
SKILL_IDS = [s["skill_id"] for s in SKILLS_DATA]

# Training catalog
TRAINING_DATA = []
training_providers = ["Coursera", "LinkedIn Learning", "Internal L&D", "Pharma Academy",
                       "Harvard Online", "MIT OpenCourseWare", "PriceWaterhouseCoopers Academy",
                       "TOPRA", "RAPS", "ISPE", "PDA", "ICH Training"]
training_types = ["Course", "Workshop", "E-learning", "Certification"]
training_names = [
    "GMP Fundamentals", "ICH Q10 Pharmaceutical Quality System", "Clinical Trial Management",
    "Regulatory Strategy for Global Submissions", "Data Integrity in GxP Environments",
    "Python for Data Science", "Advanced SQL for Analytics", "Power BI for HR Analytics",
    "Machine Learning in Drug Discovery", "Digital Transformation in Pharma",
    "Leadership Essentials", "Coaching & Mentoring Skills", "Stakeholder Management",
    "Strategic Decision Making", "Cross-cultural Communication",
    "Supply Chain Risk Management", "Demand Planning Fundamentals", "Vendor Management Excellence",
    "Financial Modeling for Life Sciences", "Budget Planning & Control",
    "Medical Writing Certification", "Pharmacovigilance Essentials", "REMS Compliance",
    "FDA Biologics Submissions", "EMA Regulatory Pathway",
    "Project Management Professional", "Agile in Pharma", "Change Management Foundation",
    "Diversity & Inclusion in the Workplace", "Talent Development Strategies",
    "Clinical Data Management", "Biostatistics Fundamentals", "SAS Programming",
    "R for Life Sciences", "CAPA Management",
    "Quality Risk Management (ICH Q9)", "Aseptic Processing Techniques",
    "Cold Chain & Temperature Control", "Serialization & Track-Trace",
    "Contract Negotiation Skills", "Procurement Excellence",
    "Key Account Management", "Sales Effectiveness", "Market Access Strategy",
    "Health Economics & Outcomes Research", "Medical Affairs Fundamentals",
    "Cybersecurity for Healthcare", "Cloud Computing Essentials", "API Development Basics",
    "RPA with UiPath", "Tableau Desktop Specialist",
    "Ethics in Clinical Research", "Patient Centricity",
    "Rare Disease Drug Development", "Gene & Cell Therapy Basics",
    "Biosimilar Development", "Oncology Drug Development",
    "Pharmacokinetics & Pharmacodynamics", "Toxicology in Drug Development",
    "Formulation Development Fundamentals", "Analytical Chemistry Techniques",
    "GCP Refresher", "GLP Fundamentals", "GDP for Supply Chain",
    "Investigator Site Training", "IRB/Ethics Committee Training",
    "Executive Presence", "Presentation Skills", "Conflict Resolution",
    "Negotiation Mastery", "Team Building & Collaboration",
    "Risk Management Frameworks", "Innovation Management",
    "Sustainability in Pharma Supply Chain", "ESG Reporting Basics",
    "GDPR for HR Professionals", "Labor Law Essentials",
    "Compensation & Benefits Design", "Talent Acquisition Excellence",
    "Performance Management Cycle", "HR Business Partnering",
]
random.shuffle(training_names)
training_names = training_names[:80]
for i, tname in enumerate(training_names, 1):
    TRAINING_DATA.append({
        "training_id": f"TRN-{i:03d}",
        "training_name": tname,
        "training_type": random.choice(training_types),
        "provider": random.choice(training_providers),
    })

# GIG titles
GIG_TITLES_PROJECT = [
    "Supply Chain Optimization Initiative", "Quality System Overhaul",
    "Clinical Data Migration", "ERP System Implementation", "Digital Lab Transformation",
    "Regulatory Submission Acceleration", "Cost Reduction Task Force", "ESG Reporting Setup",
    "Talent Analytics Dashboard", "Sales Force Effectiveness", "Market Access Redesign",
    "AI in Drug Discovery Pilot", "Manufacturing Yield Improvement", "Cold Chain Digitization",
    "Global HR System Rollout", "Cybersecurity Uplift Program", "Data Governance Framework",
    "Procurement Category Review", "Brand Relaunch Initiative", "Clinical Site Expansion",
    "Biosimilar Launch Support", "Oncology Portfolio Review", "R&D Portfolio Prioritization",
    "Lean Six Sigma Deployment", "Patient Engagement Platform", "Medical Affairs Transformation",
    "KAM Excellence Program", "Finance Systems Upgrade", "Global Payroll Consolidation",
    "Diversity & Inclusion Roadmap", "Learning Platform Migration",
]
GIG_TITLES_MENTORING = [
    "Leadership Mentoring – Emerging Talents", "Career Coaching for Scientists",
    "Cross-functional Shadowing Program", "Regulatory Mentorship", "IT Career Mentoring",
    "Clinical Operations Coaching", "Finance Career Development", "HR Business Partner Mentoring",
    "Supply Chain Leadership Mentoring", "Sales Mentoring Program",
    "Data Science Mentoring", "Medical Writing Coaching", "Global Mobility Mentoring",
    "Women in Leadership Mentoring", "Junior Manager Coaching",
    "Scientific Advisor Program", "Strategy & Innovation Mentoring",
]

def remove_accents(s):
    nfkd = unicodedata.normalize('NFKD', s)
    return ''.join(c for c in nfkd if not unicodedata.combining(c))

def rand_date(start: date, end: date) -> date:
    delta = (end - start).days
    if delta <= 0:
        return start
    return start + timedelta(days=random.randint(0, delta))

def rand_datetime(start: date, end: date) -> str:
    d = rand_date(start, end)
    h = random.randint(7, 18)
    m = random.randint(0, 59)
    s = random.randint(0, 59)
    return f"{d} {h:02d}:{m:02d}:{s:02d}"

def weighted_choice(items, weights):
    total = sum(weights)
    r = random.random() * total
    cumulative = 0
    for item, w in zip(items, weights):
        cumulative += w
        if r <= cumulative:
            return item
    return items[-1]

def gen_hex_id(prefix, length=8):
    chars = "0123456789ABCDEF"
    return prefix + ''.join(random.choices(chars, k=length))

def unique_id_gen(prefix, length=8):
    seen = set()
    while True:
        uid = gen_hex_id(prefix, length)
        if uid not in seen:
            seen.add(uid)
            yield uid

emp_id_gen = unique_id_gen("EMP-")
ja_id_gen = unique_id_gen("JA-")
app_id_gen = unique_id_gen("APP-")
evt_id_gen = unique_id_gen("EVT-")

EMP_ID_POOL = [next(emp_id_gen) for _ in range(5000)]
random.shuffle(EMP_ID_POOL)

print("Reference data initialized.")
print(f"Skills: {len(SKILLS_DATA)}, Trainings: {len(TRAINING_DATA)}")
print(f"Job catalog entries: {sum(len(v) for v in FUNCTION_JOBS.values())}")

# ── Step 1: Generate employees ───────────────────────────────────────────────

print("Generating employees...")

REF_DATE = date(2025, 1, 1)  # reference for age calc
WIN_START = date(2023, 1, 1)
WIN_END = date(2025, 12, 31)
HIRE_EARLIEST = date(2018, 1, 1)

def pick_birth_date():
    bucket = weighted_choice(
        [(b[0], b[1]) for b in AGE_BUCKETS],
        [b[2] for b in AGE_BUCKETS]
    )
    age = random.randint(bucket[0], bucket[1])
    year = REF_DATE.year - age
    month = random.randint(1, 12)
    day = random.randint(1, 28)
    return date(year, month, day)

def pick_country():
    return weighted_choice(COUNTRIES, COUNTRY_PROBS)

def pick_gender():
    r = random.random()
    if r < 0.53: return "Female"
    elif r < 0.99: return "Male"
    elif r < 0.995: return "Non-binary"
    else: return "Not disclosed"

def pick_name(gender, country):
    region = COUNTRY_REGION.get(country, "EUR")
    nb_override = gender in ("Non-binary", "Not disclosed")
    
    if nb_override:
        first = random.choice(FIRST_NAMES_NB)
    elif gender == "Female":
        pool = FIRST_NAMES_F.get(region, FIRST_NAMES_F["EUR"])
        first = random.choice(pool)
        # 1-2% mismatch: occasionally use male name
        if random.random() < 0.015:
            first = random.choice(FIRST_NAMES_M.get(region, FIRST_NAMES_M["EUR"]))
    else:
        pool = FIRST_NAMES_M.get(region, FIRST_NAMES_M["EUR"])
        first = random.choice(pool)
        if random.random() < 0.015:
            first = random.choice(FIRST_NAMES_F.get(region, FIRST_NAMES_F["EUR"]))
    
    last_pool = LAST_NAMES.get(country, LAST_NAMES["United States"])
    last = random.choice(last_pool)
    return first, last

def normalize_email_part(s):
    s = remove_accents(s).lower()
    s = s.replace("'", "").replace(" ", ".")
    return ''.join(c for c in s if c.isalnum() or c == '.')

def pick_job_level_at_hire():
    probs = list(JOB_LEVEL_DIST.values())
    return weighted_choice(LEVELS, probs)

def pick_hire_date():
    # ~60% hired before window, ~40% during
    if random.random() < 0.60:
        return rand_date(HIRE_EARLIEST, WIN_START - timedelta(days=1))
    else:
        return rand_date(WIN_START, WIN_END - timedelta(days=90))

def churn_prob(level, has_gig_participation):
    base = 0.10
    level_adj = {"L1": 0.03, "L2": 0.02, "L3": 0.01, "L4": 0.0, "L5": -0.01, "L6": -0.015, "L7": -0.02}
    p = base + level_adj.get(level, 0)
    if has_gig_participation:
        p -= 0.025
    return max(0.01, min(p, 0.30))

employees = []
email_count = defaultdict(int)

for i, emp_id in enumerate(EMP_ID_POOL):
    country = pick_country()
    gender = pick_gender()
    birth_date = pick_birth_date()
    first, last = pick_name(gender, country)
    
    base_email = f"{normalize_email_part(first)}.{normalize_email_part(last)}@example.com"
    email_count[base_email] += 1
    count = email_count[base_email]
    if count == 1:
        email = base_email
    else:
        email = base_email.replace("@example.com", f".{count-1}@example.com")
    
    hire_date = pick_hire_date()
    # Ensure hire_date > birth_date + 18 years
    min_hire = date(birth_date.year + 18, birth_date.month, birth_date.day)
    if hire_date < min_hire:
        hire_date = min_hire + timedelta(days=random.randint(0, 365))
    
    job_level_at_hire = pick_job_level_at_hire()
    contract_type = weighted_choice(CONTRACT_TYPES, CONTRACT_PROBS)
    
    # Compute age at ref date
    age = REF_DATE.year - birth_date.year
    
    # Churn decision (will be refined after gig assignment, but placeholder)
    # Annual churn probability over the window (up to 3 years)
    # We'll compute termination later; for now store base fields
    employees.append({
        "employee_id": emp_id,
        "first_name": first,
        "last_name": last,
        "company_email": email,
        "birth_date": birth_date,
        "gender": gender,
        "country_of_birth": country,
        "hire_date": hire_date,
        "termination_date": None,
        "contract_type": contract_type,
        "employment_status": "Active",
        "job_level_at_hire": job_level_at_hire,
        "_age": age,
    })

print(f"Generated {len(employees)} employees")

# ── Step 2: Job Assignments (SCD2) ───────────────────────────────────────────
print("Generating job assignments...")

# Build employee lookup
EMP_MAP = {e["employee_id"]: e for e in employees}
EMP_IDS = [e["employee_id"] for e in employees]

# Assign functions to employees (stable function per employee)
FUNC_WEIGHTS = [1.0] * len(FUNCTIONS)  # uniform
EMP_FUNCTION = {}
EMP_COUNTRY = {}
for e in employees:
    EMP_FUNCTION[e["employee_id"]] = random.choice(FUNCTIONS)
    EMP_COUNTRY[e["employee_id"]] = e["country_of_birth"]

job_assignments = []
# Track current level for each employee (for promotion logic)
EMP_CURRENT_LEVEL = {}
EMP_MANAGER = {}  # emp_id -> manager_id

# First pass: build manager pool by level
MANAGERS_BY_LEVEL = defaultdict(list)
for e in employees:
    MANAGERS_BY_LEVEL[e["job_level_at_hire"]].append(e["employee_id"])

def get_manager(emp_id, level):
    level_idx = LEVEL_IDX[level]
    # Manager must be higher level
    for higher_lvl in LEVELS[level_idx+1:]:
        pool = MANAGERS_BY_LEVEL[higher_lvl]
        if pool:
            mgr = random.choice(pool)
            if mgr != emp_id:
                return mgr
    # Fallback: any L6/L7
    for lvl in ["L7", "L6", "L5"]:
        pool = MANAGERS_BY_LEVEL[lvl]
        if pool:
            mgr = random.choice(pool)
            if mgr != emp_id:
                return mgr
    return None

def promotion_prob(level, gig_participant, tenure_years):
    base = 0.10
    if level in ("L6", "L7"):
        base = 0.04
    elif level in ("L1", "L2"):
        base = 0.12
    tenure_adj = min(0.03, tenure_years * 0.005)
    p = base + tenure_adj
    if gig_participant:
        p += 0.025
    return min(p, 0.30)

# Generate job assignments per employee
for e in employees:
    emp_id = e["employee_id"]
    hire_date = e["hire_date"]
    fn = EMP_FUNCTION[emp_id]
    current_level = e["job_level_at_hire"]
    EMP_CURRENT_LEVEL[emp_id] = current_level
    
    # Active assignment start: hire_date (or 2023-01-01 if hired before window)
    assign_start = hire_date
    
    mgr = get_manager(emp_id, current_level)
    EMP_MANAGER[emp_id] = mgr
    
    # First assignment
    job_entry = random.choice(FUNCTION_JOBS[fn])
    # Find matching level
    level_jobs = [j for j in FUNCTION_JOBS[fn] if j["job_level"] == current_level]
    if level_jobs:
        job_entry = random.choice(level_jobs)
    
    sub_fn = random.choice(SUB_FUNCTIONS[fn])
    bu = FUNCTION_TO_BU[fn]
    emp_country = EMP_COUNTRY[emp_id]
    
    # 0-3 changes during window
    num_changes = random.choices([0, 1, 2, 3], weights=[0.5, 0.3, 0.15, 0.05])[0]
    
    segments = []
    seg_start = assign_start
    
    for ch in range(num_changes + 1):
        is_last = (ch == num_changes)
        if is_last:
            seg_end = None  # open
        else:
            # End somewhere in the window
            max_end = WIN_END
            min_seg = seg_start + timedelta(days=90)
            if min_seg >= max_end:
                seg_end = None
                is_last = True
            else:
                seg_end = rand_date(min_seg, max_end - timedelta(days=30))
        
        segments.append((seg_start, seg_end, current_level, job_entry, sub_fn, bu, mgr))
        
        if not is_last and seg_end is not None:
            # Possible promotion
            tenure_years = (seg_end - hire_date).days / 365.0
            gig_p = False  # will refine later
            pp = promotion_prob(current_level, gig_p, tenure_years)
            if random.random() < pp and LEVEL_IDX[current_level] < len(LEVELS) - 1:
                current_level = LEVELS[LEVEL_IDX[current_level] + 1]
                EMP_CURRENT_LEVEL[emp_id] = current_level
                mgr = get_manager(emp_id, current_level)
                EMP_MANAGER[emp_id] = mgr
            
            level_jobs = [j for j in FUNCTION_JOBS[fn] if j["job_level"] == current_level]
            if level_jobs:
                job_entry = random.choice(level_jobs)
            sub_fn = random.choice(SUB_FUNCTIONS[fn])
            seg_start = seg_end + timedelta(days=1)
    
    for seg_start, seg_end, lvl, je, sub_fn, bu, mgr in segments:
        ja_id = next(ja_id_gen)
        job_assignments.append({
            "employee_id": emp_id,
            "job_assignment_id": ja_id,
            "job_id": je["job_id"],
            "job_title": je["job_title"],
            "job_level": lvl,
            "function": fn,
            "sub_function": sub_fn,
            "business_unit": bu,
            "manager_id": mgr if mgr else "",
            "employment_country": emp_country,
            "start_date": seg_start,
            "end_date": seg_end if seg_end else "",
        })

print(f"Generated {len(job_assignments)} job assignment rows")

# ── Step 3: Gig master + required skills ────────────────────────────────────
print("Generating gigs...")

# Senior employees more likely to own gigs
SENIOR_IDS = [e["employee_id"] for e in employees if e["job_level_at_hire"] in ("L4","L5","L6","L7")]
ALL_IDS_FOR_GIGS = SENIOR_IDS + SENIOR_IDS + [e["employee_id"] for e in employees]  # weight seniors 2x

gig_masters = []
gig_required_skills = []
gig_id_counter = 1

for year in [2023, 2024, 2025]:
    for _ in range(500):
        gig_id = f"GIG-{gig_id_counter:06d}"
        gig_id_counter += 1
        
        gig_type = random.choices(["Project", "Mentoring"], weights=[0.70, 0.30])[0]
        
        if gig_type == "Project":
            title = random.choice(GIG_TITLES_PROJECT) + f" {year}"
        else:
            title = random.choice(GIG_TITLES_MENTORING) + f" {year}"
        
        # Hours: mentoring lower
        if gig_type == "Mentoring":
            hours_pw = random.randint(2, 8)
        else:
            hours_pw = random.randint(6, 20)
        
        duration_w = random.randint(1, 26)
        
        owner_id = random.choice(ALL_IDS_FOR_GIGS)
        creator_id = owner_id if random.random() < 0.85 else random.choice(ALL_IDS_FOR_GIGS)
        
        year_start = date(year, 1, 1)
        year_end = date(year, 11, 30)  # leave room for application period
        posted_date = rand_date(year_start, year_end)
        
        fn = EMP_FUNCTION.get(owner_id, random.choice(FUNCTIONS))
        bu = FUNCTION_TO_BU[fn]
        
        gig_masters.append({
            "gig_id": gig_id,
            "gig_title": title,
            "gig_type": gig_type,
            "owner_employee_id": owner_id,
            "hours_per_week_planned": hours_pw,
            "duration_weeks_planned": duration_w,
            "business_unit": bu,
            "posted_date": posted_date,
            "created_by_employee_id": creator_id,
        })
        
        # Required skills: 2-6
        n_skills = random.randint(2, 6)
        req_skills = random.sample(SKILL_IDS, n_skills)
        for sid in req_skills:
            gig_required_skills.append({"gig_id": gig_id, "skill_id": sid})

GIG_MAP = {g["gig_id"]: g for g in gig_masters}
GIG_IDS = [g["gig_id"] for g in gig_masters]
GIG_REQ_SKILLS = defaultdict(list)
for row in gig_required_skills:
    GIG_REQ_SKILLS[row["gig_id"]].append(row["skill_id"])

print(f"Generated {len(gig_masters)} gigs, {len(gig_required_skills)} gig-skill rows")

# ── Step 4: Applications & Assignments ──────────────────────────────────────
print("Generating applications and assignments...")

applications = []
# Track which employees got selected for gigs and when
EMP_GIG_ASSIGNMENTS = defaultdict(list)  # emp_id -> list of (gig_id, start, end)

for gig in gig_masters:
    gig_id = gig["gig_id"]
    posted = gig["posted_date"]
    hours_pw = gig["hours_per_week_planned"]
    dur_weeks = gig["duration_weeks_planned"]
    
    # Number of applications: skewed 5-30
    n_apps = random.choices(
        list(range(5, 31)),
        weights=[max(1, 30 - abs(i - 12)) for i in range(5, 31)]
    )[0]
    
    # Pick applicants (exclude owner)
    eligible = [eid for eid in EMP_IDS if eid != gig["owner_employee_id"]]
    applicants = random.sample(eligible, min(n_apps, len(eligible)))
    
    # One selected, rest distributed among statuses
    selected_idx = random.randint(0, len(applicants) - 1)
    
    for idx, emp_id in enumerate(applicants):
        app_id = next(app_id_gen)
        
        app_date_raw = rand_date(posted, posted + timedelta(days=30))
        app_dt = f"{app_date_raw} {random.randint(7,18):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}"
        
        # Status assignment
        if idx == selected_idx:
            status = "Selected"
        else:
            status = random.choices(
                ["Applied", "Shortlisted", "Rejected", "Withdrawn"],
                weights=[0.30, 0.20, 0.35, 0.15]
            )[0]
        
        # Manager approval
        emp_level = EMP_CURRENT_LEVEL.get(emp_id, "L2")
        level_num = LEVEL_IDX.get(emp_level, 1)
        
        needs_approval = random.random() < 0.40
        if not needs_approval:
            mgr_approval_flag = "Not required"
            mgr_approval_date = ""
        else:
            # Higher level → more likely approved
            approve_p = 0.5 + level_num * 0.07
            if random.random() < approve_p:
                mgr_approval_flag = "Yes"
                approval_dt = app_date_raw + timedelta(days=random.randint(1, 5))
                mgr_approval_date = f"{approval_dt} {random.randint(8,17):02d}:{random.randint(0,59):02d}:00"
            else:
                mgr_approval_flag = "No"
                if status == "Selected":
                    status = "Rejected"
                approval_dt = app_date_raw + timedelta(days=random.randint(1, 7))
                mgr_approval_date = f"{approval_dt} {random.randint(8,17):02d}:{random.randint(0,59):02d}:00"
        
        # Assignment dates for selected
        if status == "Selected":
            assign_start = app_date_raw + timedelta(days=random.randint(3, 14))
            actual_weeks = max(1, dur_weeks + random.randint(-2, 2))
            assign_end = assign_start + timedelta(weeks=actual_weeks) + timedelta(days=random.randint(-7, 7))
            assigned_hours = max(1, min(40, int(hours_pw * random.uniform(0.8, 1.2))))
            
            EMP_GIG_ASSIGNMENTS[emp_id].append({
                "gig_id": gig_id,
                "start": assign_start,
                "end": assign_end,
            })
        else:
            assign_start = ""
            assign_end = ""
            assigned_hours = ""
        
        applications.append({
            "application_id": app_id,
            "employee_id": emp_id,
            "gig_id": gig_id,
            "application_date": app_dt,
            "application_status": status,
            "manager_approval_flag": mgr_approval_flag,
            "manager_approval_date": mgr_approval_date,
            "assignment_start_date": assign_start,
            "assignment_end_date": assign_end,
            "assigned_hours_per_week": assigned_hours,
        })

print(f"Generated {len(applications)} applications")
print(f"Employees with gig assignments: {len(EMP_GIG_ASSIGNMENTS)}")

# ── Step 5: Apply churn with gig effect, set termination dates ───────────────
print("Applying churn logic...")

GIG_PARTICIPANTS = set(EMP_GIG_ASSIGNMENTS.keys())

for e in employees:
    emp_id = e["employee_id"]
    level = e["job_level_at_hire"]
    hire_date = e["hire_date"]
    has_gig = emp_id in GIG_PARTICIPANTS
    
    # Age-based: near retirement
    age = e["_age"]
    if age >= 62 and random.random() < 0.15:
        e["employment_status"] = "Retired"
        ret_date = rand_date(WIN_START, WIN_END)
        if ret_date > hire_date:
            e["termination_date"] = ret_date
        continue
    
    # Annual churn probability
    cp = churn_prob(level, has_gig)
    # Over 3-year window: probability of churning at least once
    p_churn_window = 1 - (1 - cp) ** 3
    
    if random.random() < p_churn_window:
        # Pick termination date
        term_date = rand_date(
            max(hire_date + timedelta(days=90), WIN_START),
            date(2026, 3, 31)
        )
        if term_date > hire_date:
            e["termination_date"] = term_date
            e["employment_status"] = "Terminated"
    
    # Small leave probability
    if e["employment_status"] == "Active" and random.random() < 0.03:
        e["employment_status"] = "Leave"

print("Churn applied.")

# ── Step 6: Employee Skills ──────────────────────────────────────────────────
print("Generating employee skills...")

employee_skills = []
# Track skills per employee to avoid duplicates
EMP_SKILLS = defaultdict(dict)  # emp_id -> {skill_id: {level, source, date}}

def add_skill(emp_id, skill_id, level, source, added_date):
    existing = EMP_SKILLS[emp_id].get(skill_id)
    if existing is None or level > existing["skill_level"]:
        EMP_SKILLS[emp_id][skill_id] = {
            "skill_level": level,
            "skill_source": source,
            "added_date": added_date,
        }

# Initial skills at hire
for e in employees:
    emp_id = e["employee_id"]
    hire_date = e["hire_date"]
    n_init = random.randint(3, 10)
    init_skills = random.sample(SKILL_IDS, n_init)
    for sid in init_skills:
        added = hire_date + timedelta(days=random.randint(0, 30))
        source = random.choices(["Self", "Manager"], weights=[0.6, 0.4])[0]
        add_skill(emp_id, sid, random.randint(1, 4), source, added)

# Skill uplift from completed gig assignments
for emp_id, assignments in EMP_GIG_ASSIGNMENTS.items():
    for asgn in assignments:
        gig_id = asgn["gig_id"]
        end_date = asgn["end"]
        req_skills = GIG_REQ_SKILLS.get(gig_id, [])
        gig_type = GIG_MAP[gig_id]["gig_type"]
        uplift_p = 0.50 if gig_type == "Project" else 0.35
        for sid in req_skills:
            if random.random() < uplift_p:
                new_level = random.randint(2, 5)
                add_skill(emp_id, sid, new_level, "Gig", end_date)

# Flatten
for emp_id, skills_dict in EMP_SKILLS.items():
    for skill_id, info in skills_dict.items():
        employee_skills.append({
            "employee_id": emp_id,
            "skill_id": skill_id,
            "skill_level": info["skill_level"],
            "skill_source": info["skill_source"],
            "added_date": info["added_date"],
        })

print(f"Generated {len(employee_skills)} employee skill rows")

# ── Step 7: Training records ─────────────────────────────────────────────────
print("Generating training records...")

TRAINING_SKILLS_MAP = defaultdict(list)
training_skills_rows = []

for t in TRAINING_DATA:
    tid = t["training_id"]
    n_skills = random.randint(1, 5)
    t_skills = random.sample(SKILL_IDS, n_skills)
    TRAINING_SKILLS_MAP[tid] = t_skills
    for sid in t_skills:
        training_skills_rows.append({"training_id": tid, "skill_id": sid})

training_records = []

for e in employees:
    emp_id = e["employee_id"]
    level = e["job_level_at_hire"]
    hire_date = e["hire_date"]
    
    # Junior employees take more trainings
    level_num = LEVEL_IDX.get(level, 2)
    base_trainings = max(1, 8 - level_num)
    n_trainings = random.randint(base_trainings, base_trainings + 5)
    
    selected_trainings = random.sample(TRAINING_DATA, min(n_trainings, len(TRAINING_DATA)))
    
    for t in selected_trainings:
        tid = t["training_id"]
        comp_date = rand_date(
            max(hire_date, WIN_START - timedelta(days=365)),
            WIN_END
        )
        hours = round(random.uniform(2.0, 40.0), 1)
        
        training_records.append({
            "employee_id": emp_id,
            "training_id": tid,
            "completion_date": comp_date,
            "hours": hours,
        })
        
        # Add skills from training
        for sid in TRAINING_SKILLS_MAP[tid]:
            if random.random() < 0.60:
                added = comp_date + timedelta(days=random.randint(0, 14))
                add_skill(emp_id, sid, random.randint(1, 4), "Training", added)

# Rebuild employee_skills with training additions
employee_skills = []
for emp_id, skills_dict in EMP_SKILLS.items():
    for skill_id, info in skills_dict.items():
        employee_skills.append({
            "employee_id": emp_id,
            "skill_id": skill_id,
            "skill_level": info["skill_level"],
            "skill_source": info["skill_source"],
            "added_date": info["added_date"],
        })

print(f"Generated {len(training_records)} training records")
print(f"Generated {len(training_skills_rows)} training-skill rows")
print(f"Total employee skill rows: {len(employee_skills)}")

# ── Step 8: User Activity Log ────────────────────────────────────────────────
print("Generating user activity log...")

# Build application lookup for ApplyGig events
APP_BY_EMP_GIG = defaultdict(list)
for app in applications:
    APP_BY_EMP_GIG[(app["employee_id"], app["gig_id"])].append(app)

activity_log = []

# Monthly active users: target 20-40% per month
# Generate login + browsing events, with bursts around gig postings

# Build posted date index
GIG_POSTED_BY_MONTH = defaultdict(list)
for g in gig_masters:
    key = (g["posted_date"].year, g["posted_date"].month)
    GIG_POSTED_BY_MONTH[key].append(g["gig_id"])

def add_event(emp_id, etype, ts, gig_id="", meta=None):
    activity_log.append({
        "event_id": next(evt_id_gen),
        "employee_id": emp_id,
        "event_type": etype,
        "event_timestamp": ts,
        "gig_id": gig_id,
        "metadata": json.dumps(meta) if meta else "",
    })

# Seasonal weights by month (lower in Aug, Dec)
MONTH_WEIGHTS = {1:1.0,2:1.05,3:1.1,4:1.05,5:1.0,6:0.95,
                 7:0.90,8:0.75,9:1.05,10:1.1,11:1.0,12:0.80}

# Select active users per month (20-40%)
all_months = []
for year in [2023, 2024, 2025]:
    for month in range(1, 13):
        all_months.append((year, month))

ACTIVE_USERS_PER_MONTH = {}
for (yr, mo) in all_months:
    pct = random.uniform(0.20, 0.40) * MONTH_WEIGHTS.get(mo, 1.0)
    n = int(5000 * min(pct, 0.42))
    active = random.sample(EMP_IDS, n)
    ACTIVE_USERS_PER_MONTH[(yr, mo)] = active

# Generate apply events from applications
APP_EVENTS = set()
for app in applications:
    emp_id = app["employee_id"]
    gig_id = app["gig_id"]
    app_dt = app["application_date"]
    
    if app["application_status"] != "Withdrawn":
        add_event(emp_id, "ApplyGig", app_dt, gig_id, {"application_id": app["application_id"]})
        APP_EVENTS.add((emp_id, gig_id))
    else:
        add_event(emp_id, "ApplyGig", app_dt, gig_id)
        # Withdraw event
        withdraw_dt_d = date.fromisoformat(app_dt[:10]) + timedelta(days=random.randint(1, 5))
        withdraw_dt = f"{withdraw_dt_d} {random.randint(8,17):02d}:{random.randint(0,59):02d}:00"
        add_event(emp_id, "WithdrawApplication", withdraw_dt, gig_id)

# Generate CompleteGig events
for emp_id, assignments in EMP_GIG_ASSIGNMENTS.items():
    for asgn in assignments:
        end_d = asgn["end"]
        # Add ±2 days jitter
        complete_d = end_d + timedelta(days=random.randint(-2, 2))
        complete_dt = f"{complete_d} {random.randint(8,17):02d}:{random.randint(0,59):02d}:00"
        add_event(emp_id, "CompleteGig", complete_dt, asgn["gig_id"])

# Generate login + browse events for monthly active users
for (yr, mo) in all_months:
    active = ACTIVE_USERS_PER_MONTH[(yr, mo)]
    month_start = date(yr, mo, 1)
    import calendar
    last_day = calendar.monthrange(yr, mo)[1]
    month_end = date(yr, mo, last_day)
    
    for emp_id in active:
        # 2-8 logins per month
        n_logins = random.randint(2, 8)
        for _ in range(n_logins):
            login_d = rand_date(month_start, month_end)
            login_dt = f"{login_d} {random.randint(7,19):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}"
            add_event(emp_id, "Login", login_dt)
            
            # After login: possible view/search gigs
            if random.random() < 0.60:
                view_d = login_d
                view_dt = f"{view_d} {random.randint(7,19):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}"
                posted_gigs = GIG_POSTED_BY_MONTH.get((yr, mo), GIG_IDS[:20])
                if posted_gigs:
                    view_gig = random.choice(posted_gigs)
                    add_event(emp_id, "ViewGig", view_dt, view_gig)
            
            if random.random() < 0.30:
                search_dt = f"{login_d} {random.randint(7,19):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}"
                add_event(emp_id, "SearchGig", search_dt, "", {"query": random.choice(["clinical", "digital", "leadership", "supply chain", "IT"])})
        
        # Profile update occasionally
        if random.random() < 0.10:
            upd_d = rand_date(month_start, month_end)
            upd_dt = f"{upd_d} {random.randint(8,17):02d}:{random.randint(0,59):02d}:00"
            add_event(emp_id, "UpdateProfile", upd_dt)
        
        # AddSkill occasionally
        if random.random() < 0.08:
            upd_d = rand_date(month_start, month_end)
            upd_dt = f"{upd_d} {random.randint(8,17):02d}:{random.randint(0,59):02d}:00"
            add_event(emp_id, "AddSkill", upd_dt, "", {"skill_id": random.choice(SKILL_IDS)})

print(f"Generated {len(activity_log)} activity log rows")

# ── Step 9: Write all CSV files ──────────────────────────────────────────────
print("Writing CSV files...")

import csv

def write_csv(path, fieldnames, rows):
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Wrote {path} ({len(rows)} rows)")

# 1. raw_employee_master.csv
emp_rows = []
for e in employees:
    emp_rows.append({
        "employee_id": e["employee_id"],
        "first_name": e["first_name"],
        "last_name": e["last_name"],
        "company_email": e["company_email"],
        "birth_date": e["birth_date"],
        "gender": e["gender"],
        "country_of_birth": e["country_of_birth"],
        "hire_date": e["hire_date"],
        "termination_date": e["termination_date"] if e["termination_date"] else "",
        "contract_type": e["contract_type"],
        "employment_status": e["employment_status"],
        "job_level_at_hire": e["job_level_at_hire"],
    })
write_csv(f"{OUT_DIR}/raw_employee_master.csv",
    ["employee_id","first_name","last_name","company_email","birth_date","gender",
     "country_of_birth","hire_date","termination_date","contract_type","employment_status","job_level_at_hire"],
    emp_rows)

# 2. raw_employee_job_assignment.csv
write_csv(f"{OUT_DIR}/raw_employee_job_assignment.csv",
    ["employee_id","job_assignment_id","job_id","job_title","job_level","function",
     "sub_function","business_unit","manager_id","employment_country","start_date","end_date"],
    job_assignments)

# 3. dim_skill.csv
write_csv(f"{OUT_DIR}/dim_skill.csv",
    ["skill_id","skill_name","skill_category"],
    SKILLS_DATA)

# 4. raw_employee_skills.csv
write_csv(f"{OUT_DIR}/raw_employee_skills.csv",
    ["employee_id","skill_id","skill_level","skill_source","added_date"],
    employee_skills)

# 5. raw_gig_master.csv
write_csv(f"{OUT_DIR}/raw_gig_master.csv",
    ["gig_id","gig_title","gig_type","owner_employee_id","hours_per_week_planned",
     "duration_weeks_planned","business_unit","posted_date","created_by_employee_id"],
    gig_masters)

# 6. raw_gig_required_skills.csv
write_csv(f"{OUT_DIR}/raw_gig_required_skills.csv",
    ["gig_id","skill_id"],
    gig_required_skills)

# 7. raw_gig_applications_and_assignments.csv
write_csv(f"{OUT_DIR}/raw_gig_applications_and_assignments.csv",
    ["application_id","employee_id","gig_id","application_date","application_status",
     "manager_approval_flag","manager_approval_date","assignment_start_date",
     "assignment_end_date","assigned_hours_per_week"],
    applications)

# 8. raw_user_activity_log.csv
write_csv(f"{OUT_DIR}/raw_user_activity_log.csv",
    ["event_id","employee_id","event_type","event_timestamp","gig_id","metadata"],
    activity_log)

# 9. raw_training_master.csv
write_csv(f"{OUT_DIR}/raw_training_master.csv",
    ["training_id","training_name","training_type","provider"],
    TRAINING_DATA)

# 10. raw_training_skills.csv
write_csv(f"{OUT_DIR}/raw_training_skills.csv",
    ["training_id","skill_id"],
    training_skills_rows)

# 11. raw_training_records.csv
write_csv(f"{OUT_DIR}/raw_training_records.csv",
    ["employee_id","training_id","completion_date","hours"],
    training_records)

print("All CSVs written.")
