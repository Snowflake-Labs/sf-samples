"""
Heuristic-Based SOAP Data Generation Utilities

Fast template-based generation of synthetic clinical visit dialogues and SOAP summaries.
Uses predefined templates and data pools instead of LLM calls for speed.

This module provides the core generation logic and can be imported by CLI scripts.
"""

import random
from typing import Dict, List, Optional, Tuple

from snowflake.snowpark import DataFrame, Session


# =============================================================================
# Diversity Grid Categories
# =============================================================================

SPECIALTIES = [
    "Cardiology", "Pediatrics", "Orthopedics", "Neurology", "Oncology",
    "Dermatology", "Gastroenterology", "Pulmonology", "Endocrinology",
    "Rheumatology", "Nephrology", "Psychiatry", "Ophthalmology",
    "Otolaryngology", "Urology", "Obstetrics", "Gynecology",
    "Infectious Disease", "Hematology", "Allergy and Immunology",
    "Family Medicine", "Internal Medicine", "Emergency Medicine",
    "Geriatrics", "Sports Medicine"
]

CONDITION_TYPES = [
    "Acute illness",
    "Chronic disease management",
    "Preventive care",
    "Follow-up visit",
    "Emergency presentation"
]

AGE_GROUPS = [
    "Pediatric (0-5 years)",
    "Child (6-12 years)",
    "Adolescent (13-17 years)",
    "Young adult (18-35 years)",
    "Middle-aged adult (36-55 years)",
    "Older adult (56-70 years)",
    "Elderly (71+ years)"
]

VISIT_CONTEXTS = [
    "New patient intake",
    "Follow-up appointment",
    "Specialist referral",
    "Routine checkup",
    "Urgent care visit"
]


# =============================================================================
# Name Pools
# =============================================================================

DOCTOR_FIRST_NAMES = [
    "James", "Sarah", "Michael", "Emily", "David", "Jennifer", "Robert", "Lisa",
    "William", "Amanda", "Richard", "Jessica", "Thomas", "Ashley", "Christopher",
    "Nicole", "Daniel", "Stephanie", "Matthew", "Elizabeth", "Andrew", "Michelle",
    "Joseph", "Kimberly", "Charles", "Melissa", "Steven", "Laura", "Kevin", "Maria",
    "Brian", "Anna", "Edward", "Rachel", "George", "Katherine", "Timothy", "Patricia",
    "Ronald", "Linda", "Jason", "Barbara", "Jeffrey", "Susan", "Ryan", "Margaret",
    "Jacob", "Dorothy", "Gary", "Karen"
]

DOCTOR_LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker",
    "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
    "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell", "Carter"
]

PATIENT_FIRST_NAMES = [
    "Emma", "Liam", "Olivia", "Noah", "Ava", "Ethan", "Sophia", "Mason",
    "Isabella", "William", "Mia", "James", "Charlotte", "Benjamin", "Amelia",
    "Lucas", "Harper", "Henry", "Evelyn", "Alexander", "Abigail", "Michael",
    "Emily", "Daniel", "Elizabeth", "Jacob", "Sofia", "Logan", "Avery", "Jackson",
    "Ella", "Sebastian", "Scarlett", "Jack", "Grace", "Aiden", "Chloe", "Owen",
    "Victoria", "Samuel", "Riley", "Ryan", "Aria", "Nathan", "Lily", "John",
    "Aurora", "Luke", "Zoey", "Dylan", "Penelope", "Caleb", "Layla", "Isaac",
    "Nora", "Anthony", "Camila", "Grayson", "Hannah", "Eli", "Lillian", "Jayden",
    "Addison", "Charles", "Eleanor", "Joshua", "Natalie", "Christopher", "Luna",
    "Andrew", "Savannah", "Theodore", "Brooklyn", "Isaiah", "Leah", "Matthew",
    "Zoe", "David", "Stella", "Joseph", "Hazel", "Carter", "Ellie", "Wyatt",
    "Paisley", "Julian", "Audrey", "Gabriel", "Skylar", "Anthony", "Violet",
    "Lincoln", "Claire", "Jaxon", "Bella", "Levi", "Lucy", "Mateo", "Anna"
]

PATIENT_LAST_NAMES = DOCTOR_LAST_NAMES  # Reuse


# =============================================================================
# Specialty-Specific Data Pools
# =============================================================================

CHIEF_COMPLAINTS = {
    "Cardiology": [
        ("chest pain", "angina pectoris"),
        ("shortness of breath", "dyspnea on exertion"),
        ("palpitations", "arrhythmia"),
        ("leg swelling", "peripheral edema"),
        ("fatigue and dizziness", "suspected heart failure"),
    ],
    "Pediatrics": [
        ("fever and cough", "upper respiratory infection"),
        ("ear pain", "acute otitis media"),
        ("rash", "viral exanthem"),
        ("stomach ache", "viral gastroenteritis"),
        ("sore throat", "pharyngitis"),
    ],
    "Orthopedics": [
        ("knee pain", "osteoarthritis"),
        ("back pain", "lumbar strain"),
        ("shoulder pain", "rotator cuff tendinitis"),
        ("ankle injury", "ankle sprain"),
        ("hip pain", "bursitis"),
    ],
    "Neurology": [
        ("headaches", "migraine"),
        ("numbness in hands", "carpal tunnel syndrome"),
        ("dizziness", "benign positional vertigo"),
        ("memory concerns", "mild cognitive impairment"),
        ("tremor", "essential tremor"),
    ],
    "Dermatology": [
        ("skin rash", "eczema"),
        ("acne", "acne vulgaris"),
        ("suspicious mole", "atypical nevus"),
        ("itchy skin", "contact dermatitis"),
        ("hair loss", "alopecia areata"),
    ],
    "Gastroenterology": [
        ("abdominal pain", "irritable bowel syndrome"),
        ("heartburn", "gastroesophageal reflux disease"),
        ("nausea and vomiting", "gastritis"),
        ("constipation", "chronic constipation"),
        ("diarrhea", "infectious colitis"),
    ],
    "Pulmonology": [
        ("chronic cough", "chronic bronchitis"),
        ("wheezing", "asthma exacerbation"),
        ("shortness of breath", "COPD"),
        ("chest tightness", "reactive airway disease"),
        ("snoring and daytime fatigue", "obstructive sleep apnea"),
    ],
    "Endocrinology": [
        ("fatigue and weight gain", "hypothyroidism"),
        ("excessive thirst", "diabetes mellitus"),
        ("weight loss", "hyperthyroidism"),
        ("irregular periods", "polycystic ovary syndrome"),
        ("bone pain", "osteoporosis"),
    ],
    "Psychiatry": [
        ("feeling sad and hopeless", "major depressive disorder"),
        ("anxiety and worry", "generalized anxiety disorder"),
        ("trouble sleeping", "insomnia"),
        ("mood swings", "bipolar disorder"),
        ("panic attacks", "panic disorder"),
    ],
    "Rheumatology": [
        ("joint pain and stiffness", "rheumatoid arthritis"),
        ("muscle pain", "fibromyalgia"),
        ("swollen joints", "gout"),
        ("dry eyes and mouth", "Sjogren's syndrome"),
        ("skin tightening", "scleroderma"),
    ],
}

# Default complaints for specialties not explicitly listed
DEFAULT_COMPLAINTS = [
    ("general discomfort", "unspecified condition"),
    ("pain", "musculoskeletal pain"),
    ("fatigue", "fatigue syndrome"),
    ("malaise", "viral syndrome"),
    ("routine concern", "wellness check"),
]

VITAL_SIGNS_BY_AGE = {
    "Pediatric (0-5 years)": {
        "temp": (36.5, 37.5), "hr": (90, 140), "rr": (20, 30),
        "bp_sys": (80, 100), "bp_dia": (50, 65), "spo2": (95, 100)
    },
    "Child (6-12 years)": {
        "temp": (36.5, 37.3), "hr": (70, 110), "rr": (18, 25),
        "bp_sys": (90, 110), "bp_dia": (55, 70), "spo2": (96, 100)
    },
    "Adolescent (13-17 years)": {
        "temp": (36.3, 37.2), "hr": (60, 100), "rr": (12, 20),
        "bp_sys": (100, 120), "bp_dia": (60, 75), "spo2": (96, 100)
    },
    "Young adult (18-35 years)": {
        "temp": (36.1, 37.2), "hr": (60, 100), "rr": (12, 20),
        "bp_sys": (110, 130), "bp_dia": (65, 85), "spo2": (96, 100)
    },
    "Middle-aged adult (36-55 years)": {
        "temp": (36.1, 37.2), "hr": (60, 100), "rr": (12, 20),
        "bp_sys": (115, 140), "bp_dia": (70, 90), "spo2": (95, 99)
    },
    "Older adult (56-70 years)": {
        "temp": (36.0, 37.2), "hr": (55, 95), "rr": (12, 20),
        "bp_sys": (120, 150), "bp_dia": (70, 90), "spo2": (94, 98)
    },
    "Elderly (71+ years)": {
        "temp": (35.8, 37.0), "hr": (50, 90), "rr": (12, 22),
        "bp_sys": (125, 160), "bp_dia": (65, 90), "spo2": (92, 97)
    },
}

EXAM_FINDINGS_BY_SPECIALTY = {
    "Cardiology": [
        "Regular heart rhythm with no murmurs",
        "Mild systolic murmur grade II/VI at the apex",
        "S1 and S2 normal, no gallops",
        "Trace bilateral pedal edema",
        "JVP not elevated, no carotid bruits",
    ],
    "Pediatrics": [
        "Tympanic membranes erythematous bilaterally",
        "Pharynx mildly erythematous, no exudates",
        "Lungs clear to auscultation bilaterally",
        "Abdomen soft, non-tender, normoactive bowel sounds",
        "Skin warm and dry, no rashes",
    ],
    "Orthopedics": [
        "Tenderness to palpation over affected joint",
        "Range of motion limited by pain",
        "No gross deformity or swelling",
        "Neurovascular status intact distally",
        "Mild crepitus with movement",
    ],
    "Neurology": [
        "Cranial nerves II-XII intact",
        "Motor strength 5/5 in all extremities",
        "Sensation intact to light touch",
        "Deep tendon reflexes 2+ and symmetric",
        "Gait steady, Romberg negative",
    ],
    "Pulmonology": [
        "Breath sounds diminished at bases",
        "Scattered expiratory wheezes bilaterally",
        "No accessory muscle use",
        "Chest expansion symmetric",
        "Lungs clear after bronchodilator treatment",
    ],
}

DEFAULT_EXAM_FINDINGS = [
    "General appearance: well-developed, well-nourished, in no acute distress",
    "Vital signs within normal limits for age",
    "No acute abnormalities noted on examination",
    "Alert and oriented, cooperative with exam",
    "No lymphadenopathy appreciated",
]

TREATMENT_PLANS = {
    "Acute illness": [
        "Prescribed appropriate medication with instructions for use",
        "Advised rest and adequate hydration",
        "Return precautions discussed, follow up if symptoms worsen",
        "Over-the-counter symptom management recommended",
    ],
    "Chronic disease management": [
        "Continued current medication regimen with dosage adjustment",
        "Lifestyle modifications discussed including diet and exercise",
        "Laboratory tests ordered for monitoring",
        "Follow-up appointment scheduled in 3 months",
    ],
    "Preventive care": [
        "Immunizations updated per schedule",
        "Health maintenance counseling provided",
        "Screening tests ordered as appropriate for age",
        "Healthy lifestyle habits reinforced",
    ],
    "Follow-up visit": [
        "Progress reviewed and treatment plan adjusted accordingly",
        "Current medications continued",
        "Patient educated on disease management",
        "Next follow-up scheduled as clinically indicated",
    ],
    "Emergency presentation": [
        "Acute stabilization measures initiated",
        "Diagnostic workup completed",
        "Appropriate specialist consultation obtained",
        "Discharge with close follow-up instructions",
    ],
}


# =============================================================================
# Helper Functions
# =============================================================================

def random_name(first_names: List[str], last_names: List[str]) -> str:
    """Generate a random full name."""
    return f"{random.choice(first_names)} {random.choice(last_names)}"


def random_doctor_name() -> str:
    """Generate a random doctor name with title."""
    return f"Dr. {random_name(DOCTOR_FIRST_NAMES, DOCTOR_LAST_NAMES)}"


def random_patient_name() -> str:
    """Generate a random patient name."""
    return random_name(PATIENT_FIRST_NAMES, PATIENT_LAST_NAMES)


def random_age_from_group(age_group: str) -> int:
    """Generate a random age based on age group."""
    ranges = {
        "Pediatric (0-5 years)": (1, 5),
        "Child (6-12 years)": (6, 12),
        "Adolescent (13-17 years)": (13, 17),
        "Young adult (18-35 years)": (18, 35),
        "Middle-aged adult (36-55 years)": (36, 55),
        "Older adult (56-70 years)": (56, 70),
        "Elderly (71+ years)": (71, 90),
    }
    low, high = ranges.get(age_group, (30, 50))
    return random.randint(low, high)


def is_pediatric(age_group: str) -> bool:
    """Check if age group is pediatric (requires guardian)."""
    return age_group in ["Pediatric (0-5 years)", "Child (6-12 years)", "Adolescent (13-17 years)"]


def random_vitals(age_group: str) -> Dict[str, str]:
    """Generate random vital signs appropriate for age group."""
    ranges = VITAL_SIGNS_BY_AGE.get(age_group, VITAL_SIGNS_BY_AGE["Middle-aged adult (36-55 years)"])

    temp = round(random.uniform(*ranges["temp"]), 1)
    hr = random.randint(*ranges["hr"])
    rr = random.randint(*ranges["rr"])
    bp_sys = random.randint(*ranges["bp_sys"])
    bp_dia = random.randint(*ranges["bp_dia"])
    spo2 = random.randint(*ranges["spo2"])

    return {
        "temp": f"{temp}°C",
        "hr": f"{hr} bpm",
        "rr": f"{rr} breaths/min",
        "bp": f"{bp_sys}/{bp_dia} mmHg",
        "spo2": f"{spo2}%",
    }


def get_complaint_and_diagnosis(specialty: str) -> Tuple[str, str]:
    """Get a random chief complaint and diagnosis for specialty."""
    complaints = CHIEF_COMPLAINTS.get(specialty, DEFAULT_COMPLAINTS)
    return random.choice(complaints)


def get_exam_finding(specialty: str) -> str:
    """Get a random exam finding for specialty."""
    findings = EXAM_FINDINGS_BY_SPECIALTY.get(specialty, DEFAULT_EXAM_FINDINGS)
    return random.choice(findings)


def get_treatment_plan(condition_type: str) -> str:
    """Get a random treatment plan for condition type."""
    plans = TREATMENT_PLANS.get(condition_type, TREATMENT_PLANS["Acute illness"])
    return random.choice(plans)


# =============================================================================
# Template-Based Generation
# =============================================================================

def generate_dialogue(
    doctor: str,
    patient: str,
    guardian: Optional[str],
    age: int,
    complaint: str,
    diagnosis: str,
    exam_finding: str,
    treatment: str,
    specialty: str,
    visit_context: str,
) -> str:
    """Generate a template-based doctor-patient dialogue."""

    patient_speaker = "Patient" if not guardian else "Guardian"

    # Select dialogue template based on visit context
    if visit_context == "New patient intake":
        greeting = f"{doctor}: Good morning, I'm {doctor}. I see you're here for your first visit with us."
    elif visit_context == "Follow-up appointment":
        greeting = f"{doctor}: Hello again. How have you been since our last visit?"
    elif visit_context == "Specialist referral":
        greeting = f"{doctor}: Hello, I'm {doctor} from {specialty}. Your primary care doctor referred you to me."
    elif visit_context == "Urgent care visit":
        greeting = f"{doctor}: I understand you're here with some urgent concerns. Let's discuss what's going on."
    else:
        greeting = f"{doctor}: Hello, how are you doing today?"

    if guardian:
        intro = f"{patient_speaker}: Hi doctor, I'm here with my child {patient}, who is {age} years old."
    else:
        intro = f"Patient: Hello doctor, I'm {patient}, {age} years old."

    dialogue = f"""{greeting}
{intro}
{doctor}: What brings you in today?
{patient_speaker}: I've been experiencing {complaint} for the past few days.
{doctor}: I see. Can you tell me more about when it started and how it's been affecting you?
{patient_speaker}: It started about a week ago and has been getting progressively worse.
{doctor}: Have you tried anything for relief?
{patient_speaker}: I've tried some over-the-counter remedies but they haven't helped much.
{doctor}: Alright, let me examine you. {exam_finding}.
{doctor}: Based on my examination and your symptoms, I believe you have {diagnosis}.
{patient_speaker}: What does that mean for me?
{doctor}: {treatment}. We should see improvement within the next few weeks.
{patient_speaker}: Thank you, doctor. Is there anything else I should watch out for?
{doctor}: If your symptoms worsen or you develop new symptoms, please come back or call our office. Do you have any other questions?
{patient_speaker}: No, that covers everything. Thank you for your help.
{doctor}: You're welcome. Take care and feel better soon."""

    return dialogue


def generate_soap_note(
    patient: str,
    age: int,
    age_group: str,
    complaint: str,
    diagnosis: str,
    exam_finding: str,
    treatment: str,
    vitals: Dict[str, str],
    specialty: str,
    condition_type: str,
) -> Dict[str, str]:
    """Generate template-based SOAP note sections."""

    subjective = f"""Chief Complaint: {complaint.capitalize()}.
History of Present Illness: The patient is a {age}-year-old presenting with {complaint} for approximately one week with gradual worsening. Patient denies associated fever, chills, or significant weight changes. No prior similar episodes reported. Patient has tried over-the-counter remedies with minimal relief."""

    objective = f"""Vital Signs: Temperature {vitals['temp']}, Heart Rate {vitals['hr']}, Respiratory Rate {vitals['rr']}, Blood Pressure {vitals['bp']}, SpO2 {vitals['spo2']}.
Physical Examination: {exam_finding}. Patient appears well-developed and well-nourished, alert and oriented, in no acute distress."""

    assessment = f"""{diagnosis.capitalize()} based on clinical presentation and physical examination findings. The condition appears consistent with typical {condition_type.lower()} presentation for {specialty.lower()} complaints. No red flag symptoms identified."""

    plan = f"""{treatment}. Patient educated on condition, expected course, and warning signs requiring immediate medical attention. Follow-up as clinically indicated. Patient verbalized understanding of instructions and expressed no additional questions at this time."""

    return {"S": subjective, "O": objective, "A": assessment, "P": plan}


def generate_sample(
    specialty: str,
    condition_type: str,
    age_group: str,
    visit_context: str,
) -> Dict[str, str]:
    """Generate a single complete sample with dialogue and SOAP note."""

    doctor = random_doctor_name()
    patient = random_patient_name()
    age = random_age_from_group(age_group)
    guardian = random_patient_name() if is_pediatric(age_group) else None

    complaint, diagnosis = get_complaint_and_diagnosis(specialty)
    exam_finding = get_exam_finding(specialty)
    treatment = get_treatment_plan(condition_type)
    vitals = random_vitals(age_group)

    dialogue = generate_dialogue(
        doctor, patient, guardian, age, complaint, diagnosis,
        exam_finding, treatment, specialty, visit_context
    )

    soap = generate_soap_note(
        patient, age, age_group, complaint, diagnosis,
        exam_finding, treatment, vitals, specialty, condition_type
    )

    return {
        "DIALOGUE": dialogue,
        "S": soap["S"],
        "O": soap["O"],
        "A": soap["A"],
        "P": soap["P"],
    }


# =============================================================================
# Core Generation Function
# =============================================================================

def generate_heuristic_data(
    num_samples: int,
    session: Optional[Session] = None,
) -> DataFrame:
    """
    Generate synthetic SOAP data using heuristics and templates.

    Args:
        num_samples: Number of samples to generate
        session: Snowpark session (default: get or create)

    Returns:
        Snowpark DataFrame with DIALOGUE, S, O, A, P columns
    """
    if session is None:
        session = Session.builder.getOrCreate()

    samples = []
    for i in range(num_samples):
        # Cycle through diversity grid
        specialty = SPECIALTIES[i % len(SPECIALTIES)]
        condition_type = CONDITION_TYPES[i % len(CONDITION_TYPES)]
        age_group = AGE_GROUPS[i % len(AGE_GROUPS)]
        visit_context = VISIT_CONTEXTS[i % len(VISIT_CONTEXTS)]

        sample = generate_sample(specialty, condition_type, age_group, visit_context)
        samples.append(sample)

    return session.create_dataframe(samples)

