"""
Medical scenarios configuration for synthetic data generation.
Contains medical specialties, conditions, demographics, and scenario generators.
"""

import random
from dataclasses import dataclass
from typing import List, Dict, Optional

# Medical specialties with associated common conditions
MEDICAL_SPECIALTIES: Dict[str, List[str]] = {
    # Cardiology
    "cardiology": [
        "acute myocardial infarction", "heart failure", "atrial fibrillation",
        "hypertension", "coronary artery disease", "cardiomyopathy",
        "pericarditis", "myocarditis", "aortic stenosis", "mitral regurgitation",
        "deep vein thrombosis", "pulmonary embolism", "endocarditis",
        "arrhythmia", "angina pectoris", "cardiac tamponade"
    ],
    # Oncology
    "oncology": [
        "lung cancer", "breast cancer", "colorectal cancer", "prostate cancer",
        "lymphoma", "leukemia", "melanoma", "pancreatic cancer", "ovarian cancer",
        "thyroid cancer", "liver cancer", "kidney cancer", "bladder cancer",
        "brain tumor", "multiple myeloma", "esophageal cancer"
    ],
    # Neurology
    "neurology": [
        "stroke", "epilepsy", "multiple sclerosis", "Parkinson's disease",
        "Alzheimer's disease", "migraine", "peripheral neuropathy", "meningitis",
        "encephalitis", "brain tumor", "myasthenia gravis", "Guillain-Barré syndrome",
        "trigeminal neuralgia", "Bell's palsy", "ALS", "Huntington's disease"
    ],
    # Orthopedics
    "orthopedics": [
        "fracture", "osteoarthritis", "rheumatoid arthritis", "herniated disc",
        "rotator cuff tear", "ACL tear", "meniscus tear", "carpal tunnel syndrome",
        "osteoporosis", "scoliosis", "spinal stenosis", "tendinitis",
        "bursitis", "osteomyelitis", "bone tumor", "joint dislocation"
    ],
    # Gastroenterology
    "gastroenterology": [
        "GERD", "peptic ulcer disease", "inflammatory bowel disease",
        "Crohn's disease", "ulcerative colitis", "irritable bowel syndrome",
        "hepatitis", "cirrhosis", "pancreatitis", "cholecystitis",
        "celiac disease", "diverticulitis", "GI bleeding", "colon polyps",
        "esophageal varices", "Barrett's esophagus"
    ],
    # Pulmonology
    "pulmonology": [
        "COPD", "asthma", "pneumonia", "pulmonary fibrosis", "lung cancer",
        "pulmonary embolism", "pleural effusion", "tuberculosis", "bronchitis",
        "sleep apnea", "pulmonary hypertension", "sarcoidosis", "empyema",
        "respiratory failure", "ARDS", "bronchiectasis"
    ],
    # Nephrology
    "nephrology": [
        "chronic kidney disease", "acute kidney injury", "nephrotic syndrome",
        "glomerulonephritis", "polycystic kidney disease", "kidney stones",
        "renal artery stenosis", "diabetic nephropathy", "hypertensive nephropathy",
        "lupus nephritis", "IgA nephropathy", "end-stage renal disease",
        "electrolyte imbalance", "urinary tract infection", "pyelonephritis"
    ],
    # Endocrinology
    "endocrinology": [
        "type 1 diabetes", "type 2 diabetes", "hypothyroidism", "hyperthyroidism",
        "Cushing's syndrome", "Addison's disease", "pheochromocytoma",
        "hyperparathyroidism", "hypoparathyroidism", "pituitary adenoma",
        "acromegaly", "diabetes insipidus", "PCOS", "adrenal insufficiency",
        "thyroid nodule", "metabolic syndrome"
    ],
    # Dermatology
    "dermatology": [
        "psoriasis", "eczema", "acne", "melanoma", "basal cell carcinoma",
        "squamous cell carcinoma", "rosacea", "vitiligo", "alopecia",
        "contact dermatitis", "urticaria", "shingles", "cellulitis",
        "impetigo", "seborrheic dermatitis", "lichen planus"
    ],
    # Rheumatology
    "rheumatology": [
        "rheumatoid arthritis", "systemic lupus erythematosus", "gout",
        "ankylosing spondylitis", "psoriatic arthritis", "scleroderma",
        "Sjögren's syndrome", "vasculitis", "polymyalgia rheumatica",
        "fibromyalgia", "osteoarthritis", "reactive arthritis",
        "dermatomyositis", "polymyositis", "Behçet's disease"
    ],
    # Pediatrics
    "pediatrics": [
        "otitis media", "bronchiolitis", "croup", "kawasaki disease",
        "febrile seizure", "failure to thrive", "developmental delay",
        "congenital heart disease", "pediatric asthma", "ADHD",
        "autism spectrum disorder", "juvenile idiopathic arthritis",
        "childhood leukemia", "neonatal jaundice", "pediatric pneumonia"
    ],
    # Geriatrics
    "geriatrics": [
        "dementia", "falls and fractures", "polypharmacy", "delirium",
        "urinary incontinence", "pressure ulcers", "malnutrition",
        "sarcopenia", "frailty syndrome", "cognitive impairment",
        "osteoporosis", "chronic pain management", "end-of-life care",
        "depression in elderly", "mobility impairment"
    ],
    # Emergency medicine
    "emergency_medicine": [
        "trauma", "acute abdomen", "chest pain", "stroke", "anaphylaxis",
        "sepsis", "cardiac arrest", "respiratory distress", "seizure",
        "overdose", "burns", "fractures", "head injury", "GI bleeding",
        "diabetic emergency", "acute psychosis"
    ],
    # Infectious disease
    "infectious_disease": [
        "HIV/AIDS", "tuberculosis", "sepsis", "endocarditis", "osteomyelitis",
        "meningitis", "pneumonia", "urinary tract infection", "cellulitis",
        "hepatitis B", "hepatitis C", "Lyme disease", "COVID-19",
        "influenza", "malaria", "dengue fever"
    ],
    # Hematology
    "hematology": [
        "anemia", "leukemia", "lymphoma", "multiple myeloma", "hemophilia",
        "thrombocytopenia", "sickle cell disease", "thalassemia",
        "polycythemia vera", "myelodysplastic syndrome", "DVT",
        "DIC", "ITP", "TTP", "aplastic anemia"
    ],
    # Psychiatry
    "psychiatry": [
        "major depressive disorder", "bipolar disorder", "schizophrenia",
        "anxiety disorder", "PTSD", "OCD", "panic disorder", "eating disorder",
        "substance use disorder", "personality disorder", "ADHD",
        "insomnia", "somatoform disorder", "adjustment disorder"
    ],
    # Ophthalmology
    "ophthalmology": [
        "glaucoma", "cataracts", "macular degeneration", "diabetic retinopathy",
        "retinal detachment", "uveitis", "conjunctivitis", "keratitis",
        "optic neuritis", "strabismus", "amblyopia", "corneal ulcer",
        "dry eye syndrome", "blepharitis", "pterygium"
    ],
    # ENT (Otolaryngology)
    "otolaryngology": [
        "chronic sinusitis", "tonsillitis", "hearing loss", "vertigo",
        "Meniere's disease", "laryngitis", "sleep apnea", "head and neck cancer",
        "thyroid nodule", "nasal polyps", "epistaxis", "otitis media",
        "cholesteatoma", "vocal cord paralysis", "salivary gland tumor"
    ],
    # Urology
    "urology": [
        "benign prostatic hyperplasia", "prostate cancer", "kidney stones",
        "urinary tract infection", "bladder cancer", "erectile dysfunction",
        "urinary incontinence", "testicular cancer", "hydronephrosis",
        "pyelonephritis", "renal cell carcinoma", "interstitial cystitis",
        "urethral stricture", "varicocele", "epididymitis"
    ],
    # Obstetrics and Gynecology
    "obstetrics_gynecology": [
        "preeclampsia", "gestational diabetes", "ectopic pregnancy",
        "placenta previa", "preterm labor", "endometriosis", "ovarian cyst",
        "uterine fibroids", "cervical cancer", "ovarian cancer",
        "PCOS", "menopause", "pelvic inflammatory disease", "miscarriage",
        "postpartum hemorrhage"
    ],
    # Allergy and Immunology
    "allergy_immunology": [
        "allergic rhinitis", "food allergy", "drug allergy", "anaphylaxis",
        "asthma", "urticaria", "angioedema", "primary immunodeficiency",
        "autoimmune disease", "contact dermatitis", "atopic dermatitis",
        "eosinophilic esophagitis", "mastocytosis", "CVID"
    ],
    # Pain Management
    "pain_management": [
        "chronic back pain", "neuropathic pain", "fibromyalgia",
        "complex regional pain syndrome", "cancer pain", "post-surgical pain",
        "headache disorders", "myofascial pain", "arthritis pain",
        "phantom limb pain", "spinal cord injury pain", "visceral pain"
    ],
    # Vascular Surgery
    "vascular_surgery": [
        "aortic aneurysm", "peripheral artery disease", "carotid stenosis",
        "deep vein thrombosis", "varicose veins", "arteriovenous malformation",
        "critical limb ischemia", "mesenteric ischemia", "renal artery stenosis",
        "thoracic outlet syndrome", "lymphedema"
    ],
    # Plastic Surgery
    "plastic_surgery": [
        "burn reconstruction", "breast reconstruction", "cleft lip and palate",
        "skin cancer reconstruction", "hand surgery", "facial trauma",
        "pressure ulcer reconstruction", "wound healing complications",
        "nerve repair", "microsurgery"
    ],
    # Sports Medicine
    "sports_medicine": [
        "ACL injury", "meniscus tear", "rotator cuff injury", "tennis elbow",
        "Achilles tendinopathy", "stress fracture", "concussion",
        "muscle strain", "ligament sprain", "overuse injury",
        "runner's knee", "shin splints", "plantar fasciitis"
    ],
    # Palliative Care
    "palliative_care": [
        "terminal cancer", "end-stage heart failure", "advanced COPD",
        "end-stage renal disease", "ALS", "advanced dementia",
        "metastatic disease", "pain management at end of life",
        "symptom management", "goals of care discussion"
    ],
    # Transplant Medicine
    "transplant_medicine": [
        "kidney transplant evaluation", "liver transplant", "heart transplant",
        "lung transplant", "bone marrow transplant", "organ rejection",
        "post-transplant complications", "immunosuppression management",
        "graft versus host disease"
    ],
    # Critical Care
    "critical_care": [
        "septic shock", "ARDS", "multi-organ failure", "respiratory failure",
        "cardiogenic shock", "traumatic brain injury", "status epilepticus",
        "massive transfusion", "DIC", "acute liver failure",
        "ventilator management", "ECMO"
    ],
    # Genetics
    "genetics": [
        "hereditary cancer syndrome", "cystic fibrosis", "sickle cell disease",
        "Huntington's disease", "Marfan syndrome", "Down syndrome",
        "fragile X syndrome", "hemophilia", "muscular dystrophy",
        "familial hypercholesterolemia", "BRCA mutation"
    ],
    # Physical Medicine and Rehabilitation
    "rehabilitation": [
        "stroke rehabilitation", "spinal cord injury", "traumatic brain injury",
        "amputation rehabilitation", "cardiac rehabilitation",
        "pulmonary rehabilitation", "post-surgical rehabilitation",
        "chronic pain rehabilitation", "neurological rehabilitation"
    ]
}

# Patient demographics
GENDERS = ["male", "female"]
AGE_RANGES = [
    (0, 2, "infant"),
    (2, 12, "child"),
    (12, 18, "adolescent"),
    (18, 40, "young adult"),
    (40, 65, "middle-aged adult"),
    (65, 100, "elderly")
]

ETHNICITIES = [
    "Caucasian", "African American", "Hispanic", "Asian",
    "South Asian", "Middle Eastern", "Native American",
    "Pacific Islander", "Mixed ethnicity"
]

OCCUPATIONS = [
    "office worker", "manual laborer", "healthcare worker", "teacher",
    "engineer", "retired", "student", "homemaker", "farmer",
    "factory worker", "construction worker", "truck driver",
    "professional athlete", "military personnel", "unemployed"
]

# Medical history elements
COMORBIDITIES = [
    "hypertension", "diabetes mellitus type 2", "hyperlipidemia",
    "obesity", "coronary artery disease", "COPD", "asthma",
    "chronic kidney disease", "hypothyroidism", "depression",
    "anxiety", "GERD", "osteoarthritis", "atrial fibrillation",
    "heart failure", "stroke history", "cancer history"
]

SOCIAL_HISTORY_ELEMENTS = {
    "smoking": ["never smoker", "former smoker (quit 5 years ago)", 
                "former smoker (quit 10 years ago)", "current smoker (10 pack-years)",
                "current smoker (20 pack-years)", "current smoker (30 pack-years)"],
    "alcohol": ["non-drinker", "social drinker (1-2 drinks/week)",
                "moderate drinker (1 drink/day)", "heavy drinker",
                "former heavy drinker, now abstinent"],
    "drugs": ["no illicit drug use", "history of marijuana use",
              "history of cocaine use", "history of opioid use",
              "current IV drug user"]
}

FAMILY_HISTORY = [
    "father with heart disease", "mother with breast cancer",
    "sibling with diabetes", "parent with stroke",
    "family history of colon cancer", "no significant family history",
    "mother with thyroid disease", "father with prostate cancer",
    "family history of autoimmune disease", "family history of mental illness"
]

# Complexity levels for dialogue generation
COMPLEXITY_LEVELS = ["straightforward", "moderate", "complex"]


@dataclass
class PatientDemographics:
    """Patient demographic information."""
    age: int
    age_category: str
    gender: str
    ethnicity: str
    occupation: str


@dataclass
class MedicalHistory:
    """Patient medical history."""
    comorbidities: List[str]
    smoking_status: str
    alcohol_use: str
    drug_use: str
    family_history: List[str]
    surgical_history: Optional[str]
    medications: Optional[str]


@dataclass
class MedicalScenario:
    """Complete medical scenario for dialogue generation."""
    specialty: str
    condition: str
    demographics: PatientDemographics
    medical_history: MedicalHistory
    complexity: str
    presentation_type: str  # new diagnosis, follow-up, emergency, routine check


def generate_patient_demographics(
    specialty: str = None,
    condition: str = None
) -> PatientDemographics:
    """Generate random patient demographics appropriate for the condition."""
    
    # Adjust age distribution based on specialty
    if specialty == "pediatrics":
        age = random.randint(0, 17)
    elif specialty == "geriatrics":
        age = random.randint(65, 95)
    elif specialty == "obstetrics_gynecology":
        age = random.randint(18, 50)
    else:
        # General distribution weighted toward adults
        age_weights = [5, 10, 10, 25, 30, 20]  # infant, child, adolescent, young, middle, elderly
        age_range_idx = random.choices(range(len(AGE_RANGES)), weights=age_weights)[0]
        age_min, age_max, _ = AGE_RANGES[age_range_idx]
        age = random.randint(age_min, age_max - 1)
    
    # Determine age category
    age_category = "adult"
    for min_age, max_age, category in AGE_RANGES:
        if min_age <= age < max_age:
            age_category = category
            break
    
    # Adjust gender based on specialty/condition
    if specialty == "obstetrics_gynecology":
        gender = "female"
    elif condition and any(c in condition.lower() for c in ["prostate", "testicular"]):
        gender = "male"
    elif condition and any(c in condition.lower() for c in ["ovarian", "cervical", "breast"]):
        gender = "female"
    else:
        gender = random.choice(GENDERS)
    
    return PatientDemographics(
        age=age,
        age_category=age_category,
        gender=gender,
        ethnicity=random.choice(ETHNICITIES),
        occupation=random.choice(OCCUPATIONS) if age >= 18 else "student" if age >= 5 else "N/A"
    )


def generate_medical_history(
    demographics: PatientDemographics,
    specialty: str = None
) -> MedicalHistory:
    """Generate random medical history appropriate for patient demographics."""
    
    # Number of comorbidities increases with age
    if demographics.age < 18:
        num_comorbidities = random.choices([0, 1], weights=[80, 20])[0]
    elif demographics.age < 40:
        num_comorbidities = random.choices([0, 1, 2], weights=[60, 30, 10])[0]
    elif demographics.age < 65:
        num_comorbidities = random.choices([0, 1, 2, 3], weights=[20, 35, 30, 15])[0]
    else:
        num_comorbidities = random.choices([1, 2, 3, 4, 5], weights=[15, 25, 30, 20, 10])[0]
    
    comorbidities = random.sample(COMORBIDITIES, min(num_comorbidities, len(COMORBIDITIES)))
    
    # Social history - adjust based on age
    if demographics.age < 18:
        smoking_status = "never smoker"
        alcohol_use = "non-drinker"
        drug_use = "no illicit drug use"
    else:
        smoking_status = random.choice(SOCIAL_HISTORY_ELEMENTS["smoking"])
        alcohol_use = random.choice(SOCIAL_HISTORY_ELEMENTS["alcohol"])
        drug_use = random.choices(
            SOCIAL_HISTORY_ELEMENTS["drugs"],
            weights=[70, 15, 5, 5, 5]
        )[0]
    
    # Family history
    num_family_history = random.choices([0, 1, 2], weights=[30, 50, 20])[0]
    family_history = random.sample(FAMILY_HISTORY, num_family_history)
    
    # Surgical history
    surgical_options = [
        None, "appendectomy", "cholecystectomy", "cesarean section",
        "knee replacement", "hip replacement", "hernia repair",
        "tonsillectomy", "cardiac catheterization", "spinal surgery"
    ]
    surgical_history = random.choices(
        surgical_options,
        weights=[50] + [50 // (len(surgical_options) - 1)] * (len(surgical_options) - 1)
    )[0]
    
    return MedicalHistory(
        comorbidities=comorbidities,
        smoking_status=smoking_status,
        alcohol_use=alcohol_use,
        drug_use=drug_use,
        family_history=family_history,
        surgical_history=surgical_history,
        medications=None  # Will be generated based on comorbidities if needed
    )


def generate_scenario(scenario_id: int = None) -> MedicalScenario:
    """Generate a complete medical scenario for dialogue generation."""
    
    # Select specialty and condition
    specialty = random.choice(list(MEDICAL_SPECIALTIES.keys()))
    condition = random.choice(MEDICAL_SPECIALTIES[specialty])
    
    # Generate demographics and history
    demographics = generate_patient_demographics(specialty, condition)
    medical_history = generate_medical_history(demographics, specialty)
    
    # Complexity and presentation type
    complexity = random.choices(
        COMPLEXITY_LEVELS,
        weights=[40, 40, 20]
    )[0]
    
    presentation_types = ["new diagnosis", "follow-up visit", "emergency presentation", "routine check"]
    presentation_weights = [35, 30, 20, 15]
    presentation_type = random.choices(presentation_types, weights=presentation_weights)[0]
    
    return MedicalScenario(
        specialty=specialty,
        condition=condition,
        demographics=demographics,
        medical_history=medical_history,
        complexity=complexity,
        presentation_type=presentation_type
    )


def scenario_to_prompt(scenario: MedicalScenario) -> str:
    """Convert a medical scenario to a prompt for dialogue generation."""
    
    # Format demographics
    demographics_str = (
        f"- Age: {scenario.demographics.age} years old ({scenario.demographics.age_category})\n"
        f"- Gender: {scenario.demographics.gender}\n"
        f"- Ethnicity: {scenario.demographics.ethnicity}\n"
        f"- Occupation: {scenario.demographics.occupation}"
    )
    
    # Format medical history
    comorbidities_str = (
        ", ".join(scenario.medical_history.comorbidities) 
        if scenario.medical_history.comorbidities 
        else "None"
    )
    family_history_str = (
        ", ".join(scenario.medical_history.family_history)
        if scenario.medical_history.family_history
        else "No significant family history"
    )
    surgical_str = scenario.medical_history.surgical_history or "None"
    
    history_str = (
        f"- Comorbidities: {comorbidities_str}\n"
        f"- Smoking: {scenario.medical_history.smoking_status}\n"
        f"- Alcohol: {scenario.medical_history.alcohol_use}\n"
        f"- Drug use: {scenario.medical_history.drug_use}\n"
        f"- Family history: {family_history_str}\n"
        f"- Surgical history: {surgical_str}"
    )
    
    prompt = f"""Generate a realistic, detailed doctor-patient dialogue for the following medical scenario.

MEDICAL SCENARIO:
- Specialty: {scenario.specialty.replace('_', ' ').title()}
- Primary condition: {scenario.condition}
- Visit type: {scenario.presentation_type}
- Case complexity: {scenario.complexity}

PATIENT DEMOGRAPHICS:
{demographics_str}

RELEVANT MEDICAL HISTORY:
{history_str}

REQUIREMENTS:
1. Create a natural, flowing conversation between doctor and patient
2. Include appropriate medical history taking, physical examination discussion, and test results if relevant
3. The dialogue should be 2000-3500 characters long
4. Include realistic vital signs, lab values, imaging results as appropriate for the condition
5. The doctor should explain findings and discuss treatment options
6. The patient should ask relevant questions and express concerns naturally
7. Use realistic medical terminology but ensure patient responses are in layman's terms
8. Format: Each speaker's turn should start with "Doctor: " or "Patient: " on a new line
9. Do not include any text before or after the dialogue itself

Generate the dialogue now:"""
    
    return prompt


def generate_scenarios_batch(
    num_scenarios: int,
    start_id: int = 0
) -> List[MedicalScenario]:
    """Generate a batch of diverse medical scenarios."""
    
    scenarios = []
    
    # Ensure diversity by tracking used specialties and conditions
    specialty_counts = {s: 0 for s in MEDICAL_SPECIALTIES.keys()}
    max_per_specialty = (num_scenarios // len(MEDICAL_SPECIALTIES)) + 2
    
    for i in range(num_scenarios):
        # Try to balance specialties
        available_specialties = [
            s for s, count in specialty_counts.items() 
            if count < max_per_specialty
        ]
        if not available_specialties:
            available_specialties = list(MEDICAL_SPECIALTIES.keys())
        
        specialty = random.choice(available_specialties)
        condition = random.choice(MEDICAL_SPECIALTIES[specialty])
        
        demographics = generate_patient_demographics(specialty, condition)
        medical_history = generate_medical_history(demographics, specialty)
        
        complexity = random.choices(COMPLEXITY_LEVELS, weights=[40, 40, 20])[0]
        presentation_types = ["new diagnosis", "follow-up visit", "emergency presentation", "routine check"]
        presentation_type = random.choices(presentation_types, weights=[35, 30, 20, 15])[0]
        
        scenario = MedicalScenario(
            specialty=specialty,
            condition=condition,
            demographics=demographics,
            medical_history=medical_history,
            complexity=complexity,
            presentation_type=presentation_type
        )
        
        scenarios.append(scenario)
        specialty_counts[specialty] += 1
    
    return scenarios


if __name__ == "__main__":
    # Test scenario generation
    print("Testing medical scenario generation...")
    print(f"Total specialties: {len(MEDICAL_SPECIALTIES)}")
    print(f"Total conditions: {sum(len(c) for c in MEDICAL_SPECIALTIES.values())}")
    
    # Generate a sample scenario
    scenario = generate_scenario()
    print(f"\nSample scenario:")
    print(f"  Specialty: {scenario.specialty}")
    print(f"  Condition: {scenario.condition}")
    print(f"  Patient: {scenario.demographics.age}yo {scenario.demographics.gender}")
    print(f"  Complexity: {scenario.complexity}")
    print(f"  Presentation: {scenario.presentation_type}")
    
    # Generate prompt
    prompt = scenario_to_prompt(scenario)
    print(f"\nGenerated prompt length: {len(prompt)} characters")
    print("\n--- Prompt Preview ---")
    print(prompt[:500] + "...")
