"""
Data synthesis module for generating medical training data.

This module provides tools to generate synthetic doctor-patient dialogues
with SOAP notes using Claude Sonnet 4.5 via Snowflake Cortex.
"""

from .medical_scenarios import (
    generate_scenario,
    generate_scenarios_batch,
    scenario_to_prompt,
    MedicalScenario,
    PatientDemographics,
    MedicalHistory,
    MEDICAL_SPECIALTIES,
)

__all__ = [
    "generate_scenario",
    "generate_scenarios_batch", 
    "scenario_to_prompt",
    "MedicalScenario",
    "PatientDemographics",
    "MedicalHistory",
    "MEDICAL_SPECIALTIES",
]
