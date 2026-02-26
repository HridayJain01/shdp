"""Thin façade: delegates plan generation to ai/reasoning.py."""
from app.modules.ai.reasoning import generate_healing_plan

__all__ = ["generate_healing_plan"]
