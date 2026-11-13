"""Prompt library loader for MWAA converter."""
import yaml
from pathlib import Path
from typing import Dict


def load_prompt(prompt_name: str) -> str:
    """
    Load a prompt template from prompts.yaml.
    
    Args:
        prompt_name: Name of the prompt template
        
    Returns:
        Prompt template string
    """
    prompts_file = Path(__file__).parent / "prompts.yaml"
    
    with open(prompts_file, 'r', encoding='utf-8') as f:
        prompts = yaml.safe_load(f)
    
    if prompt_name not in prompts:
        raise ValueError(f"Prompt '{prompt_name}' not found in library")
    
    return prompts[prompt_name]['prompt']


def get_prompt_description(prompt_name: str) -> str:
    """
    Get description of a prompt.
    
    Args:
        prompt_name: Name of the prompt template
        
    Returns:
        Prompt description
    """
    prompts_file = Path(__file__).parent / "prompts.yaml"
    
    with open(prompts_file, 'r', encoding='utf-8') as f:
        prompts = yaml.safe_load(f)
    
    if prompt_name not in prompts:
        raise ValueError(f"Prompt '{prompt_name}' not found in library")
    
    return prompts[prompt_name].get('description', '')


def list_prompts() -> Dict[str, str]:
    """
    List all available prompts with descriptions.
    
    Returns:
        Dictionary of prompt names and descriptions
    """
    prompts_file = Path(__file__).parent / "prompts.yaml"
    
    with open(prompts_file, 'r', encoding='utf-8') as f:
        prompts = yaml.safe_load(f)
    
    return {
        name: data.get('description', '')
        for name, data in prompts.items()
    }
