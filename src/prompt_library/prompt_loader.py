"""
Simple prompt loader utility for HiveQL to Spark SQL converter
"""
import yaml
import os
from pathlib import Path


class PromptLoader:
    """Load and format prompts from prompts.yaml"""
    
    def __init__(self, prompts_file: str = None):
        """
        Initialize prompt loader
        
        Args:
            prompts_file: Path to prompts.yaml (defaults to same directory)
        """
        if prompts_file is None:
            current_dir = Path(__file__).parent
            prompts_file = current_dir / 'prompts.yaml'
        
        self.prompts_file = prompts_file
        self._prompts = None
    
    def _load_prompts(self):
        """Load prompts from YAML file (cached)"""
        if self._prompts is None:
            with open(self.prompts_file, 'r') as f:
                self._prompts = yaml.safe_load(f)
        return self._prompts
    
    def get_prompt(self, prompt_name: str, **kwargs) -> str:
        """
        Get a formatted prompt by name
        
        Args:
            prompt_name: Name of the prompt to load
            **kwargs: Variables to format into the prompt template
            
        Returns:
            str: Formatted prompt string
        """
        prompts = self._load_prompts()
        
        if prompt_name not in prompts:
            raise ValueError(f"Prompt '{prompt_name}' not found in prompts.yaml")
        
        template = prompts[prompt_name]['template']
        
        # Format the template with provided arguments
        if kwargs:
            return template.format(**kwargs)
        else:
            return template


# Backward compatibility function
def load_prompt(prompt_name, **kwargs):
    """
    Load a prompt from the prompts.yaml file and format it with provided arguments
    
    Args:
        prompt_name: Name of the prompt to load
        **kwargs: Variables to format into the prompt template
        
    Returns:
        str: Formatted prompt string
    """
    loader = PromptLoader()
    return loader.get_prompt(prompt_name, **kwargs)
