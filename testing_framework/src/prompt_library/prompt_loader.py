"""
Prompt loader for the Hive vs Spark SQL Test Framework.
Independent copy — no imports from the converter.
"""
import yaml
from pathlib import Path


class PromptLoader:
    """Load and format prompts from prompts.yaml"""

    def __init__(self, prompts_file: str = None):
        if prompts_file is None:
            prompts_file = Path(__file__).parent / 'prompts.yaml'
        self.prompts_file = prompts_file
        self._prompts = None

    def _load_prompts(self):
        if self._prompts is None:
            with open(self.prompts_file, 'r') as f:
                self._prompts = yaml.safe_load(f)
        return self._prompts

    def get_prompt(self, prompt_name: str, **kwargs) -> str:
        prompts = self._load_prompts()
        if prompt_name not in prompts:
            raise ValueError(f"Prompt '{prompt_name}' not found in prompts.yaml")
        template = prompts[prompt_name]['template']
        if kwargs:
            return template.format(**kwargs)
        return template


def load_prompt(prompt_name, **kwargs):
    """Backward-compatible function interface."""
    loader = PromptLoader()
    return loader.get_prompt(prompt_name, **kwargs)
