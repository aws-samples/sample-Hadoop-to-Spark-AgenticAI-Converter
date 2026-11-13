"""Single, reviewed entry point for launching the LLM CLIs (kiro-cli / claude).

Every component that hands a prompt to an LLM CLI goes through ``run_llm_cli``
so the subprocess call is defined — and security-reviewed — in exactly one
place.

Why this call is safe (justification for the subprocess audit findings):

* No shell. The command is always an argv list and ``shell=False``, so shell
  metacharacters in a prompt are never interpreted.
* Fixed executables. Only the CLIs in ``ALLOWED_EXECUTABLES`` can be launched;
  the executable is never taken from user input.
* No option injection. The prompt is passed as one argv element. kiro-cli does
  not honour the ``--`` end-of-options separator, so instead ``_guard_prompt``
  guarantees the prompt can never start with ``-`` and be parsed as a flag.
* Fixed flags. All other arguments are constants chosen by this module.

Residual risk (documented in SECURITY.md): prompts embed the content of input
files and the CLIs run with tool auto-approval, so the input files must be
trusted. The prompt content itself is not sanitised here.

Only the standard library is used so this module can be imported from the
orchestrator, the MCP servers and the stand-alone scripts alike.
"""
import subprocess
from pathlib import Path
from typing import List, Optional, Union

KIRO_CLI = "kiro-cli"
CLAUDE_CLI = "claude"
ALLOWED_EXECUTABLES = frozenset({KIRO_CLI, CLAUDE_CLI})

# Prefix used when a prompt would otherwise start with "-" (see _guard_prompt).
_SAFE_PROMPT_PREFIX = "Task:\n"


def _guard_prompt(prompt: str) -> str:
    """Return a prompt that cannot be mistaken for a command-line option."""
    if not isinstance(prompt, str):
        raise TypeError("prompt must be a str, got %s" % type(prompt).__name__)
    if prompt.lstrip().startswith("-"):
        return _SAFE_PROMPT_PREFIX + prompt
    return prompt


def kiro_chat_argv(prompt: str) -> List[str]:
    """argv for a non-interactive kiro-cli session with tool auto-approval."""
    return [KIRO_CLI, "chat", "--no-interactive", "--trust-all-tools", _guard_prompt(prompt)]


def claude_print_argv(
    prompt: str,
    permission_mode: str = "bypassPermissions",
    model: Optional[str] = None,
) -> List[str]:
    """argv for a non-interactive Claude Code session."""
    argv = [CLAUDE_CLI, "--print", "--permission-mode", permission_mode]
    if model:
        argv.extend(["--model", model])
    argv.extend(["-p", _guard_prompt(prompt)])
    return argv


def run_llm_cli(
    argv: List[str],
    timeout: int,
    cwd: Union[str, Path, None] = None,
) -> subprocess.CompletedProcess:
    """Run an LLM CLI command built by ``kiro_chat_argv`` / ``claude_print_argv``.

    Raises:
        ValueError: if argv is not a list of strings or targets a non-allowed executable.
        subprocess.TimeoutExpired / OSError: propagated unchanged for callers to handle.
    """
    if not isinstance(argv, list) or not argv or not all(isinstance(a, str) for a in argv):
        raise ValueError("argv must be a non-empty list of str")
    if argv[0] not in ALLOWED_EXECUTABLES:
        raise ValueError("executable %r is not an allowed LLM CLI" % argv[0])

    # Reviewed: argv list, shell=False, allow-listed executable, constant flags,
    # option-injection guard on the prompt (see module docstring).
    return subprocess.run(  # nosec B603  # nosemgrep: python.lang.security.audit.dangerous-subprocess-use-audit.dangerous-subprocess-use-audit
        argv,
        shell=False,
        # No stdin: the prompt is in argv. Without this the CLI inherits the
        # caller's terminal; the claude CLI then reads it, which blocks (or, for a
        # background run, stops the process with SIGTTIN) instead of working.
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=timeout,
        cwd=str(cwd) if cwd is not None else None,
        check=False,
    )
