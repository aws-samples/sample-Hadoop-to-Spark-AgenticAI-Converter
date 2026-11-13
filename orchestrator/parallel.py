"""Parallel task scheduler for the Orchestrator Agent.

Supports:
  - Parallel file processing (multiple HQL files at once)
  - Parallel chunk processing (multiple chunks of one file)
  - Parallel independent stages (accuracy + mermaid while validating)
  - Respects dependency ordering within each file's pipeline
"""
import asyncio
import logging
from typing import List

from orchestrator.models import AgentRequest, AgentResponse, TaskStatus

logger = logging.getLogger(__name__)

# Default concurrency limits
DEFAULT_MAX_PARALLEL_FILES = 3
DEFAULT_MAX_PARALLEL_CHUNKS = 2


async def run_parallel(
    dispatch_fn,
    requests: List[AgentRequest],
    max_concurrent: int = DEFAULT_MAX_PARALLEL_FILES,
) -> List[AgentResponse]:
    """Run multiple independent AgentRequests in parallel with concurrency limit.

    Args:
        dispatch_fn: async callable(AgentRequest) -> AgentResponse
        requests: List of independent requests (no inter-dependencies)
        max_concurrent: Max simultaneous tasks

    Returns:
        List of AgentResponses in completion order
    """
    semaphore = asyncio.Semaphore(max_concurrent)
    results: List[AgentResponse] = []

    async def _limited(req: AgentRequest) -> AgentResponse:
        async with semaphore:
            return await dispatch_fn(req)

    tasks = [asyncio.create_task(_limited(r)) for r in requests]
    for coro in asyncio.as_completed(tasks):
        resp = await coro
        results.append(resp)
        status = "✅" if resp.status == TaskStatus.SUCCESS else "❌"
        logger.info("%s [parallel] %s", status, resp.task_id)

    return results


async def run_pipeline_per_file(
    dispatch_fn,
    file_pipelines: List[List[AgentRequest]],
    max_parallel_files: int = DEFAULT_MAX_PARALLEL_FILES,
) -> List[AgentResponse]:
    """Run multiple file pipelines in parallel. Within each pipeline, tasks run sequentially.

    Pattern: multiple files processed in parallel, within each file steps are sequential
    (analyze → convert → validate → execute).

    Args:
        dispatch_fn: async callable(AgentRequest) -> AgentResponse
        file_pipelines: List of pipelines, each pipeline is a list of sequential AgentRequests
        max_parallel_files: Max files processed simultaneously

    Returns:
        All AgentResponses from all pipelines
    """
    semaphore = asyncio.Semaphore(max_parallel_files)
    all_responses: List[AgentResponse] = []

    async def _run_single_pipeline(pipeline: List[AgentRequest]) -> List[AgentResponse]:
        async with semaphore:
            responses = []
            for req in pipeline:
                resp = await dispatch_fn(req)
                responses.append(resp)
                # Stop pipeline if a task fails (deps won't be met)
                if resp.status == TaskStatus.FAILED:
                    logger.warning("Pipeline stopped at %s due to failure", req.task_id)
                    break
            return responses

    tasks = [asyncio.create_task(_run_single_pipeline(p)) for p in file_pipelines]
    for coro in asyncio.as_completed(tasks):
        responses = await coro
        all_responses.extend(responses)

    return all_responses
