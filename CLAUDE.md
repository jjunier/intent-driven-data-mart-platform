# Project instructions

## Project goal
This project is an intent-driven data mart automation platform.
It analyzes source data in a data warehouse and proposes or generates suitable data marts.

## Working rules
- Always explain the plan before making major code changes.
- Prefer small, reviewable changes.
- Do not invent commands or architecture details not present in the repo.
- When uncertain, inspect files first and report findings clearly.

## Coding preferences
- Keep functions small and readable.
- Write docstrings for important modules.
- Prefer explicit naming over short abbreviations.

## Development workflow
- First understand the repository structure.
- Then propose a plan.
- Then implement.
- Then suggest validation or tests.

## Git workflow
- Always branch from `develop`, never from `main`.
- Branch naming: `feature/<description>` or `refactor/<description>`.
- Merge order is strictly enforced: `feature → develop → main`.
- Direct `feature → main` PR is prohibited. If attempted, stop and correct the base branch.
- After merging `feature → develop`, create a separate `develop → main` PR.
- All commit messages and PR titles must be written in English.
- Do not commit unrelated changes. Keep each commit scoped to the stated task.

## PR rules
- Every PR body must contain all four of the following sections, in order:
  1. **Background** — why this change is needed
  2. **What Changed** — what was added, modified, or removed
  3. **Test Results** — pass/fail counts, regression status
  4. **Risks / Follow-ups** — known limitations, deferred items, edge cases not covered
- Omitting any section is not allowed, even if the content is brief.

## Stage reporting format
After completing each implementation stage, report in this exact format:
1. Stage name
2. Created branch
3. Modified files
4. Test results
5. Commit hash
6. PR title
7. PR result
8. Deferred items for next stage

## Guardrails
- Work only within the explicitly agreed scope. Do not add features, refactors, or improvements beyond what was requested.
- Any out-of-scope item must be listed under "Deferred items" in the report, not silently implemented.
- If a PR creation or merge fails, report: the exact command run, the error message, and the manual command the user can run to complete it.
- Do not use `reset --hard` or `push --force` on shared branches (`main`, `develop`) without explicit user instruction.
- Do not skip or bypass test execution before committing. All tests must pass before a commit is made.
