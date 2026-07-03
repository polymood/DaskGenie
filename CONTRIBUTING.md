# Contributing to DaskGenie

Thank you for your interest in contributing to DaskGenie, a memory profiler and
live dashboard for Dask. This document describes how we work together on this
project: how to set up your environment, how to write and commit changes, how
branches are managed, and how releases are tagged. Please read it in full before
opening your first pull request.

The goal of these guidelines is to keep the codebase consistent, reviewable, and
reliable. Following them helps maintainers review your work quickly and helps
everyone trust that the `main` branch is always in a releasable state.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Environment](#development-environment)
- [Code Quality and Tooling](#code-quality-and-tooling)
- [Branching Model](#branching-model)
- [Commit Messages](#commit-messages)
- [Pull Requests](#pull-requests)
- [Testing](#testing)
- [Versioning and Releases](#versioning-and-releases)
- [Reporting Issues](#reporting-issues)

## Code of Conduct

We expect all contributors to be respectful and constructive in every
interaction. Disagreement about technical decisions is welcome and healthy, but
personal attacks, harassment, and dismissive behavior are not acceptable.
Maintainers reserve the right to remove comments, commits, or contributors that
violate this principle.

## Getting Started

1. Fork the repository on your account, or, if you have write access, create a
   branch directly in the main repository.
2. Clone your fork or the repository to your local machine.
3. Set up the development environment as described below.
4. Create a branch for your work following the branching model.
5. Make your changes, ensuring all code quality checks pass.
6. Open a pull request against the `develop` branch.

## Development Environment

DaskGenie has two parts: a Python package (`src/daskgenie`) and a Next.js
dashboard (`web/`). The Python side targets Python 3.11 or newer and uses
[uv](https://docs.astral.sh/uv/) for dependency management and virtual
environments. The dashboard uses Node.js 20+.

### Prerequisites

- Python 3.11 or newer.
- `uv` installed on your system.
- Node.js 20 or newer (only for dashboard work).
- Git, and Docker if you want to run the full stack locally.

### Setting up (Python)

Install the project together with its development and optional dependencies:

```bash
uv sync --group dev --extra collector --extra deep --extra examples
```

Prefix commands with `uv run`, for example:

```bash
uv run python -c "import daskgenie; print(daskgenie.__version__)"
```

Install the pre-commit hooks so that quality checks run automatically before
every commit:

```bash
uv run pre-commit install
```

From this point on, the configured hooks run each time you create a commit. You
can also run them against the whole codebase at any time:

```bash
uv run pre-commit run --all-files
```

### Setting up (dashboard)

```bash
cd web && npm install
npm run dev        # http://localhost:3000, proxies /api to the collector
```

Run the collector separately (`uv run python -m daskgenie.collector --port 8765`),
or bring up the whole stack with `docker compose up -d --build`.

## Code Quality and Tooling

We hold the codebase to a consistent standard and enforce it automatically. A
pull request will not be merged unless all of the following checks pass.

### Formatting and Linting

We use [Ruff](https://docs.astral.sh/ruff/) as both formatter and linter. The
configuration lives in `pyproject.toml`.

```bash
uv run ruff format .          # format in place
uv run ruff check --fix .     # lint and apply safe fixes
```

For the dashboard, `npm run build` must succeed (it type-checks and lints via
Next.js/ESLint).

### Static Type Checking

All Python code must be fully type annotated. We use mypy in strict mode.

```bash
uv run mypy src/
```

Please do not silence type errors with `# type: ignore` unless it is genuinely
unavoidable. When you do, add a specific error code and a short comment
explaining why.

### Typing Conventions

- Prefer precise types over `Any`; reach for `Any` only when there is no
  reasonable alternative, and document the reason.
- Use the modern built-in generic syntax (`list[str]`, `dict[str, int]`).
- Use `from __future__ import annotations` where it keeps annotations readable.

### Running All Checks Locally

```bash
uv run pre-commit run --all-files
uv run mypy src/
uv run pytest -m "not integration"
```

## Branching Model

This project uses a two-trunk model with long-lived `main` and `develop`
branches.

- `main` always reflects the latest released version. Every commit on `main`
  corresponds to a tagged release. Nothing is committed to `main` directly except
  release merges.
- `develop` is the integration branch where completed work accumulates between
  releases. It must always remain in a working, testable state.

All day-to-day work happens on short-lived branches created from `develop` and
merged back into `develop` through pull requests. A release is performed by
merging `develop` into `main` and tagging that commit.

### Branch Naming

Use short, descriptive branch names that begin with a category prefix and use
hyphens to separate words. Always branch from the current `develop`:

- `feat/` for new functionality, e.g. `feat/per-key-residency`.
- `fix/` for bug fixes, e.g. `fix/timescale-decimal-serialization`.
- `docs/` for documentation-only changes.
- `refactor/` for internal changes that do not alter behavior.
- `test/` for changes that only add or adjust tests.
- `chore/` for maintenance work such as dependency updates or tooling.

When a branch addresses a tracked issue, include the issue number, e.g.
`fix/142-oom-attribution`.

```bash
git switch develop
git pull origin develop
git switch -c feat/per-key-residency
# work, commit, push, open a pull request into develop
```

### Hotfixes

An urgent fix to the released version is the only exception to the rule that work
starts from `develop`. A hotfix branch is created from `main`, named with the
`fix/` prefix, merged into `main` as a patch release, and then merged back into
`develop` so the fix is not lost.

### Keeping Branches Current

Keep your branch up to date with `develop`. We prefer rebasing over merging to
keep the history linear:

```bash
git fetch origin
git rebase origin/develop
```

Resolve conflicts locally, run the full set of checks again, and continue. Avoid
merge commits inside feature branches.

## Commit Messages

We follow the [Conventional Commits](https://www.conventionalcommits.org/)
specification.

```
<type>(<optional scope>): <short summary>

<optional body>

<optional footer>
```

The summary line is in the imperative mood, does not end with a period, and stays
within roughly fifty characters. The body, when present, explains what changed
and why rather than how, wrapped at seventy-two characters.

Allowed types: `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`,
`build`, `ci`, `chore`. A breaking change is indicated with a `!` after the type
(`feat!:`) or a `BREAKING CHANGE:` footer describing the impact and migration.

```
feat(deepmem): fold memray stacks to the first user source line

Allocations are now attributed to the caller's line rather than into
numpy/dask internals, so the flamegraph reads on your own code.
```

## Pull Requests

- Make sure your branch is rebased on the latest `develop` and that all checks
  pass locally.
- Push your branch and open a pull request against `develop`. The only pull
  requests that target `main` are release merges and hotfixes.
- Fill in the description: explain the motivation, summarize the changes, and
  link related issues with a closing keyword such as `Closes #142`.
- Keep pull requests focused on a single concern.
- Be responsive to review feedback; push follow-up commits during review, and we
  squash them on merge so the final history stays clean.

### Review and Merge

- At least one maintainer approval is required.
- Continuous integration must be green.
- We merge with the squash strategy so each pull request becomes a single commit
  on `develop`. The squash commit message must follow Conventional Commits.

## Testing

We use pytest. New features must be accompanied by tests, and bug fixes should
include a regression test that fails before the fix and passes after it.

```bash
uv run pytest -m "not integration"    # fast, deterministic suite
uv run pytest -m integration          # spins up a real LocalCluster + collector
```

Tests marked `integration` start real clusters and are excluded from the default
run so the standard suite stays fast. Aim for meaningful coverage of the behavior
you add, including error paths (worker death, schema mismatch, memray-unavailable
degradation).

## Versioning and Releases

This project follows [Semantic Versioning](https://semver.org/). While below
`1.0.0` the public interface should be considered unstable, and minor versions
may include breaking changes.

### Cutting a Release

Releases are cut by a maintainer by promoting `develop` to `main` and tagging the
result:

1. Ensure `develop` is green and contains all changes intended for the release.
2. On `develop`, update the version in `pyproject.toml` and in
   `src/daskgenie/__init__.py`, and move the entries under the **Unreleased**
   heading of `CHANGELOG.md` into a new section for the version. Commit this.
3. Open a pull request from `develop` into `main` and merge it (with a regular
   merge commit, not squashed) once approved and green.
4. Check out `main`, pull the merge, and create an annotated tag prefixed with
   `v`:

   ```bash
   git switch main
   git pull origin main
   git tag -a v0.2.0 -m "Release 0.2.0"
   git push origin v0.2.0
   ```

Pushing the tag triggers the publish workflow (`.github/workflows/workflow-pypi.yml`),
which builds the distributions, publishes them to PyPI using a Trusted Publisher
(so no API token is stored in the repository), builds and pushes the collector and
dashboard images to GHCR, and creates the GitHub release. No manual release
command is needed.

Tags must always point to a commit on `main` and must never be moved or deleted
once published. After the release, `main` is merged back into `develop` if the
release introduced commits not already present there.

## Reporting Issues

If you find a bug or want to request a feature, please open an issue. A good
report includes:

- A clear and descriptive title.
- The version of DaskGenie and of Python (and OS) you are using.
- The steps required to reproduce the problem.
- What you expected to happen and what actually happened.
- Any relevant logs or tracebacks.

If you believe you have found a security vulnerability, report it privately to
the maintainers rather than opening a public issue.

Thank you for helping make DaskGenie better.
