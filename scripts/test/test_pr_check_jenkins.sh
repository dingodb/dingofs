#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)

python3 - "${ROOT}" <<'PY'
import json
import os
import pathlib
import subprocess
import sys

try:
    import yaml
except ImportError:
    yaml = None


root = pathlib.Path(sys.argv[1])
pr_check_path = root / ".github/workflows/pr-check.yml"
source_path = root / ".github/workflows/pr-source.yml"


class WorkflowLoadError(RuntimeError):
    pass


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def normalize_yq(value):
    if value is None:
        # JSON loses whether YAML used an empty value, `null`, or `~`. Use the
        # BaseLoader representation of the common empty-value form (`key:`).
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, dict):
        return {str(key): normalize_yq(child) for key, child in value.items()}
    if isinstance(value, list):
        return [normalize_yq(child) for child in value]
    return value


def load_workflow(path):
    if yaml is not None:
        try:
            data = yaml.load(path.read_text(), Loader=yaml.BaseLoader)
        except yaml.YAMLError as error:
            raise WorkflowLoadError(f"{path.name}: invalid YAML: {error}") from error
    else:
        try:
            result = subprocess.run(
                ["yq", "-o=json", str(path)],
                capture_output=True,
                text=True,
                check=False,
            )
        except FileNotFoundError as error:
            raise WorkflowLoadError(
                "PyYAML is unavailable and yq was not found in PATH"
            ) from error
        if result.returncode != 0:
            detail = result.stderr.strip() or f"exit code {result.returncode}"
            raise WorkflowLoadError(f"{path.name}: yq failed: {detail}")
        try:
            data = normalize_yq(json.loads(result.stdout))
        except json.JSONDecodeError as error:
            raise WorkflowLoadError(f"{path.name}: yq returned invalid JSON: {error}") from error
    require(isinstance(data, dict), f"{path.name}: workflow must be a mapping")
    return data


def strings(value):
    if isinstance(value, dict):
        for key, child in value.items():
            yield str(key)
            yield from strings(child)
    elif isinstance(value, list):
        for child in value:
            yield from strings(child)
    elif value is not None:
        yield str(value)


def keys(value):
    if isinstance(value, dict):
        for key, child in value.items():
            yield str(key)
            yield from keys(child)
    elif isinstance(value, list):
        for child in value:
            yield from keys(child)


def require_external_configuration(path, required_lines):
    lines = {line.strip() for line in path.read_text().splitlines()}
    for line in required_lines:
        require(line in lines, f"{path.name}: missing external configuration note: {line}")


def validate_source_workflow(workflow):
    require(
        workflow.get("on")
        == {
            "pull_request_target": {"branches": ["main"]},
            "merge_group": {"branches": ["main"]},
        },
        "pr-source.yml: events must be pull_request_target and merge_group for main",
    )
    require(
        workflow.get("permissions") == {"contents": "read"},
        "pr-source.yml: permissions must be contents: read",
    )

    jobs = workflow.get("jobs")
    require(
        isinstance(jobs, dict) and set(jobs) == {"trusted-source"},
        "pr-source.yml: trusted-source must be the only job",
    )
    job = jobs["trusted-source"]
    require(
        set(job) == {"runs-on", "steps"} and job["runs-on"] == "ubuntu-latest",
        "pr-source.yml: trusted-source may contain only runs-on and steps",
    )
    require("environment" not in set(keys(job)), "pr-source.yml: Environment is forbidden")
    require(
        not any("secrets." in value for value in strings(job)),
        "pr-source.yml: secret references are forbidden",
    )
    require(
        not any("checkout" in value.lower() for value in strings(job)),
        "pr-source.yml: checkout is forbidden",
    )

    steps = job["steps"]
    require(
        isinstance(steps, list) and len(steps) == 2,
        "pr-source.yml: trusted-source must contain exactly two steps",
    )
    source_step, merge_group_step = steps
    require(
        set(source_step) == {"name", "if", "env", "run"},
        "pr-source.yml: source check step has unexpected fields",
    )
    require(
        source_step["if"] == "github.event_name == 'pull_request_target'",
        "pr-source.yml: source check must run only for pull_request_target",
    )
    require(
        source_step["env"]
        == {
            "HEAD_REPOSITORY": "${{ github.event.pull_request.head.repo.full_name }}",
            "BASE_REPOSITORY": "${{ github.event.pull_request.base.repo.full_name }}",
        },
        "pr-source.yml: source check must compare event head and base repositories",
    )
    source_script = source_step["run"]
    require(isinstance(source_script, str), "pr-source.yml: source check run must be a script")

    base_env = {"PATH": os.environ.get("PATH", "")}
    same_repo = subprocess.run(
        ["bash", "-c", "set -euo pipefail\n" + source_script],
        env={
            **base_env,
            "HEAD_REPOSITORY": "dingodb/dingofs",
            "BASE_REPOSITORY": "dingodb/dingofs",
        },
        capture_output=True,
        text=True,
        check=False,
    )
    require(
        same_repo.returncode == 0,
        f"pr-source.yml: same-repository source check failed: {same_repo.stderr.strip()}",
    )
    fork = subprocess.run(
        ["bash", "-c", "set -euo pipefail\n" + source_script],
        env={
            **base_env,
            "HEAD_REPOSITORY": "contributor/dingofs",
            "BASE_REPOSITORY": "dingodb/dingofs",
        },
        capture_output=True,
        text=True,
        check=False,
    )
    require(fork.returncode != 0, "pr-source.yml: fork source check must fail")

    require(
        set(merge_group_step) == {"name", "if", "run"},
        "pr-source.yml: merge-group step has unexpected fields",
    )
    require(
        merge_group_step["if"] == "github.event_name == 'merge_group'",
        "pr-source.yml: merge-group step must run only for merge_group",
    )
    require(
        merge_group_step["run"]
        == 'echo "The merge group was admitted only after the PR source gate passed"',
        "pr-source.yml: merge-group step must only describe prior gate admission",
    )


def validate_jenkins_job(workflow):
    require(
        workflow.get("permissions") == {"contents": "read"},
        "pr-check.yml: permissions must be contents: read",
    )
    jobs = workflow.get("jobs")
    require(isinstance(jobs, dict), "pr-check.yml: jobs must be a mapping")
    require("jenkins-regression" in jobs, "pr-check.yml: missing jenkins-regression job")
    job = jobs["jenkins-regression"]
    require(
        set(job)
        == {"needs", "if", "runs-on", "environment", "timeout-minutes", "steps"},
        "pr-check.yml: jenkins-regression has unexpected fields",
    )
    require(job["needs"] == "e2e", "pr-check.yml: Jenkins job must need e2e")
    require(
        job["if"] == "github.event_name == 'merge_group' && needs.e2e.result == 'success'",
        "pr-check.yml: Jenkins job must run only for a successful merge-group e2e",
    )
    require(job["runs-on"] == "ubuntu-latest", "pr-check.yml: wrong Jenkins runner")
    require(
        job["environment"] == "jenkins-regression",
        "pr-check.yml: Jenkins job must use the jenkins-regression Environment",
    )
    require(job["timeout-minutes"] == "270", "pr-check.yml: wrong Jenkins timeout")

    steps = job["steps"]
    require(
        isinstance(steps, list) and len(steps) == 2,
        "pr-check.yml: Jenkins job must contain exactly two steps",
    )
    checkout_step, run_step = steps
    require(
        set(checkout_step) == {"name", "uses", "with"},
        "pr-check.yml: trusted checkout step has unexpected fields",
    )
    require(
        checkout_step["uses"] == "actions/checkout@v4",
        "pr-check.yml: trusted checkout must use actions/checkout@v4",
    )
    require(
        checkout_step["with"]
        == {
            "repository": "dingodb/dingofs",
            "ref": "refs/heads/main",
            "path": "trusted-main",
            "persist-credentials": "false",
        },
        "pr-check.yml: trusted checkout repository/ref/path/credentials mismatch",
    )

    require(
        set(run_step) == {"name", "env", "run"},
        "pr-check.yml: Jenkins trigger step has unexpected fields",
    )
    require(
        run_step["env"]
        == {
            "JENKINS_URL": "${{ vars.JENKINS_URL }}",
            "JENKINS_JOB_PATH": "${{ vars.JENKINS_JOB_PATH }}",
            "JENKINS_USER": "${{ secrets.JENKINS_USER }}",
            "JENKINS_API_TOKEN": "${{ secrets.JENKINS_API_TOKEN }}",
            "GIT_REF": "${{ github.ref }}",
            "GIT_SHA": "${{ github.sha }}",
            "GITHUB_RUN_ID": "${{ github.run_id }}",
            "GITHUB_REPOSITORY": "${{ github.repository }}",
            "GITHUB_SERVER_URL": "${{ github.server_url }}",
        },
        "pr-check.yml: Jenkins trigger environment mapping mismatch",
    )
    require(
        run_step["run"]
        == 'bash "${GITHUB_WORKSPACE}/trusted-main/.github/scripts/trigger-jenkins.sh"',
        "pr-check.yml: Jenkins job may execute only the trusted-main trigger",
    )


def validate_contract_test_step(workflow):
    unit_job = workflow["jobs"]["unit-test"]
    steps = unit_job["steps"]
    checkout_indexes = [
        index for index, step in enumerate(steps) if step.get("uses") == "actions/checkout@v4"
    ]
    require(len(checkout_indexes) == 1, "pr-check.yml: unit-test must have one checkout step")
    checkout_index = checkout_indexes[0]
    require(
        checkout_index + 1 < len(steps),
        "pr-check.yml: contract test step must follow unit-test checkout",
    )
    contract_step = steps[checkout_index + 1]
    require(
        set(contract_step) == {"name", "run"}
        and contract_step["run"] == "bash scripts/test/test_pr_check_jenkins.sh",
        "pr-check.yml: contract test must run immediately after unit-test checkout",
    )


try:
    pr_check = load_workflow(pr_check_path)
    source = load_workflow(source_path)
    validate_source_workflow(source)
    validate_jenkins_job(pr_check)
    validate_contract_test_step(pr_check)
    require_external_configuration(
        source_path,
        {
            "# - source repository: dingodb/dingofs",
            "# - workflow path: .github/workflows/pr-source.yml",
            "# - ref: refs/heads/main",
            "# - merge queue grouping strategy: ALLGREEN",
            "# - no bypass actors",
        },
    )
    require_external_configuration(
        pr_check_path,
        {
            "# - require status checks: unit-test, build, e2e, jenkins-regression",
            "# - expected source for each status check: GitHub Actions",
            "# - do not also add this file as an organization required workflow",
            "# - Environment jenkins-regression: no required reviewers",
            "# - store JENKINS_USER and JENKINS_API_TOKEN only as Environment secrets",
            "# Candidate-controlled test: honest regression coverage only, not a security boundary.",
        },
    )
except (AssertionError, KeyError, TypeError, WorkflowLoadError) as error:
    print(f"workflow contract failed: {error}", file=sys.stderr)
    raise SystemExit(1)

print("PASS: PR Check Jenkins contract")
PY

bash "${ROOT}/scripts/test/test_jenkins_setup_docs.sh"

# GitHub-hosted runners do not guarantee that ripgrep is installed. Exercise
# the documentation contract with a minimal PATH that provides grep but not rg.
fallback_path=$(mktemp -d)
cleanup_fallback_path() {
  rm -rf -- "${fallback_path}"
}
trap cleanup_fallback_path EXIT
ln -s "$(command -v dirname)" "${fallback_path}/dirname"
ln -s "$(command -v grep)" "${fallback_path}/grep"
PATH="${fallback_path}" /usr/bin/bash \
  "${ROOT}/scripts/test/test_jenkins_setup_docs.sh"
