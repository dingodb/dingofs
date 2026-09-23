#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)

python3 - "${ROOT}" <<'PY'
import json
import os
import pathlib
import re
import subprocess
import sys
import tempfile
from types import SimpleNamespace

try:
    import yaml
except ImportError:
    yaml = None


root = pathlib.Path(sys.argv[1])
pr_check_path = root / ".github/workflows/pr-check.yml"
source_path = root / ".github/workflows/pr-source.yml"
pipeline_path = root / "scripts/jenkins/dingofs-merge-regression.Jenkinsfile"


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


def validate_branch_filters(workflow, events):
    for event in events:
        patterns = workflow["on"][event]["branches"]
        for branch, expected in (
            ("main", True),
            ("v5.2", True),
            ("v5.10", True),
            ("v5.1", True),
            ("v4.2", False),
            ("v6.0", False),
            ("vnext", False),
            ("v50", False),
            ("v5-test", False),
            ("v5.", False),
            ("v5.2-debug", False),
            ("v5.2.0", False),
            ("release-5.2", False),
            ("feature/v5.2", False),
            ("v5.2/topic", False),
        ):
            # Model the filters' GitHub glob subset: *, [0-9], and +.
            matched = any(
                re.fullmatch(
                    re.escape(pattern)
                    .replace(r"\*", "[^/]*")
                    .replace(r"\[0\-9\]", "[0-9]")
                    .replace(r"\+", "+"),
                    branch,
                )
                for pattern in patterns
            )
            require(
                matched == expected,
                f"{event}: branch {branch} admission is {matched}, expected {expected}",
            )


def runnable_jobs(workflow, context):
    runnable = set()
    for name, job in workflow["jobs"].items():
        # These gates use string equality, boolean operators and startsWith.
        # Evaluate their outcomes instead of pinning expression formatting.
        expression = job.get("if", "True").strip()
        if expression.startswith("${{") and expression.endswith("}}"):
            expression = expression[3:-2].strip()
        expression = expression.replace("&&", " and ").replace("||", " or ")
        if eval(expression, {"__builtins__": {}}, context):
            runnable.add(name)
    return runnable


def validate_job_routes(workflow):
    for event, branch, enabled, expected in (
        ("pull_request", "main", "", set()),
        ("pull_request", "v5.2", "", set()),
        ("merge_group", "main", "", {"unit-test", "build", "e2e", "jenkins-regression"}),
        ("merge_group", "v5.2", "", {"unit-test", "build", "e2e"}),
        ("merge_group", "main", "false", {"unit-test", "build", "e2e"}),
    ):
        context = {
            "github": SimpleNamespace(
                event_name=event,
                event=SimpleNamespace(
                    merge_group=SimpleNamespace(
                        base_ref=f"refs/heads/{branch}" if event == "merge_group" else "",
                    ),
                ),
            ),
            "vars": SimpleNamespace(JENKINS_REGRESSION_ENABLED=enabled),
        }
        runnable = runnable_jobs(workflow, context)
        require(
            runnable == expected,
            f"{event}/{branch}, Jenkins switch={enabled!r}: "
            f"runnable jobs {sorted(runnable)}, expected {sorted(expected)}",
        )


def validate_release_routes(workflow):
    image_jobs = {"build", "docker-publish"}
    for ref, expected in (
        ("refs/heads/main", image_jobs | {"wheels"}),
        ("refs/heads/v5.2", image_jobs),
        ("refs/heads/v5.3", image_jobs),
        ("refs/tags/v5.2.0", image_jobs | {"wheels", "pypi-publish"}),
        ("refs/tags/v5.2.0-rc.1", image_jobs | {"wheels", "pypi-publish"}),
    ):
        runnable = runnable_jobs(
            workflow,
            {
                "github": SimpleNamespace(ref=ref),
                "startsWith": lambda value, prefix: value.lower().startswith(prefix.lower()),
            },
        )
        require(
            runnable == expected,
            f"release/{ref}: runnable jobs {sorted(runnable)}, expected {sorted(expected)}",
        )


def validate_source_workflow(workflow):
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
        set(job) == {"if", "runs-on", "steps"}
        and job["runs-on"] == "ubuntu-latest",
        "pr-source.yml: trusted-source has unexpected fields",
    )
    require(
        job["if"] == "vars.TRUSTED_SOURCE_ENABLED != 'false'",
        "pr-source.yml: trusted-source must honor TRUSTED_SOURCE_ENABLED",
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
        {"name", "if", "run"}.issubset(source_step)
        and set(source_step).issubset({"name", "if", "env", "run"}),
        "pr-source.yml: source admission step has unexpected fields",
    )
    require(
        source_step["if"] == "github.event_name == 'pull_request_target'",
        "pr-source.yml: source check must run only for pull_request_target",
    )
    source_script = source_step["run"]
    require(
        isinstance(source_script, str),
        "pr-source.yml: source admission run must be a script",
    )

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
    require(
        fork.returncode == 0,
        f"pr-source.yml: fork source admission failed: {fork.stderr.strip()}",
    )

    require(
        set(merge_group_step) == {"name", "if", "run"},
        "pr-source.yml: merge-group step has unexpected fields",
    )
    require(
        merge_group_step["if"] == "github.event_name == 'merge_group'",
        "pr-source.yml: merge-group step must run only for merge_group",
    )
    merge_group_script = merge_group_step["run"]
    require(
        isinstance(merge_group_script, str),
        "pr-source.yml: merge-group admission must be a script",
    )
    merge_group = subprocess.run(
        ["bash", "-c", "set -euo pipefail\n" + merge_group_script],
        env=base_env,
        capture_output=True,
        text=True,
        check=False,
    )
    require(
        merge_group.returncode == 0,
        f"pr-source.yml: merge-group admission failed: {merge_group.stderr.strip()}",
    )


def validate_jenkins_job(workflow):
    require(
        workflow.get("permissions")
        == {"contents": "read", "pull-requests": "read"},
        "pr-check.yml: permissions must allow PR metadata lookup",
    )
    jobs = workflow.get("jobs")
    require(isinstance(jobs, dict), "pr-check.yml: jobs must be a mapping")
    require("jenkins-regression" in jobs, "pr-check.yml: missing jenkins-regression job")
    job = jobs["jenkins-regression"]
    require(
        set(job)
        == {"if", "runs-on", "environment", "timeout-minutes", "steps"},
        "pr-check.yml: jenkins-regression has unexpected fields",
    )
    validate_job_routes(workflow)
    require(job["runs-on"] == "ubuntu-latest", "pr-check.yml: wrong Jenkins runner")
    require(
        job["environment"] == "jenkins-regression",
        "pr-check.yml: Jenkins job must use the jenkins-regression Environment",
    )
    require(job["timeout-minutes"] == "270", "pr-check.yml: wrong Jenkins timeout")

    require(
        jobs["unit-test"].get("if") == "github.event_name == 'merge_group'",
        "pr-check.yml: unit-test must run only for merge groups",
    )
    require("needs" not in jobs["unit-test"],
            "pr-check.yml: unit-test must start immediately in merge groups")
    require(jobs["build"].get("needs") == "unit-test",
            "pr-check.yml: build must wait for unit-test")
    require(jobs["e2e"].get("needs") == "build",
            "pr-check.yml: e2e must wait for build")

    steps = job["steps"]
    require(
        isinstance(steps, list) and len(steps) == 3,
        "pr-check.yml: Jenkins job must resolve PR metadata before triggering Jenkins",
    )
    checkout_step, metadata_step, run_step = steps
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
        set(metadata_step) == {"name", "id", "env", "run"}
        and metadata_step["id"] == "pr-meta",
        "pr-check.yml: missing merge queue PR metadata step",
    )
    require(
        metadata_step["env"]
        == {
            "GH_TOKEN": "${{ github.token }}",
            "MERGE_REF": "${{ github.ref }}",
            "REPOSITORY": "${{ github.repository }}",
            "API_URL": "${{ github.api_url }}",
        },
        "pr-check.yml: PR metadata resolver environment mismatch",
    )
    with tempfile.TemporaryDirectory() as temp_dir:
        output_path = pathlib.Path(temp_dir) / "github-output"
        metadata_result = subprocess.run(
            [
                "/usr/bin/bash",
                "--noprofile",
                "--norc",
                "-c",
                "set -euo pipefail\n" + metadata_step["run"],
            ],
            cwd=root,
            env={
                "PATH": str(root / "scripts/test/fixtures/jenkins")
                + os.pathsep
                + os.environ.get("PATH", ""),
                "GH_TOKEN": "test-token",
                "MERGE_REF": (
                    "refs/heads/gh-readonly-queue/main/"
                    "pr-1083-c49f80eab51c019f276ea17459727ea3d9080d87"
                ),
                "REPOSITORY": "dingodb/dingofs",
                "API_URL": "https://api.github.test",
                "GITHUB_OUTPUT": str(output_path),
            },
            capture_output=True,
            text=True,
            check=False,
        )
        require(
            metadata_result.returncode == 0,
            "pr-check.yml: PR metadata resolver failed: "
            + metadata_result.stderr.strip(),
        )
        require(
            output_path.read_text().splitlines()
            == ["pr_number=1083", "pr_author=octocat"],
            "pr-check.yml: PR metadata resolver returned wrong values",
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
            "PR_NUMBER": "${{ steps.pr-meta.outputs.pr_number }}",
            "PR_AUTHOR": "${{ steps.pr-meta.outputs.pr_author }}",
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




def validate_topology_template():
    pipeline = pipeline_path.read_text()
    match = re.search(
        r"cat >\"\$\{topology_file\}\" <<'TOPOLOGY'\n(.*?)\nTOPOLOGY",
        pipeline,
        flags=re.DOTALL,
    )
    require(
        match is not None,
        "Jenkinsfile: topology heredoc must be single-quoted to preserve dingo variables",
    )
    for marker, shell_variable in (
        ("@CANDIDATE_IMAGE_TAG@", "candidate_image_tag"),
        ("@CLUSTER_RUNTIME@", "cluster_runtime"),
        ("@STORE_IMAGE@", "STORE_IMAGE"),
        ("@EXECUTOR_IMAGE@", "EXECUTOR_IMAGE"),
    ):
        require(
            f's|{marker}|${{{shell_variable}}}|g' in pipeline,
            f"Jenkinsfile: topology renderer must replace {marker}",
        )
    template = match.group(1)
    rendered = (
        template.replace("@CANDIDATE_IMAGE_TAG@", "candidate-image")
        .replace("@CLUSTER_RUNTIME@", "/regression/runtime")
        .replace("@STORE_IMAGE@", "store-image")
        .replace("@EXECUTOR_IMAGE@", "executor-image")
    )
    require(
        "/regression/runtime/data/${service_role}${service_host_sequence}"
        in rendered,
        "Jenkinsfile: rendered topology must preserve dingo service variables",
    )
    require(
        all(
            marker not in rendered
            for marker in (
                "@CANDIDATE_IMAGE_TAG@",
                "@CLUSTER_RUNTIME@",
                "@STORE_IMAGE@",
                "@EXECUTOR_IMAGE@",
            )
        ),
        "Jenkinsfile: rendered topology contains unresolved protected markers",
    )
try:
    pr_check = load_workflow(pr_check_path)
    source = load_workflow(source_path)
    validate_branch_filters(pr_check, ("pull_request", "merge_group"))
    validate_branch_filters(source, ("pull_request_target", "merge_group"))
    release = load_workflow(root / ".github/workflows/release.yml")
    validate_branch_filters(release, ("push",))
    validate_release_routes(release)
    validate_source_workflow(source)
    validate_jenkins_job(pr_check)
    validate_topology_template()
except (AssertionError, KeyError, TypeError, WorkflowLoadError) as error:
    print(f"workflow contract failed: {error}", file=sys.stderr)
    raise SystemExit(1)

print("PASS: PR Check Jenkins contract")
PY

bash "${ROOT}/scripts/test/test_trigger_jenkins_metadata.sh"

