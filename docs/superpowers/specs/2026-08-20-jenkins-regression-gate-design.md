# Jenkins Regression Gate Design

## 1. Purpose

Add a Jenkins-backed regression gate to the DingoFS merge-queue flow. The
gate deploys the exact GitHub merge-group commit to one shared regression
environment, runs the DingoFS daily regression suite, and permits the merge
only when Jenkins reports `SUCCESS`.

The regression takes approximately one hour. To avoid running it twice per
pull request and to ensure that the tested revision includes the latest
`main` plus any preceding queued changes, the regression runs only for the
GitHub `merge_group` event. On an ordinary `pull_request` event, the GitHub
Actions job is present but skipped.

## 2. Scope

This design covers:

- the GitHub Actions job that triggers and waits for Jenkins;
- Jenkins authentication and authorization;
- the parameterized Jenkins Pipeline job;
- serialization of access to the shared regression environment;
- checkout and build of the exact merge-group commit;
- deployment of the three DingoFS binaries;
- execution of the existing daily regression command;
- result propagation, artifact collection, cleanup, and rollback;
- branch-protection and merge-queue settings;
- rollout and acceptance testing.

This design does not change the regression suite itself, create per-PR test
environments, or publish merge-group artifacts or Docker images.

## 3. Selected Architecture

```text
GitHub Merge Queue
    |
    | merge_group event (GIT_REF + GIT_SHA)
    v
GitHub Actions job: jenkins-regression
    |
    | Jenkins Remote Access API
    v
Jenkins Pipeline: dingofs-merge-regression
    |
    | build exact SHA, then acquire shared-environment lock
    v
Deploy binaries -> clean environment -> run regression -> restore binaries
    |
    | Jenkins result polling
    v
GitHub required check
    |
    +-- SUCCESS ------------------------------> merge allowed
    +-- FAILURE/UNSTABLE/ABORTED/timeout/error -> merge blocked
```

The Jenkins Pipeline definition is stored directly in the protected Jenkins
job configuration. It must not be loaded from the merge-group revision being
tested because the Pipeline holds deployment credentials and controls a
shared environment. Moving it to a separate protected CI repository can be a
future operational change but is not part of this implementation.

## 4. GitHub Actions Design

### 4.1 Job placement and status-check identity

Add a normal inline job named `jenkins-regression` to
`.github/workflows/pr-check.yml`:

- `needs: e2e`;
- `if: github.event_name == 'merge_group'`;
- `runs-on: ubuntu-latest`;
- an overall timeout of 270 minutes.

The inline job preserves the stable status-check name
`jenkins-regression`. It must not be implemented as a reusable-workflow job.

On `pull_request`, the conditional skips the job. A job-level skip reports a
successful conclusion to required-status-check evaluation. On `merge_group`,
the job runs only after the existing unit-test, release build, and E2E chain
has succeeded, avoiding use of the shared environment for revisions already
known to be bad.

### 4.2 GitHub configuration

Repository variables:

```text
JENKINS_URL=https://lapping-diagnoses-unbeaten.ngrok-free.dev
JENKINS_JOB_PATH=dingofs-merge-regression
```

Repository secrets:

```text
JENKINS_USER=github-dingofs-ci
JENKINS_API_TOKEN=(the generated token for github-dingofs-ci)
```

The repository processes only pull requests from trusted branches in the
same repository. No `pull_request_target` workflow is introduced.

### 4.3 Parameters sent to Jenkins

The GitHub Actions job sends:

- `GIT_REF`: `github.ref`, the merge-queue temporary ref;
- `GIT_SHA`: `github.sha`, the merge-group commit;
- `GITHUB_RUN_ID`;
- `GITHUB_REPOSITORY`;
- `GITHUB_SERVER_URL`.

The DingoFS repository URL and all deployment paths remain fixed in Jenkins
and are not accepted as caller-provided parameters.

### 4.4 Jenkins API lifecycle

The GitHub job performs the following fail-closed sequence:

1. POST to the parameterized-build endpoint using HTTP Basic authentication
   with the Jenkins user and API token.
2. Require a successful HTTP response and capture the queue-item URL from the
   `Location` response header.
3. Validate the returned queue identifier and construct subsequent URLs from
   the configured Jenkins base URL rather than blindly following arbitrary
   redirects.
4. Poll the Queue API until an executable build number is assigned or the
   queue item is cancelled.
5. Poll that build's JSON API until `building` is false.
6. Accept only `result == SUCCESS`.
7. Print the non-secret Jenkins build URL in the GitHub log.
8. On GitHub cancellation, make a best-effort request to cancel the queue item
   or stop the running Jenkins build. Jenkins remains responsible for running
   its rollback logic when a build is stopped.

API tokens are used instead of passwords. Jenkins CSRF protection remains
enabled; API-token-authenticated requests do not require a crumb on supported
Jenkins versions.

The polling implementation belongs in
`.github/scripts/trigger-jenkins.sh`. It must use strict shell error handling,
bounded polling, URL-encoded parameters, and must never print credentials.

## 5. Jenkins Controller Configuration

### 5.1 Required capabilities

Install or verify:

- Pipeline;
- Git;
- Credentials Binding;
- Lockable Resources.

The Pipeline uses `deleteDir()` for build-workspace cleanup, so Workspace
Cleanup is not required by this design.

Configure the Jenkins root URL as:

```text
https://lapping-diagnoses-unbeaten.ngrok-free.dev/
```

The ngrok tunnel must remain available for the complete GitHub wait period,
including Jenkins queue time and regression execution.

### 5.2 Service account

Create a dedicated Jenkins account named `github-dingofs-ci`. Grant only the
permissions needed to inspect Jenkins and trigger the one regression job:

- `Overall/Read`;
- `Job/Read` on `dingofs-merge-regression`;
- `Job/Build` on `dingofs-merge-regression`.

Do not use an administrator token. Store the generated API token only in the
GitHub Actions secret `JENKINS_API_TOKEN`, and rotate it periodically.

Anonymous Jenkins access remains disabled. Deployment, SSH, storage, and test
environment credentials remain in Jenkins Credentials and are never passed
from GitHub.

### 5.3 Shared resource

In **Manage Jenkins -> Lockable Resources**, create:

```text
Name: dingofs-regression-env
Description: DingoFS shared regression environment
```

The lock uses default FIFO ordering. Do not enable inverse precedence. The
lock is held from the first destructive environment operation until reports
have been collected and the previous binaries have been restored.

### 5.4 Pipeline job

Create a Pipeline job:

```text
Name: dingofs-merge-regression
Type: Pipeline
```

Declare string parameters:

- `GIT_REF`;
- `GIT_SHA`;
- `GITHUB_RUN_ID`;
- `GITHUB_REPOSITORY`;
- `GITHUB_SERVER_URL`.

Pipeline-level behavior:

- disable concurrent builds without aborting the running build;
- skip the implicit SCM checkout;
- retain timestamps;
- retain the most recent 30 build records;
- use an overall timeout of 240 minutes;
- use a 90-minute timeout around the regression command;
- treat `UNSTABLE`, `ABORTED`, `NOT_BUILT`, timeout, and any exception as gate
  failure.

## 6. Jenkins Pipeline Lifecycle

### 6.1 Validate input

Before allocating the shared environment:

- require `GIT_SHA` to be a full 40-character hexadecimal commit ID;
- require `GIT_REF` to start with
  `refs/heads/gh-readonly-queue/main/`;
- reject blank correlation parameters;
- use the fixed repository URL
  `https://github.com/dingodb/dingofs.git` configured in the protected
  Pipeline.

Invalid parameters fail the build before any environment mutation.

### 6.2 Checkout the exact merge-group revision

In a clean, build-specific workspace:

1. fetch `GIT_REF` from the fixed DingoFS origin;
2. check out the fetched commit in detached-HEAD mode;
3. verify that `git rev-parse HEAD` exactly equals `GIT_SHA`;
4. synchronize and initialize submodules.

Any mismatch fails the build. The Pipeline must not fall back to `main` or to
the latest remote revision.

### 6.3 Build DingoFS

Reuse the same Rocky 9 release-build recipe used by the GitHub composite
action:

- use `dingodatabase/dingo-eureka:rocky9-fs`;
- build or restore dingo-sdk using the repository's shared build helper;
- run dependency initialization;
- run the Release build for `src/*` with unit tests disabled.

Before deployment, require executable outputs at:

```text
build/bin/dingo-client
build/bin/dingo-mds-client
build/bin/dingo-cache
```

Calculate and record SHA256 checksums for all three outputs. Build failure
occurs before acquiring the environment lock and therefore cannot alter the
shared environment.

### 6.4 Acquire the environment and prepare the test tool

Acquire `dingofs-regression-env`. While holding the lock, update and build the
test tool:

```bash
cd "${DINGOFS_TESTSUITE_TOOL_DIR}"
git checkout main
git pull --ff-only
bash ./build.sh --debug
install -m 0755 dingofs-testsuite-tool \
  /home/jenkins/.local/bin/dingofs-testsuite-tool
```

`DINGOFS_TESTSUITE_TOOL_DIR` is a fixed Jenkins environment setting. The
Pipeline does not accept it from GitHub.

### 6.5 Back up and atomically deploy binaries

Back up the current binaries to a build-specific directory before replacing
anything:

```text
/home/jenkins/.dingo/components/dingo-client/main/dingo-client
/home/jenkins/.dingo/components/dingo-mds-client/main/dingo-mds-client
/home/jenkins/.dingo/components/dingo-cache/main/dingo-cache
```

For each component:

1. verify that the source build output exists and is executable;
2. verify that the target directory is the exact configured directory;
3. copy to a temporary file in the target directory;
4. set the intended owner and executable mode;
5. verify the temporary file checksum matches the source;
6. atomically rename the temporary file over the target;
7. record the deployed checksum.

Do not run any `dingo component update` command. The test environment must use
the binaries built from `GIT_SHA`.

### 6.6 Prepare the regression environment

Still holding the resource lock, perform the requested destructive setup:

```bash
deletefsall
mc rm --recursive --force jenkins126/dingofs
```

Before deleting data, verify that the configured MinIO alias and exact target
are the expected regression target. Do not allow a caller-provided bucket or
prefix.

Clear the fixed client log directory only after validating its exact canonical
path:

```text
/mnt/disk5/dingo_autotest/client_log
```

The former broad process-killing command based on `grep while` is omitted.
Any preparation failure stops the regression and enters rollback.

### 6.7 Run regression

Configure:

```text
PATH=/home/jenkins/miniforge3/bin:${PATH}
PYTHONHOME=/home/jenkins/miniforge3
PYTHONPATH=/home/jenkins/miniforge3/lib/python3.13/site-packages
```

From `DINGOFS_TESTSUITE_TOOL_DIR`, execute with a 90-minute timeout:

```bash
dtt daily \
  --email dingofs@zetyun.com \
  --wechat \
  --daily \
  --report-path /mnt/disk5/daigy/tmp/output \
  --report-port 8889
```

Exit code zero is success. Every other exit code fails the Pipeline. Email and
WeChat notification behavior remains owned by `dtt`.

### 6.8 Collect evidence and roll back

Use a `try/finally` boundary inside the resource lock. On success, failure,
timeout, or cancellation:

1. capture the regression exit state;
2. archive the DTT report, deployment manifest, checksums, and relevant
   component logs;
3. restore all three pre-run binaries using temporary files and atomic
   renames;
4. verify restored SHA256 checksums match the backups;
5. run `deletefsall`, remove `jenkins126/dingofs` through `mc`, and clear the
   validated client-log directory after its logs have been archived;
6. remove build-specific temporary and backup data only after successful
   restoration;
7. release the shared-environment lock.

A restoration or cleanup failure forces the final Jenkins result to
`FAILURE`, even if regression passed. The build log must explicitly mark the
shared environment as requiring manual inspection.

## 7. Merge Queue and Branch Protection

Configure the `main` ruleset or branch protection to:

- require the existing checks `unit-test`, `build`, and `e2e`;
- require the new `jenkins-regression` check;
- require Merge Queue;
- prevent unreviewed direct pushes or merges;
- limit bypass permissions;
- set Merge Queue build concurrency to `1`, matching the single shared
  regression environment;
- set the Merge Queue status-check timeout to 360 minutes. This exceeds the
  existing CI chain plus the 270-minute GitHub job timeout and covers Jenkins
  queueing and the one-hour regression.

Do not add `jenkins-regression` as a required check until Jenkins, its
credentials, GitHub variables/secrets, and at least one successful test run
are all in place. Requiring a check that has never reported can deadlock the
queue in an expected/pending state.

## 8. Failure Semantics

The integration fails closed for:

- Jenkins DNS, TLS, network, or authentication failures;
- non-successful HTTP responses;
- a missing or malformed Jenkins queue location;
- queue cancellation;
- inability to obtain a Jenkins build number within the configured timeout;
- Jenkins `FAILURE`, `UNSTABLE`, `ABORTED`, `NOT_BUILT`, or unknown results;
- Git ref or SHA validation failure;
- checkout or build failure;
- test-tool build failure;
- backup, deployment, preparation, or regression failure;
- failure to archive the DTT report, deployment manifest, checksums, or
  requested component logs;
- binary restoration or environment cleanup failure.

The GitHub check succeeds only when the Jenkins build finishes with
`SUCCESS`.

## 9. Rollout and Acceptance Tests

Roll out in this order:

1. Install the Jenkins plugins and create the lockable resource.
2. Create the restricted Jenkins service account and API token.
3. Create and validate the protected Pipeline job.
4. Configure Jenkins credentials and fixed environment paths.
5. Add GitHub repository variables and secrets.
6. Add the trigger-and-wait script and the non-required GitHub Actions job.
7. Run the integration using a known `main` revision.
8. Verify failure, rollback, cancellation, and serialization behavior.
9. Set Merge Queue build concurrency and timeout.
10. Add `jenkins-regression` to required status checks.
11. Update `.github/CICD.md` with operations and troubleshooting guidance.

Acceptance tests:

- a known-good `main` SHA completes successfully;
- a forced regression failure makes the GitHub check fail;
- before/after checksums prove that all binaries are restored after failure;
- two triggered builds serialize rather than deploy concurrently;
- GitHub cancellation stops or cancels Jenkins and Jenkins restores the
  environment;
- invalid credentials fail without exposing the token;
- an invalid ref or SHA is rejected before environment mutation;
- a Jenkins outage or polling timeout blocks the merge;
- an ordinary PR shows `jenkins-regression` as skipped;
- a merge-group run executes Jenkins and cannot merge until Jenkins succeeds.

## 10. Operational Notes

- The ngrok endpoint is part of the critical merge path. Tunnel availability,
  TLS validity, and Jenkins root-URL correctness must be monitored.
- The Jenkins build URL should be visible in GitHub logs, but credentials and
  authenticated URLs must never be printed.
- Test-tool `main`, the Rocky 9 build image, and any external dependencies can
  drift over time. Their exact revisions or image IDs should be recorded in
  Jenkins logs for reproducibility.
- If the shared environment is marked unhealthy after a rollback failure,
  operators must reserve the Lockable Resource until it is repaired rather
  than manually allowing later jobs to proceed.
