# Milestone 28 Infrastructure as Code

## Scope

Every cluster add-on in this lab - Sealed Secrets, Argo Rollouts, Argo CD, KEDA, Linkerd, Kyverno, CloudNativePG - was installed via a one-off `helm install` command typed during whichever milestone introduced it, documented in that milestone's own report but never captured as something re-runnable. This milestone codifies the host + cluster-addon layer as an Ansible playbook (`iac/ansible/`) and - the actual point of the exercise - proves it by running it against the real, already-provisioned server and confirming it converges to a no-op, not by writing a playbook and assuming it works.

Deliberately scoped to the add-on layer, not the applications: `kubernetes/base`/`kubernetes/overlays/local` are Argo CD's job (Milestone 15) and stay that way. This also deliberately does **not** attempt an actual destroy-and-rebuild of the live server - proving idempotency by converging a real host to a no-op is real validation; gambling the one server this entire lab runs on to "prove" reproducibility is a different, much larger risk decision that wasn't authorized for this milestone.

## Design

- **`iac/ansible/roles/docker`, `roles/k3s`**: install Docker and K3s (pinned to `v1.36.2+k3s1`, the version this cluster actually runs) - both genuinely need root, which this environment's non-sudo user can't provide, so both check first and no-op on every host this playbook has actually touched. Written for reproducing a fresh host, not exercised end-to-end here - documented as exactly that, not silently assumed to work.
- **`roles/cluster-addons`**: a data-driven loop (`group_vars/all.yml`'s `helm_releases` list) over `kubernetes.core.helm_repository` + `kubernetes.core.helm` for the six single-step add-ons, plus a dedicated block for Linkerd's multi-step CNI-mode install (Milestone 24's sequence: CRDs, CNI plugin, control plane, viz) since it doesn't fit the generic one-`helm install` shape.
- **Cluster policies**: `kubernetes.core.k8s` applies the Kyverno and Linkerd `AuthorizationPolicy` manifests directly from `kubernetes/cluster-policies/` - the playbook is the *application* mechanism for files that already live in git, not a duplicate copy of their content.

## What didn't work

**The K3s role's kubeconfig setup needed `become: true` for a task that shouldn't need root at all - and would have silently clobbered a file that was already correctly in place.** First `--check` run failed outright: `sudo: a password is required`. The task tried to *symlink* `~/.kube/config` to `/etc/rancher/k3s/k3s.yaml`, which happened to need elevated access for reasons unrelated to the actual goal. Checking the real file directly (`ls -la ~/.kube/config /etc/rancher/k3s/k3s.yaml`) showed `~/.kube/config` already existed as a plain, user-owned file - not a symlink - and `k3s.yaml` was already world-readable (`--write-kubeconfig-mode 644`, set at install time). Rewritten as a plain `copy` (world-readable source into the user's own home directory, no elevation needed at all) guarded by a `stat` check that skips the whole block when a working kubeconfig is already there - which is every time, on this host.

**`kubernetes.core.helm_repository` failed outright for 2 of 6 repos with "Repository already have a repository named X" - despite the stored URL being byte-for-byte identical to what the playbook specified, and 4 other repos with the exact same already-exists condition succeeding as clean no-ops in the same run.** Inspecting `~/.config/helm/repositories.yaml` directly ruled out a real URL mismatch - this reads as a genuine idempotency bug in the module itself (likely an ordering/timing artifact across the loop, not anything content-specific to those two repos), not something fixable by changing what this playbook declares. Worked around with `force_update: true`, at the honest cost of "changed" being reported on every run for the Helm-repo-add step even when nothing about the target state actually changed - documented as an accepted trade-off (always-converges beats occasionally-fails-outright) rather than hidden.

**While tracking down the repo error, a second, unrelated and more interesting bug surfaced: `sealed-secrets`'s URL in this playbook was the exact stale `bitnami-labs.github.io` address Milestone 17 had already found and fixed on the live cluster - reintroduced here by writing the playbook from memory instead of checking `helm repo list` on the actual server first.** A live lab's own operational history is a more reliable source of truth than remembering what a milestone report said months of (lab) time later - checking `helm repo list` directly caught it immediately, and the same check surfaced the real cause of the two "already have a repository" failures too: none of the six configured repos anywhere on this server matched the "moved" URL for anything else, ruling out a broader pattern.

**A separate cluster-policy manifest silently didn't exist on the server's working copy at all**, even though it was already live-applied to the cluster (Milestone 25) and already committed to git - it had only ever been `kubectl apply`'d from a `/tmp/` scratch copy during that milestone's debugging, never actually `rsync`'d into this server's tracked repo path the way every other file in this session has been. The playbook's `k8s` task failed with a plain file-not-found - a real, if minor, gap between "committed to git" and "present in this server's own working tree" that this milestone's validation run is exactly the kind of check that catches.

## Results

Final `--check --diff` run against the live server, after both real bugs above were fixed:

```
PLAY RECAP
lab-local  : ok=6  changed=1  unreachable=0  failed=0  skipped=12  rescued=0  ignored=0
```

The single `changed` is the accepted `force_update: true` trade-off on the Helm-repo-add step - every other task, across Docker, K3s, all six Helm-based add-ons, Linkerd's four-step CNI install, and both cluster policies, reports a clean no-op against the real, live cluster. A subsequent **real** (non-check) run produced the identical result - `ok=8 changed=1 failed=0` - confirming the module's own "check mode can fail to report changes in certain cases" warning didn't hide anything here.

### Regression check

`k3s-smoke-test.sh`: passing, before and after the real playbook run. `kubectl get pods -A` showed zero pods disturbed by the run beyond four pre-existing, unrelated `Error`-state Jobs already 37 hours old at the time (nothing this milestone touched).

## Running the experiment

```bash
cd iac/ansible
pip install --user ansible-core kubernetes
ansible-galaxy collection install kubernetes.core community.docker

ansible-playbook -i inventory.ini site.yml --check --diff   # prove it's a no-op against a correctly-configured host
ansible-playbook -i inventory.ini site.yml                  # apply for real (safe - every task above is idempotent)
```
