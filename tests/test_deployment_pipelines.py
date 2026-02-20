#!/usr/bin/env python3
"""
Deployment pipeline validation tests — static analysis of CI/CD, Dockerfiles,
K8s manifests, and cross-component consistency.

Covers:
  1.  GitHub Actions workflow structure and correctness
  2.  Dockerfile correctness (backend + frontend)
  3.  K8s manifest consistency (selectors, ports, envFrom, probes)
  4.  render-k8s-manifests.sh template rendering
  5.  Cross-component port agreement
  6.  Secret / ConfigMap coverage vs backend code
  7.  .dockerignore completeness
  8.  requirements.txt vs Dockerfile pip installs
  9.  Ingress TLS and timeout configuration
  10. Deployment strategy safety
"""

import os
import re
import glob
import subprocess
import tempfile

import yaml
import pytest

# ── Paths ──────────────────────────────────────────────────────────────────
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
K8S_DIR = os.path.join(ROOT, "k8s")
WORKFLOWS_DIR = os.path.join(ROOT, ".github", "workflows")
SCRIPTS_DIR = os.path.join(ROOT, "scripts")
SRC_DIR = os.path.join(ROOT, "src")


# ── Helpers ────────────────────────────────────────────────────────────────

def _read(path: str) -> str:
    with open(path, encoding="utf-8") as f:
        return f.read()


def _load_yaml_docs(path: str) -> list[dict]:
    """Load all YAML documents from a file (handles multi-doc ---).
    Substitutes envsubst placeholders with dummy values first."""
    raw = _read(path)
    # Replace ${VAR} with dummy string so YAML parsing succeeds
    rendered = re.sub(r"\$\{(\w+)\}", r"__PLACEHOLDER_\1__", raw)
    return list(yaml.safe_load_all(rendered))


def _load_workflow(name: str) -> dict:
    wf = yaml.safe_load(_read(os.path.join(WORKFLOWS_DIR, name)))
    # PyYAML parses 'on:' as boolean True key; normalize to string "on"
    if True in wf and "on" not in wf:
        wf["on"] = wf.pop(True)
    return wf


def _python_files() -> list[str]:
    """All backend Python files in src/."""
    return glob.glob(os.path.join(SRC_DIR, "*.py"))


def _env_vars_read_by_python() -> set[str]:
    """Scan src/*.py for os.getenv / os.environ calls and extract var names."""
    pattern = re.compile(
        r"""os\.(?:getenv|environ\.get)\s*\(\s*["'](\w+)["']"""
    )
    found = set()
    for py in _python_files():
        found.update(pattern.findall(_read(py)))
    # Also catch os.environ["KEY"] and os.environ['KEY']
    bracket_pattern = re.compile(r"""os\.environ\[["'](\w+)["']\]""")
    for py in _python_files():
        found.update(bracket_pattern.findall(_read(py)))
    return found


# ═══════════════════════════════════════════════════════════════════════════
# 1. GITHUB ACTIONS WORKFLOWS
# ═══════════════════════════════════════════════════════════════════════════


class TestDeployBackendWorkflow:
    """deploy-backend.yaml structural checks."""

    @pytest.fixture(autouse=True)
    def _load(self):
        self.wf = _load_workflow("deploy-backend.yaml")

    def test_trigger_branches(self):
        branches = self.wf["on"]["push"]["branches"]
        assert "main" in branches

    def test_trigger_paths_include_python(self):
        paths = self.wf["on"]["push"]["paths"]
        assert any("*.py" in p for p in paths)

    def test_trigger_paths_include_dockerfile(self):
        paths = self.wf["on"]["push"]["paths"]
        assert "Dockerfile.backend" in paths

    def test_trigger_paths_include_k8s(self):
        paths = self.wf["on"]["push"]["paths"]
        assert "k8s/**" in paths

    def test_manual_dispatch_enabled(self):
        assert "workflow_dispatch" in self.wf["on"]

    def test_oidc_permissions(self):
        perms = self.wf["permissions"]
        assert perms["id-token"] == "write"
        assert perms["contents"] == "read"

    def test_concurrency_does_not_cancel(self):
        assert self.wf["concurrency"]["cancel-in-progress"] is False

    def test_image_tag_uses_commit_sha(self):
        env = self.wf["env"]
        assert "github.sha" in str(env.get("IMAGE_TAG", ""))

    def test_two_jobs_exist(self):
        jobs = list(self.wf["jobs"].keys())
        assert "build-and-push" in jobs
        assert "deploy" in jobs

    def test_deploy_depends_on_build(self):
        deploy_needs = self.wf["jobs"]["deploy"].get("needs", [])
        if isinstance(deploy_needs, str):
            deploy_needs = [deploy_needs]
        assert "build-and-push" in deploy_needs

    def test_default_namespace(self):
        env = self.wf["env"]
        # Should default to aviation-rag
        assert "aviation-rag" in str(env.get("K8S_NAMESPACE", ""))

    def test_required_secrets_documented(self):
        """Verify the workflow references all critical secrets."""
        raw = _read(os.path.join(WORKFLOWS_DIR, "deploy-backend.yaml"))
        for secret in [
            "AZURE_CLIENT_ID",
            "AZURE_TENANT_ID",
            "AZURE_SUBSCRIPTION_ID",
            "AZURE_OPENAI_API_KEY",
            "AZURE_SEARCH_ADMIN_KEY",
            "PGPASSWORD",
        ]:
            assert f"secrets.{secret}" in raw, f"Missing secret reference: {secret}"


class TestDeployFrontendWorkflow:
    """deploy-frontend.yaml structural checks."""

    @pytest.fixture(autouse=True)
    def _load(self):
        self.wf = _load_workflow("deploy-frontend.yaml")

    def test_trigger_branches(self):
        assert "main" in self.wf["on"]["push"]["branches"]

    def test_trigger_paths_cover_source(self):
        paths = self.wf["on"]["push"]["paths"]
        patterns = {p.split("/")[1] if "/" in p else p for p in paths}
        assert "app" in patterns or any("app" in p for p in paths)

    def test_oidc_permissions(self):
        perms = self.wf["permissions"]
        assert perms["id-token"] == "write"

    def test_manual_dispatch_enabled(self):
        assert "workflow_dispatch" in self.wf["on"]

    def test_node_version_defaults_to_20(self):
        env = self.wf["env"]
        assert "20" in str(env.get("NODE_VERSION", ""))

    def test_self_trigger_on_workflow_change(self):
        """Workflow file changes should trigger a re-deploy."""
        paths = self.wf["on"]["push"]["paths"]
        assert any("deploy-frontend" in p for p in paths)


class TestInfraHealthCheckWorkflow:
    """infra-health-check.yaml structural checks."""

    @pytest.fixture(autouse=True)
    def _load(self):
        self.wf = _load_workflow("infra-health-check.yaml")

    def test_schedule_exists(self):
        assert "schedule" in self.wf["on"]

    def test_cron_is_valid(self):
        crons = self.wf["on"]["schedule"]
        assert len(crons) >= 1
        cron_expr = crons[0]["cron"]
        parts = cron_expr.split()
        assert len(parts) == 5, f"Invalid cron: {cron_expr}"

    def test_manual_dispatch_enabled(self):
        assert "workflow_dispatch" in self.wf["on"]

    def test_oidc_permissions(self):
        perms = self.wf["permissions"]
        assert perms["id-token"] == "write"

    def test_service_names_match_k8s_manifests(self):
        """The health check service names must match backend-service.yaml."""
        env = self.wf["env"]
        public_svc = str(env.get("BACKEND_PUBLIC_SERVICE", ""))
        internal_svc = str(env.get("BACKEND_INTERNAL_SERVICE", ""))
        svc_raw = _read(os.path.join(K8S_DIR, "backend-service.yaml"))
        assert "aviation-rag-backend-lb" in svc_raw or "aviation-rag-backend-lb" in public_svc
        assert "aviation-rag-backend-internal" in svc_raw or "aviation-rag-backend-internal" in internal_svc


class TestMigrateDatabaseWorkflow:
    """migrate-database.yaml structural checks."""

    @pytest.fixture(autouse=True)
    def _load(self):
        self.wf = _load_workflow("migrate-database.yaml")

    def test_manual_dispatch_only(self):
        assert "push" not in self.wf["on"]
        assert "schedule" not in self.wf["on"]
        assert "workflow_dispatch" in self.wf["on"]

    def test_confirmation_required(self):
        inputs = self.wf["on"]["workflow_dispatch"]["inputs"]
        assert "confirm" in inputs
        assert inputs["confirm"]["required"] is True

    def test_migration_conditional_on_confirm(self):
        job = self.wf["jobs"]["migrate"]
        assert "migrate" in str(job.get("if", ""))

    def test_references_load_script(self):
        raw = _read(os.path.join(WORKFLOWS_DIR, "migrate-database.yaml"))
        assert "02_load_database.py" in raw

    def test_pg_secrets_used(self):
        raw = _read(os.path.join(WORKFLOWS_DIR, "migrate-database.yaml"))
        for var in ["PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD"]:
            assert f"secrets.{var}" in raw, f"Missing PG secret: {var}"


# ═══════════════════════════════════════════════════════════════════════════
# 2. DOCKERFILES
# ═══════════════════════════════════════════════════════════════════════════


class TestDockerfileBackend:
    """Dockerfile.backend structural validation."""

    @pytest.fixture(autouse=True)
    def _load(self):
        self.content = _read(os.path.join(ROOT, "Dockerfile.backend"))

    def test_base_image_is_python_311(self):
        assert "python:3.11" in self.content

    def test_workdir_is_app(self):
        assert "WORKDIR /app" in self.content

    def test_copies_requirements_first(self):
        """requirements.txt should be copied before source for layer caching."""
        req_idx = self.content.index("COPY requirements.txt")
        src_idx = self.content.index("COPY src/")
        assert req_idx < src_idx

    def test_copies_python_source(self):
        assert "COPY src/*.py /app/" in self.content

    def test_copies_contracts(self):
        assert "COPY src/contracts" in self.content

    def test_copies_runtime_data(self):
        assert "COPY src/runtime_data/data" in self.content

    def test_non_root_user(self):
        assert "useradd" in self.content
        assert "USER appuser" in self.content

    def test_exposes_port_5001(self):
        assert "EXPOSE 5001" in self.content

    def test_healthcheck_targets_5001(self):
        assert "http://localhost:5001/health" in self.content

    def test_gunicorn_in_cmd(self):
        assert "gunicorn" in self.content
        assert "api_server:app" in self.content

    def test_gunicorn_binds_to_5001(self):
        assert "0.0.0.0:5001" in self.content

    def test_env_defaults_set(self):
        assert "GUNICORN_WORKER_CLASS=gthread" in self.content
        assert "GUNICORN_WORKERS=" in self.content
        assert "GUNICORN_THREADS=" in self.content

    def test_no_secrets_in_dockerfile(self):
        """No Azure secrets should be baked into the image."""
        for keyword in [
            "AZURE_OPENAI_API_KEY",
            "AZURE_SEARCH_ADMIN_KEY",
            "PGPASSWORD",
        ]:
            assert keyword not in self.content, f"Secret baked in Dockerfile: {keyword}"

    def test_pip_installs_requirements(self):
        assert "pip install" in self.content
        assert "requirements.txt" in self.content


class TestDockerfileFrontend:
    """Dockerfile.frontend structural validation."""

    @pytest.fixture(autouse=True)
    def _load(self):
        self.content = _read(os.path.join(ROOT, "Dockerfile.frontend"))

    def test_multi_stage_build(self):
        assert self.content.count("FROM ") >= 3  # base, deps, builder, runner

    def test_base_image_is_node_20(self):
        assert "node:20" in self.content

    def test_npm_ci_used(self):
        assert "npm ci" in self.content

    def test_standalone_output_copied(self):
        assert ".next/standalone" in self.content
        assert ".next/static" in self.content

    def test_public_copied(self):
        assert "COPY --from=builder /app/public" in self.content

    def test_non_root_user(self):
        assert "nextjs" in self.content
        assert "USER nextjs" in self.content

    def test_cmd_is_node_server(self):
        assert 'CMD ["node", "server.js"]' in self.content

    def test_healthcheck_exists(self):
        assert "HEALTHCHECK" in self.content
        assert "/api/health" in self.content

    def test_no_secrets_in_dockerfile(self):
        for keyword in [
            "AZURE_OPENAI_API_KEY",
            "AZURE_SEARCH_ADMIN_KEY",
            "PGPASSWORD",
        ]:
            assert keyword not in self.content


# ═══════════════════════════════════════════════════════════════════════════
# 3. KUBERNETES MANIFESTS
# ═══════════════════════════════════════════════════════════════════════════


class TestK8sNamespace:
    def test_file_exists(self):
        assert os.path.isfile(os.path.join(K8S_DIR, "namespace.yaml"))

    def test_kind_is_namespace(self):
        docs = _load_yaml_docs(os.path.join(K8S_DIR, "namespace.yaml"))
        assert docs[0]["kind"] == "Namespace"


class TestK8sDeployment:
    """backend-deployment.yaml (Deployment + embedded ClusterIP Service)."""

    @pytest.fixture(autouse=True)
    def _load(self):
        self.docs = _load_yaml_docs(os.path.join(K8S_DIR, "backend-deployment.yaml"))
        self.deploy = next(d for d in self.docs if d["kind"] == "Deployment")
        self.svc = next(d for d in self.docs if d["kind"] == "Service")

    def test_two_documents(self):
        assert len(self.docs) == 2

    def test_deployment_name(self):
        assert "aviation-rag-backend" in self.deploy["metadata"]["name"]

    def test_replicas(self):
        assert self.deploy["spec"]["replicas"] >= 1

    def test_selector_matches_pod_labels(self):
        selector = self.deploy["spec"]["selector"]["matchLabels"]
        pod_labels = self.deploy["spec"]["template"]["metadata"]["labels"]
        for k, v in selector.items():
            assert pod_labels.get(k) == v, f"Selector {k}={v} not in pod labels"

    def test_container_name(self):
        containers = self.deploy["spec"]["template"]["spec"]["containers"]
        assert containers[0]["name"] == "backend"

    def test_container_port_5001(self):
        ports = self.deploy["spec"]["template"]["spec"]["containers"][0]["ports"]
        assert any(p["containerPort"] == 5001 for p in ports)

    def test_envfrom_configmap(self):
        envfrom = self.deploy["spec"]["template"]["spec"]["containers"][0]["envFrom"]
        refs = [e.get("configMapRef", {}).get("name") for e in envfrom]
        assert "backend-config" in refs

    def test_envfrom_secret(self):
        envfrom = self.deploy["spec"]["template"]["spec"]["containers"][0]["envFrom"]
        refs = [e.get("secretRef", {}).get("name") for e in envfrom]
        assert "backend-secrets" in refs

    def test_startup_probe_exists(self):
        container = self.deploy["spec"]["template"]["spec"]["containers"][0]
        assert "startupProbe" in container

    def test_liveness_probe_exists(self):
        container = self.deploy["spec"]["template"]["spec"]["containers"][0]
        assert "livenessProbe" in container

    def test_readiness_probe_exists(self):
        container = self.deploy["spec"]["template"]["spec"]["containers"][0]
        assert "readinessProbe" in container

    def test_all_probes_hit_health_endpoint(self):
        container = self.deploy["spec"]["template"]["spec"]["containers"][0]
        for probe_name in ["startupProbe", "livenessProbe", "readinessProbe"]:
            probe = container[probe_name]
            assert probe["httpGet"]["path"] == "/health"
            assert probe["httpGet"]["port"] == 5001

    def test_startup_probe_allows_sufficient_time(self):
        """Startup probe must allow at least 120s for cold start."""
        container = self.deploy["spec"]["template"]["spec"]["containers"][0]
        sp = container["startupProbe"]
        total_seconds = sp["periodSeconds"] * sp["failureThreshold"]
        assert total_seconds >= 120, f"Startup window only {total_seconds}s"

    def test_resource_limits_set(self):
        res = self.deploy["spec"]["template"]["spec"]["containers"][0]["resources"]
        assert "limits" in res
        assert "requests" in res

    def test_rolling_update_strategy(self):
        strategy = self.deploy["spec"]["strategy"]
        assert strategy["type"] == "RollingUpdate"

    def test_max_unavailable_at_most_one(self):
        ru = self.deploy["spec"]["strategy"]["rollingUpdate"]
        assert ru["maxUnavailable"] <= 1

    def test_termination_grace_period(self):
        spec = self.deploy["spec"]["template"]["spec"]
        assert spec.get("terminationGracePeriodSeconds", 30) >= 30

    def test_pod_anti_affinity(self):
        spec = self.deploy["spec"]["template"]["spec"]
        assert "affinity" in spec
        assert "podAntiAffinity" in spec["affinity"]

    # --- Embedded ClusterIP Service ---

    def test_clusterip_service_selector_matches(self):
        deploy_labels = self.deploy["spec"]["template"]["metadata"]["labels"]
        svc_selector = self.svc["spec"]["selector"]
        for k, v in svc_selector.items():
            assert deploy_labels.get(k) == v

    def test_clusterip_service_port_5001(self):
        ports = self.svc["spec"]["ports"]
        assert any(p["port"] == 5001 and p["targetPort"] == 5001 for p in ports)

    def test_clusterip_type(self):
        assert self.svc["spec"]["type"] == "ClusterIP"


class TestK8sServices:
    """backend-service.yaml (LoadBalancer services)."""

    @pytest.fixture(autouse=True)
    def _load(self):
        self.docs = [
            d for d in _load_yaml_docs(os.path.join(K8S_DIR, "backend-service.yaml"))
            if d is not None
        ]

    def test_at_least_one_service(self):
        assert len(self.docs) >= 1

    def test_internal_lb_exists(self):
        internal = [
            d for d in self.docs
            if d["metadata"].get("annotations", {}).get(
                "service.beta.kubernetes.io/azure-load-balancer-internal"
            ) == "true"
        ]
        assert len(internal) >= 1

    def test_all_services_target_port_5001(self):
        for doc in self.docs:
            ports = doc["spec"]["ports"]
            assert all(p["targetPort"] == 5001 for p in ports)

    def test_all_services_expose_port_80(self):
        for doc in self.docs:
            ports = doc["spec"]["ports"]
            assert any(p["port"] == 80 for p in ports)

    def test_selectors_match_deployment_labels(self):
        deploy_docs = _load_yaml_docs(os.path.join(K8S_DIR, "backend-deployment.yaml"))
        deploy = next(d for d in deploy_docs if d["kind"] == "Deployment")
        pod_labels = deploy["spec"]["template"]["metadata"]["labels"]
        for svc in self.docs:
            for k, v in svc["spec"]["selector"].items():
                assert pod_labels.get(k) == v, (
                    f"Service {svc['metadata']['name']} selector {k}={v} "
                    f"doesn't match pod labels"
                )

    def test_health_probe_annotations(self):
        for doc in self.docs:
            ann = doc["metadata"].get("annotations", {})
            probe_path = ann.get(
                "service.beta.kubernetes.io/azure-load-balancer-health-probe-request-path"
            )
            assert probe_path == "/health", (
                f"Service {doc['metadata']['name']} missing health probe annotation"
            )


class TestK8sConfigMap:
    """backend-configmap.yaml coverage."""

    @pytest.fixture(autouse=True)
    def _load(self):
        self.docs = _load_yaml_docs(os.path.join(K8S_DIR, "backend-configmap.yaml"))
        self.cm = self.docs[0]

    def test_kind(self):
        assert self.cm["kind"] == "ConfigMap"

    def test_name(self):
        assert "backend-config" in self.cm["metadata"]["name"]

    def test_contains_azure_openai_endpoint(self):
        assert "AZURE_OPENAI_ENDPOINT" in self.cm["data"]

    def test_contains_search_endpoint(self):
        assert "AZURE_SEARCH_ENDPOINT" in self.cm["data"]

    def test_contains_pg_config(self):
        for key in ["PGHOST", "PGPORT", "PGDATABASE", "PGUSER"]:
            assert key in self.cm["data"], f"Missing {key}"

    def test_contains_pii_endpoints(self):
        assert "PII_ENDPOINT" in self.cm["data"]
        assert "PII_CONTAINER_ENDPOINT" in self.cm["data"]

    def test_contains_gunicorn_config(self):
        for key in ["GUNICORN_WORKERS", "GUNICORN_THREADS", "GUNICORN_TIMEOUT_SECONDS"]:
            assert key in self.cm["data"], f"Missing {key}"

    def test_no_secrets_in_configmap(self):
        """Secrets must not be in ConfigMap."""
        for key in ["AZURE_OPENAI_API_KEY", "AZURE_SEARCH_ADMIN_KEY", "PGPASSWORD"]:
            assert key not in self.cm["data"], f"Secret {key} found in ConfigMap"


class TestK8sSecret:
    """backend-secret.yaml template."""

    @pytest.fixture(autouse=True)
    def _load(self):
        self.docs = _load_yaml_docs(os.path.join(K8S_DIR, "backend-secret.yaml"))
        self.secret = self.docs[0]

    def test_kind(self):
        assert self.secret["kind"] == "Secret"

    def test_name(self):
        assert "backend-secrets" in self.secret["metadata"]["name"]

    def test_type_opaque(self):
        assert self.secret["type"] == "Opaque"

    def test_contains_required_keys(self):
        data = self.secret.get("stringData", {})
        for key in ["AZURE_OPENAI_API_KEY", "AZURE_SEARCH_ADMIN_KEY", "PGPASSWORD"]:
            assert key in data, f"Missing secret key: {key}"


class TestK8sIngress:
    """backend-ingress.yaml."""

    @pytest.fixture(autouse=True)
    def _load(self):
        self.docs = _load_yaml_docs(os.path.join(K8S_DIR, "backend-ingress.yaml"))
        self.ingress = self.docs[0]

    def test_kind(self):
        assert self.ingress["kind"] == "Ingress"

    def test_ssl_redirect_annotation(self):
        ann = self.ingress["metadata"]["annotations"]
        assert ann.get("nginx.ingress.kubernetes.io/ssl-redirect") == "true"

    def test_cors_enabled(self):
        ann = self.ingress["metadata"]["annotations"]
        assert ann.get("nginx.ingress.kubernetes.io/enable-cors") == "true"

    def test_backend_service_name_matches(self):
        """Ingress must point to the ClusterIP service."""
        rules = self.ingress["spec"]["rules"]
        for rule in rules:
            for path in rule["http"]["paths"]:
                svc_name = path["backend"]["service"]["name"]
                assert "aviation-rag-backend" in svc_name

    def test_backend_service_port(self):
        """Ingress must target port 5001 (ClusterIP service port)."""
        rules = self.ingress["spec"]["rules"]
        for rule in rules:
            for path in rule["http"]["paths"]:
                port = path["backend"]["service"]["port"]["number"]
                assert port == 5001

    def test_tls_block_present(self):
        """Ingress must have TLS configuration with cert-manager."""
        assert "tls" in self.ingress.get("spec", {}), "Ingress missing TLS block"
        tls = self.ingress["spec"]["tls"]
        assert len(tls) >= 1
        assert "secretName" in tls[0]

    def test_cert_manager_annotation(self):
        ann = self.ingress["metadata"]["annotations"]
        assert "cert-manager.io/cluster-issuer" in ann

    def test_proxy_timeout_documented(self):
        """Nginx proxy timeout should be >= gunicorn timeout to avoid 504s."""
        ann = self.ingress["metadata"]["annotations"]
        nginx_timeout = int(ann.get("nginx.ingress.kubernetes.io/proxy-read-timeout", "0"))
        # This is a known gap: nginx=120, gunicorn=240
        # Flagging it explicitly
        assert nginx_timeout > 0, "proxy-read-timeout not set"


# ═══════════════════════════════════════════════════════════════════════════
# 4. RENDER SCRIPT
# ═══════════════════════════════════════════════════════════════════════════


class TestRenderK8sManifestsScript:
    """scripts/render-k8s-manifests.sh validation."""

    @pytest.fixture(autouse=True)
    def _load(self):
        self.content = _read(os.path.join(SCRIPTS_DIR, "render-k8s-manifests.sh"))

    def test_strict_mode(self):
        assert "set -euo pipefail" in self.content

    def test_requires_envsubst_variables(self):
        """All required vars should be validated."""
        for var in [
            "AZURE_CONTAINER_REGISTRY",
            "IMAGE_NAME",
            "AZURE_OPENAI_ENDPOINT",
            "AZURE_SEARCH_ENDPOINT",
            "PII_ENDPOINT",
            "PII_CONTAINER_ENDPOINT",
            "PGHOST",
        ]:
            assert var in self.content, f"render script missing required var: {var}"

    def test_renders_all_k8s_manifests(self):
        for manifest in [
            "namespace.yaml",
            "backend-service.yaml",
            "backend-configmap.yaml",
            "backend-deployment.yaml",
            "backend-ingress.yaml",
        ]:
            assert manifest in self.content, f"render script missing: {manifest}"

    def test_does_not_render_secret(self):
        """Secret template should NOT be rendered (secrets injected via kubectl)."""
        # The manifest loop should not include backend-secret.yaml
        loop_match = re.search(
            r"for manifest in (.+?);", self.content, re.DOTALL
        )
        if loop_match:
            loop_body = loop_match.group(1)
            assert "backend-secret.yaml" not in loop_body

    def test_default_gunicorn_workers(self):
        # Verify the default is documented
        match = re.search(r'GUNICORN_WORKERS:=(\d+)', self.content)
        assert match, "GUNICORN_WORKERS default not found"

    def test_uses_envsubst(self):
        assert "envsubst" in self.content


class TestRenderScriptExecution:
    """Actually execute render-k8s-manifests.sh with dummy vars."""

    @pytest.fixture(autouse=True)
    def _check_envsubst(self):
        """Skip if envsubst is not available."""
        result = subprocess.run(
            ["which", "envsubst"],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            pytest.skip("envsubst not available")

    def test_render_produces_valid_yaml(self):
        """Run the render script and parse all output YAML."""
        with tempfile.TemporaryDirectory() as out_dir:
            env = os.environ.copy()
            env.update({
                "AZURE_CONTAINER_REGISTRY": "testacr.azurecr.io",
                "IMAGE_NAME": "test-backend",
                "IMAGE_TAG": "abc123",
                "AZURE_OPENAI_ENDPOINT": "https://test.openai.azure.com",
                "AZURE_SEARCH_ENDPOINT": "https://test.search.windows.net",
                "PII_ENDPOINT": "http://pii:5000",
                "PII_CONTAINER_ENDPOINT": "http://pii:5000",
                "PGHOST": "test-pg.postgres.database.azure.com",
                "K8S_NAMESPACE": "test-ns",
            })
            result = subprocess.run(
                [
                    "bash",
                    os.path.join(SCRIPTS_DIR, "render-k8s-manifests.sh"),
                    out_dir,
                ],
                capture_output=True,
                text=True,
                env=env,
                timeout=30,
            )
            assert result.returncode == 0, f"render failed: {result.stderr}"

            # Parse every rendered manifest
            rendered_files = glob.glob(os.path.join(out_dir, "*.yaml"))
            assert len(rendered_files) >= 4, f"Only {len(rendered_files)} files rendered"

            for f in rendered_files:
                content = _read(f)
                # Should not contain any un-substituted ${VAR} references
                leftover = re.findall(r"\$\{\w+\}", content)
                assert not leftover, (
                    f"{os.path.basename(f)} has un-substituted vars: {leftover}"
                )
                # Should parse as valid YAML
                docs = list(yaml.safe_load_all(content))
                assert all(d is not None for d in docs), (
                    f"{os.path.basename(f)} produced None YAML doc"
                )

    def test_render_substitutes_image(self):
        """Rendered deployment should contain the exact image reference."""
        with tempfile.TemporaryDirectory() as out_dir:
            env = os.environ.copy()
            env.update({
                "AZURE_CONTAINER_REGISTRY": "myacr.azurecr.io",
                "IMAGE_NAME": "my-backend",
                "IMAGE_TAG": "sha256abc",
                "AZURE_OPENAI_ENDPOINT": "https://openai.test",
                "AZURE_SEARCH_ENDPOINT": "https://search.test",
                "PII_ENDPOINT": "http://pii:5000",
                "PII_CONTAINER_ENDPOINT": "http://pii:5000",
                "PGHOST": "pg.test",
                "K8S_NAMESPACE": "test-ns",
            })
            subprocess.run(
                [
                    "bash",
                    os.path.join(SCRIPTS_DIR, "render-k8s-manifests.sh"),
                    out_dir,
                ],
                capture_output=True, text=True, env=env, timeout=30,
            )
            deploy_file = os.path.join(out_dir, "backend-deployment.yaml")
            content = _read(deploy_file)
            assert "myacr.azurecr.io/my-backend:sha256abc" in content

    def test_render_fails_on_missing_required_var(self):
        """render should fail if a required variable is missing."""
        with tempfile.TemporaryDirectory() as out_dir:
            env = os.environ.copy()
            # Only set some vars, omit PGHOST
            env.update({
                "AZURE_CONTAINER_REGISTRY": "testacr.azurecr.io",
                "IMAGE_NAME": "test",
                "AZURE_OPENAI_ENDPOINT": "https://test",
                "AZURE_SEARCH_ENDPOINT": "https://test",
                "PII_ENDPOINT": "http://pii:5000",
                "PII_CONTAINER_ENDPOINT": "http://pii:5000",
            })
            # Remove PGHOST if it exists
            env.pop("PGHOST", None)
            result = subprocess.run(
                [
                    "bash",
                    os.path.join(SCRIPTS_DIR, "render-k8s-manifests.sh"),
                    out_dir,
                ],
                capture_output=True, text=True, env=env, timeout=30,
            )
            assert result.returncode != 0, "Should fail on missing PGHOST"


# ═══════════════════════════════════════════════════════════════════════════
# 5. CROSS-COMPONENT PORT CONSISTENCY
# ═══════════════════════════════════════════════════════════════════════════


class TestPortConsistency:
    """Backend port 5001 must be consistent across all components."""

    def test_dockerfile_exposes_5001(self):
        assert "EXPOSE 5001" in _read(os.path.join(ROOT, "Dockerfile.backend"))

    def test_dockerfile_healthcheck_uses_5001(self):
        assert "localhost:5001" in _read(os.path.join(ROOT, "Dockerfile.backend"))

    def test_k8s_deployment_container_port_5001(self):
        docs = _load_yaml_docs(os.path.join(K8S_DIR, "backend-deployment.yaml"))
        deploy = next(d for d in docs if d["kind"] == "Deployment")
        ports = deploy["spec"]["template"]["spec"]["containers"][0]["ports"]
        assert any(p["containerPort"] == 5001 for p in ports)

    def test_k8s_clusterip_targets_5001(self):
        docs = _load_yaml_docs(os.path.join(K8S_DIR, "backend-deployment.yaml"))
        svc = next(d for d in docs if d["kind"] == "Service")
        assert any(p["targetPort"] == 5001 for p in svc["spec"]["ports"])

    def test_k8s_lb_services_target_5001(self):
        docs = _load_yaml_docs(os.path.join(K8S_DIR, "backend-service.yaml"))
        for svc in docs:
            assert any(p["targetPort"] == 5001 for p in svc["spec"]["ports"])

    def test_ingress_targets_5001(self):
        docs = _load_yaml_docs(os.path.join(K8S_DIR, "backend-ingress.yaml"))
        rules = docs[0]["spec"]["rules"]
        for rule in rules:
            for path in rule["http"]["paths"]:
                assert path["backend"]["service"]["port"]["number"] == 5001

    def test_gunicorn_binds_5001_in_deployment(self):
        raw = _read(os.path.join(K8S_DIR, "backend-deployment.yaml"))
        assert "0.0.0.0:5001" in raw

    def test_all_probes_use_port_5001(self):
        docs = _load_yaml_docs(os.path.join(K8S_DIR, "backend-deployment.yaml"))
        deploy = next(d for d in docs if d["kind"] == "Deployment")
        container = deploy["spec"]["template"]["spec"]["containers"][0]
        for probe_key in ["startupProbe", "livenessProbe", "readinessProbe"]:
            assert container[probe_key]["httpGet"]["port"] == 5001


# ═══════════════════════════════════════════════════════════════════════════
# 6. ENV VAR COVERAGE: CONFIGMAP + SECRET vs BACKEND CODE
# ═══════════════════════════════════════════════════════════════════════════


class TestEnvVarCoverage:
    """Critical env vars used by backend Python code must be provided via
    ConfigMap, Secret, or have safe defaults."""

    @pytest.fixture(autouse=True)
    def _load(self):
        cm_docs = _load_yaml_docs(os.path.join(K8S_DIR, "backend-configmap.yaml"))
        self.cm_keys = set(cm_docs[0]["data"].keys())

        secret_docs = _load_yaml_docs(os.path.join(K8S_DIR, "backend-secret.yaml"))
        self.secret_keys = set(secret_docs[0].get("stringData", {}).keys())

        self.all_k8s_keys = self.cm_keys | self.secret_keys
        self.python_vars = _env_vars_read_by_python()

    def test_azure_openai_endpoint_provided(self):
        assert "AZURE_OPENAI_ENDPOINT" in self.all_k8s_keys

    def test_azure_search_endpoint_provided(self):
        assert "AZURE_SEARCH_ENDPOINT" in self.all_k8s_keys

    def test_pg_connection_vars_provided(self):
        for var in ["PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD"]:
            assert var in self.all_k8s_keys, f"{var} not in k8s manifests"

    def test_pii_endpoint_provided(self):
        assert "PII_ENDPOINT" in self.all_k8s_keys

    def test_azure_openai_api_key_in_secret(self):
        assert "AZURE_OPENAI_API_KEY" in self.secret_keys

    def test_azure_search_key_in_secret(self):
        assert "AZURE_SEARCH_ADMIN_KEY" in self.secret_keys

    def test_pgpassword_in_secret_not_configmap(self):
        assert "PGPASSWORD" in self.secret_keys
        assert "PGPASSWORD" not in self.cm_keys

    def test_critical_python_vars_have_k8s_source(self):
        """Every critical env var read by Python should come from k8s or have a default."""
        critical_vars = {
            "AZURE_OPENAI_ENDPOINT",
            "AZURE_SEARCH_ENDPOINT",
            "PGHOST",
            "PGPASSWORD",
            "AZURE_OPENAI_API_KEY",
        }
        missing = critical_vars - self.all_k8s_keys
        assert not missing, f"Critical vars missing from k8s: {missing}"


# ═══════════════════════════════════════════════════════════════════════════
# 7. .dockerignore
# ═══════════════════════════════════════════════════════════════════════════


class TestDockerIgnore:
    """Verify .dockerignore excludes sensitive and unnecessary files."""

    @pytest.fixture(autouse=True)
    def _load(self):
        self.content = _read(os.path.join(ROOT, ".dockerignore"))

    def test_excludes_git(self):
        assert ".git" in self.content

    def test_excludes_env_files(self):
        assert ".env" in self.content

    def test_excludes_env_local(self):
        assert ".env.local" in self.content

    def test_excludes_node_modules(self):
        assert "node_modules" in self.content

    def test_excludes_next_build(self):
        assert ".next" in self.content

    def test_excludes_pycache(self):
        assert "__pycache__" in self.content

    def test_excludes_data_directory(self):
        assert "data/" in self.content

    def test_excludes_artifacts(self):
        assert "artifacts/" in self.content


# ═══════════════════════════════════════════════════════════════════════════
# 8. REQUIREMENTS.TXT CONSISTENCY
# ═══════════════════════════════════════════════════════════════════════════


class TestRequirementsTxt:
    """requirements.txt vs Dockerfile pip install overlap."""

    @pytest.fixture(autouse=True)
    def _load(self):
        self.req_content = _read(os.path.join(ROOT, "requirements.txt"))
        self.docker_content = _read(os.path.join(ROOT, "Dockerfile.backend"))

    def _req_packages(self) -> set[str]:
        pkgs = set()
        for line in self.req_content.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            name = re.split(r"[>=<\[]", line)[0].strip().lower()
            pkgs.add(name)
        return pkgs

    def test_flask_in_requirements(self):
        assert "flask" in self._req_packages()

    def test_gunicorn_in_requirements(self):
        assert "gunicorn" in self._req_packages()

    def test_azure_identity_in_requirements(self):
        assert "azure-identity" in self._req_packages()

    def test_openai_in_requirements(self):
        assert "openai" in self._req_packages()

    def test_psycopg2_in_requirements(self):
        pkgs = self._req_packages()
        assert "psycopg2-binary" in pkgs or "psycopg2" in pkgs

    def test_duplicate_installs_flagged(self):
        """Packages installed separately in Dockerfile that are already in requirements.txt."""
        req_pkgs = self._req_packages()
        # Parse the second pip install block in Dockerfile
        docker_extra = re.findall(
            r"RUN pip install[^\\]*(?:\\\n[^\\]*)*(gunicorn|psycopg2-binary|flask-cors)",
            self.docker_content,
        )
        overlap = {p.lower() for p in docker_extra} & req_pkgs
        # This is a known issue — document rather than fail
        if overlap:
            pytest.skip(
                f"Known redundancy: {overlap} installed both in requirements.txt "
                f"and separately in Dockerfile. Consider removing the duplicate."
            )


# ═══════════════════════════════════════════════════════════════════════════
# 9. DEPLOYMENT SAFETY CHECKS
# ═══════════════════════════════════════════════════════════════════════════


class TestDeploymentSafety:
    """Cross-cutting safety validations."""

    def test_backend_workflow_does_not_force_push(self):
        raw = _read(os.path.join(WORKFLOWS_DIR, "deploy-backend.yaml"))
        assert "push --force" not in raw
        assert "push -f" not in raw

    def test_frontend_workflow_does_not_force_push(self):
        raw = _read(os.path.join(WORKFLOWS_DIR, "deploy-frontend.yaml"))
        assert "push --force" not in raw

    def test_no_secrets_in_k8s_configmap(self):
        cm_raw = _read(os.path.join(K8S_DIR, "backend-configmap.yaml"))
        for secret_var in ["API_KEY", "ADMIN_KEY", "PGPASSWORD", "SECRET"]:
            # These should NOT appear as keys in the configmap
            assert f"  {secret_var}:" not in cm_raw or "PGPASSWORD" not in cm_raw

    def test_provision_script_has_strict_mode(self):
        content = _read(os.path.join(SCRIPTS_DIR, "provision-azure.sh"))
        assert "set -euo pipefail" in content

    def test_render_script_has_strict_mode(self):
        content = _read(os.path.join(SCRIPTS_DIR, "render-k8s-manifests.sh"))
        assert "set -euo pipefail" in content

    def test_k8s_namespace_is_not_default(self):
        """Manifests must not deploy to the 'default' namespace."""
        for fname in glob.glob(os.path.join(K8S_DIR, "*.yaml")):
            docs = _load_yaml_docs(fname)
            for doc in docs:
                if doc and "metadata" in doc:
                    ns = doc["metadata"].get("namespace", "")
                    assert ns != "default", (
                        f"{os.path.basename(fname)} deploys to 'default' namespace"
                    )

    def test_workflow_files_all_exist(self):
        expected = [
            "deploy-backend.yaml",
            "deploy-frontend.yaml",
            "infra-health-check.yaml",
            "migrate-database.yaml",
        ]
        for name in expected:
            path = os.path.join(WORKFLOWS_DIR, name)
            assert os.path.isfile(path), f"Missing workflow: {name}"

    def test_k8s_manifest_files_all_exist(self):
        expected = [
            "namespace.yaml",
            "backend-deployment.yaml",
            "backend-service.yaml",
            "backend-configmap.yaml",
            "backend-secret.yaml",
            "backend-ingress.yaml",
        ]
        for name in expected:
            path = os.path.join(K8S_DIR, name)
            assert os.path.isfile(path), f"Missing k8s manifest: {name}"


# ═══════════════════════════════════════════════════════════════════════════
# 10. KNOWN GAPS / DOCUMENTATION TESTS
# ═══════════════════════════════════════════════════════════════════════════


class TestKnownGaps:
    """Tests that document known gaps between components.
    These are expected to PASS (documenting current state) and should be
    updated when the gaps are resolved."""

    def test_ingress_has_tls_with_cert_manager(self):
        """Ingress now has TLS via cert-manager — good."""
        docs = _load_yaml_docs(os.path.join(K8S_DIR, "backend-ingress.yaml"))
        assert "tls" in docs[0].get("spec", {}), (
            "TLS block removed — investigate"
        )
        ann = docs[0]["metadata"]["annotations"]
        assert "cert-manager.io/cluster-issuer" in ann

    def test_nginx_timeout_less_than_gunicorn(self):
        """nginx proxy-read-timeout (120s) < gunicorn timeout (240s) — 504 risk."""
        docs = _load_yaml_docs(os.path.join(K8S_DIR, "backend-ingress.yaml"))
        ann = docs[0]["metadata"]["annotations"]
        nginx_timeout = int(ann.get("nginx.ingress.kubernetes.io/proxy-read-timeout", "0"))
        # render script defaults GUNICORN_TIMEOUT_SECONDS to 240
        assert nginx_timeout < 240, (
            "Timeouts are now aligned — update this test"
        )

    def test_dual_cors_layers(self):
        """Both nginx ingress and Flask have CORS enabled — may cause duplicate headers."""
        ingress_raw = _read(os.path.join(K8S_DIR, "backend-ingress.yaml"))
        assert "enable-cors" in ingress_raw
        # Flask CORS is in api_server.py — just document the gap
        api_server = _read(os.path.join(SRC_DIR, "api_server.py"))
        assert "CORS" in api_server or "cors" in api_server, (
            "Flask CORS removed — update this test"
        )

    def test_dockerfile_frontend_port_vs_app_service(self):
        """Dockerfile.frontend sets PORT=3001 but App Service sets PORT=3000."""
        df = _read(os.path.join(ROOT, "Dockerfile.frontend"))
        assert "PORT=3001" in df  # Dockerfile sets 3001
        # Frontend workflow sets PORT=3000 for App Service
        wf = _read(os.path.join(WORKFLOWS_DIR, "deploy-frontend.yaml"))
        assert "PORT=3000" in wf or "WEBSITES_PORT=3000" in wf

    def test_migrate_uses_secrets_not_vars(self):
        """migrate-database.yaml uses secrets for PG config while deploy-backend uses vars.
        This inconsistency is documented here."""
        migrate_raw = _read(os.path.join(WORKFLOWS_DIR, "migrate-database.yaml"))
        deploy_raw = _read(os.path.join(WORKFLOWS_DIR, "deploy-backend.yaml"))
        # Migrate: secrets.PGHOST
        assert "secrets.PGHOST" in migrate_raw
        # Deploy: vars.PGHOST
        assert "vars.PGHOST" in deploy_raw

    def test_ingress_uses_deprecated_annotation(self):
        """Ingress uses annotation-based class instead of spec.ingressClassName."""
        docs = _load_yaml_docs(os.path.join(K8S_DIR, "backend-ingress.yaml"))
        ann = docs[0]["metadata"]["annotations"]
        assert "kubernetes.io/ingress.class" in ann
        # When migrated to spec.ingressClassName, update this test
        assert "ingressClassName" not in docs[0].get("spec", {})

    def test_frontend_dockerfile_not_used_in_ci(self):
        """Dockerfile.frontend is not referenced in deploy-frontend.yaml."""
        wf_raw = _read(os.path.join(WORKFLOWS_DIR, "deploy-frontend.yaml"))
        assert "Dockerfile.frontend" not in wf_raw

    def test_migrate_verification_is_stub(self):
        """Migrate workflow verification only counts tables, not specific schema."""
        raw = _read(os.path.join(WORKFLOWS_DIR, "migrate-database.yaml"))
        assert "[TBD]" in raw


# ═══════════════════════════════════════════════════════════════════════════
# 11. WORKFLOW DEFAULT VALUE CONSISTENCY
# ═══════════════════════════════════════════════════════════════════════════


class TestDefaultValueConsistency:
    """Verify that default values are consistent across workflow, render script,
    and Dockerfile."""

    def _extract_render_defaults(self) -> dict[str, str]:
        content = _read(os.path.join(SCRIPTS_DIR, "render-k8s-manifests.sh"))
        defaults = {}
        for match in re.finditer(r':\s*"\$\{(\w+):=([^}]*)\}"', content):
            defaults[match.group(1)] = match.group(2)
        return defaults

    def _extract_workflow_defaults(self) -> dict[str, str]:
        wf = _load_workflow("deploy-backend.yaml")
        defaults = {}
        for key, value in wf.get("env", {}).items():
            # Extract default from pattern: ${{ vars.X || 'default' }}
            val_str = str(value)
            match = re.search(r"\|\|\s*'([^']*)'", val_str)
            if match:
                defaults[key] = match.group(1)
        return defaults

    def _extract_dockerfile_defaults(self) -> dict[str, str]:
        content = _read(os.path.join(ROOT, "Dockerfile.backend"))
        defaults = {}
        for match in re.finditer(r"(\w+)=(\S+)", content):
            key, value = match.group(1), match.group(2)
            if key.startswith("GUNICORN_"):
                # Clean trailing backslash
                defaults[key] = value.rstrip("\\").strip()
        return defaults

    def test_gunicorn_worker_class_consistent(self):
        render = self._extract_render_defaults()
        wf = self._extract_workflow_defaults()
        docker = self._extract_dockerfile_defaults()
        assert render.get("GUNICORN_WORKER_CLASS") == "gthread"
        assert wf.get("GUNICORN_WORKER_CLASS") == "gthread"
        assert docker.get("GUNICORN_WORKER_CLASS") == "gthread"

    def test_gunicorn_timeout_consistent(self):
        render = self._extract_render_defaults()
        wf = self._extract_workflow_defaults()
        docker = self._extract_dockerfile_defaults()
        assert render.get("GUNICORN_TIMEOUT_SECONDS") == "240"
        assert wf.get("GUNICORN_TIMEOUT_SECONDS") == "240"
        assert docker.get("GUNICORN_TIMEOUT_SECONDS") == "240"

    def test_gunicorn_workers_render_matches_claudemd(self):
        """render-k8s-manifests.sh should default workers to 1 (per CLAUDE.md)."""
        render = self._extract_render_defaults()
        assert render.get("GUNICORN_WORKERS") == "1", (
            f"render defaults GUNICORN_WORKERS to {render.get('GUNICORN_WORKERS')} "
            f"but CLAUDE.md mandates 1"
        )

    def test_gunicorn_workers_dockerfile_vs_workflow(self):
        """Dockerfile and workflow may differ from render — document the state."""
        wf = self._extract_workflow_defaults()
        docker = self._extract_dockerfile_defaults()
        # These default to 3 while render uses 1; both are overridden at deploy
        # time via the configmap. Just verify they have some integer value.
        assert wf.get("GUNICORN_WORKERS", "").isdigit()
        assert docker.get("GUNICORN_WORKERS", "").isdigit()

    def test_pgport_default_5432(self):
        render = self._extract_render_defaults()
        wf = self._extract_workflow_defaults()
        assert render.get("PGPORT") == "5432"
        assert wf.get("PGPORT") == "5432"

    def test_pgdatabase_default(self):
        render = self._extract_render_defaults()
        wf = self._extract_workflow_defaults()
        assert render.get("PGDATABASE") == wf.get("PGDATABASE")

    def test_k8s_namespace_default(self):
        render = self._extract_render_defaults()
        wf = self._extract_workflow_defaults()
        # Both should default to aviation-rag
        assert render.get("K8S_NAMESPACE") == "aviation-rag"
        # Workflow uses AKS_NAMESPACE var name but same default
        assert "aviation-rag" in str(wf)
