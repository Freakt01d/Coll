name: azure-dart-parquet-api-poc-deploy

on:
workflow_dispatch:
inputs:
version:
description: “Deploy Version”
required: true
default: “uat_25.mm.dd.1”

env:
HTTP_PROXY: “http://proxyapp.arpege.socgen:11000”
HTTPS_PROXY: “http://proxyapp.arpege.socgen:11000”
NO_PROXY: “localhost,sgithub.fr.world.socgen,dsp-artifacts.fr.world.socgen,*.world.socgen”
SSL_CERT_FILE: /home/runner/certs/cacerts.crt

jobs:
deploy:
runs-on: [ self-hosted, linux ]

```
steps:
  - name: Setup Kubectl
    uses: SGitHubActions/setup-tools@stable
    with:
      tool-kind: 'kubectl'
      distribution: 'kubectl'
      tool-version: '1.29.5'

  - name: Setup Kubelogin
    uses: SGitHubActions/setup-tools@stable
    with:
      tool-kind: "kubelogin"
      distribution: "kubelogin"
      tool-version: "0.1.7"

  - name: Azure login and config setup
    shell: bash
    run: |
      set -euo pipefail
      az login --service-principal \
        -u "${{ secrets.AZURE_CLIENT_ID_DEV }}" \
        -p "${{ secrets.AZURE_CLIENT_SECRET_DEV }}" \
        -t "${{ secrets.AZURE_TENANT_ID }}"
      az account set -s "${{ secrets.AZURE_SUBSCRIPTION_ID_DEV }}"
      az aks get-credentials \
        --name "gbto-red-dev-we-${{ vars.ACTIVE_AKS_TYPE_DEV }}-aks" \
        --resource-group "gbto-red-dev-we-${{ vars.ACTIVE_AKS_TYPE_DEV }}-aks" \
        --overwrite-existing \
        --subscription "gbis-gbsu-reg-rgi-red-1-dev"
      kubelogin convert-kubeconfig -l azurecli
      # Rewrite host and inject corporate CA bundle into kubeconfig
      sed -i 's/hcp/portal.hcp/g' /home/runner/.kube/config
      yq -e ".clusters[0].cluster.certificate-authority-data = \"$(cat $HOME/certs/cacerts.crt | base64 -w 0)\"" -i /home/runner/.kube/config
      export KUBECONFIG=/home/runner/.kube/config
      echo "KUBECONFIG=/home/runner/.kube/config" >> $GITHUB_ENV

  - name: Verify access
    run: kubectl get ns

  - name: Checkout manifests repo (dart-parquet-api-poc)
    uses: actions/checkout@v3
    with:
      repository: RED/dart-parquet-api-poc   # <- Repo that contains k8s/deployment.yaml
      ref: main                               # <- Adjust if your manifest is on another branch
      token: ${{ secrets.GIT_TOKEN }}

  - name: Template image tag into manifest
    shell: bash
    run: |
      set -euo pipefail
      MANIFEST_PATH="k8s/deployment.yaml"
      OUT_PATH="Kube/app-deployment.yaml"

      echo "=== Repo root ==="
      ls -al || true
      echo "=== k8s folder ==="
      ls -al k8s || true
      echo "==================="

      if [ ! -f "$MANIFEST_PATH" ]; then
        echo "Manifest not found at $MANIFEST_PATH"
        exit 1
      fi

      mkdir -p Kube
      sed "s/{{IMAGE_TAG}}/${{ github.event.inputs.version }}/g" "$MANIFEST_PATH" > "$OUT_PATH"

      echo "Templated manifest (first 40 lines):"
      head -n 40 "$OUT_PATH" || true

  - name: Deploy to AKS
    shell: bash
    env:
      KUBECONFIG: /home/runner/.kube/config
    run: |
      set -euo pipefail
      kubectl apply -f Kube/app-deployment.yaml -n red-dev --validate=false
      echo "Waiting for rollout..."
      kubectl rollout status deploy/parquet-audit-api -n red-dev --timeout=6m

  # Optional: better diagnostics if rollout fails
  - name: Show troubleshooting info on failure
    if: ${{ failure() }}
    shell: bash
    env:
      KUBECONFIG: /home/runner/.kube/config
    run: |
      echo "==== DESCRIBE DEPLOYMENT ===="
      kubectl describe deploy/parquet-audit-api -n red-dev || true
      echo "==== PODS ===="
      kubectl get pods -n red-dev -o wide || true
      echo "==== EVENTS ===="
      kubectl get events -n red-dev --sort-by=.lastTimestamp | tail -n 50 || true
```