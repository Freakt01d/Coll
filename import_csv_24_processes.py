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
    run: |
      az login --service-principal -u ${{ secrets.AZURE_CLIENT_ID_DEV }} -p ${{ secrets.AZURE_CLIENT_SECRET_DEV }} -t ${{ secrets.AZURE_TENANT_ID }}
      az account set -s ${{ secrets.AZURE_SUBSCRIPTION_ID_DEV }}
      az aks get-credentials --name gbto-red-dev-we-${{ vars.ACTIVE_AKS_TYPE_DEV }}-aks --resource-group gbto-red-dev-we-${{ vars.ACTIVE_AKS_TYPE_DEV }}-aks --overwrite-existing --subscription gbis-gbsu-reg-rgi-red-1-dev
      kubelogin convert-kubeconfig -l azurecli
      sed -i 's/hcp/portal.hcp/g' /home/runner/.kube/config
      yq -e ".clusters[0].cluster.certificate-authority-data = \"$(cat $HOME/certs/cacerts.crt | base64 -w 0)\"" -i /home/runner/.kube/config
      cat /home/runner/.kube/config
      export KUBECONFIG=/home/runner/.kube/config

  - name: Running kubectl
    run: kubectl get ns

  - name: Checkout Repo
    uses: actions/checkout@v3
    with:
      repository: RED/dart-parquet-api-poc
      ref: main
      token: ${{ secrets.GIT_TOKEN }}

  - name: Setup Deployment Yml Image version
    run: |
      mkdir -p Kube
      echo "Version to deploy: ${{ github.event.inputs.version }}"
      cat k8s/deployment.yaml | sed 's/{{IMAGE_TAG}}/${{ github.event.inputs.version }}/g' > Kube/app-deployment.yml
      ls -al
      ls -al Kube
      pwd

  - name: Setup Kube and Deploy
    run: |
      echo "KUBECONFIG=/home/runner/.kube/config" >> $GITHUB_ENV
      echo "----------------------------"
      export KUBECONFIG=/home/runner/.kube/config
      echo "----------------------------"
      ls -al
      pwd
      kubectl get pods -n red-dev
      echo "----------------------------"
      echo "deploying"
      kubectl apply -f Kube/app-deployment.yml -n red-dev --validate=false
```