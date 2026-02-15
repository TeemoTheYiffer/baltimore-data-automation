# Infrastructure as Code - GitLab CI/CD

Automated build and deployment pipeline using GitLab CI/CD and Google Cloud Run.

## Pipeline Overview

```
Push to main ──→ build ──→ deploy-production (maryland-automation-app)
Open MR     ──→ build ──→ deploy-staging    (maryland-automation-app-staging)
```

- **Production**: Auto-deploys on every push to `main`. Full specs (8 CPU, 16GB, min 1 instance).
- **Staging**: Deploys on merge requests for pre-merge testing. Lower specs (4 CPU, 8GB, min 0 instances) to save cost. Separate Cloud Run URL.

## One-Time Setup

### 1. Create a GCP Service Account

The CI/CD pipeline needs a service account with permissions to build images and deploy to Cloud Run.

```bash
# Create the service account
gcloud iam service-accounts create gitlab-ci-deployer \
  --display-name="GitLab CI/CD Deployer" \
  --project=parabolic-clock-457218-f5

# Grant required roles
SA_EMAIL="gitlab-ci-deployer@parabolic-clock-457218-f5.iam.gserviceaccount.com"

gcloud projects add-iam-policy-binding parabolic-clock-457218-f5 \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/cloudbuild.builds.editor"

gcloud projects add-iam-policy-binding parabolic-clock-457218-f5 \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/run.admin"

gcloud projects add-iam-policy-binding parabolic-clock-457218-f5 \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/storage.admin"

gcloud projects add-iam-policy-binding parabolic-clock-457218-f5 \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/iam.serviceAccountUser"

# Generate the key
gcloud iam service-accounts keys create sa-key.json \
  --iam-account="${SA_EMAIL}"
```

### 2. Add the Key to GitLab

1. Go to your GitLab project → **Settings** → **CI/CD** → **Variables**
2. Add a new variable:
   - **Key**: `GCP_SA_KEY`
   - **Value**: Paste the entire contents of `sa-key.json`
   - **Type**: Variable
   - **Flags**: Check **Mask variable** and **Protect variable**
3. Delete the local `sa-key.json` file after adding it to GitLab

### 3. Verify

Push a commit or open an MR. Check the pipeline at **CI/CD** → **Pipelines** in GitLab.

## Testing Before Merge

1. Create a branch and push your changes
2. Open a Merge Request in GitLab
3. The pipeline builds and deploys to `maryland-automation-app-staging`
4. The staging URL is printed in the deploy job output
5. Test against the staging URL in your browser
6. Once verified, merge the MR → production auto-deploys

## Updating gcloud SDK Version

The pipeline uses the `google/cloud-sdk:slim` Docker image. To pin or update the version, change the image tag in `.gitlab-ci.yml`:

```yaml
# Pin to a specific version
image: google/cloud-sdk:503.0.0-slim

# Or use latest (current default)
image: google/cloud-sdk:slim
```

No `gcloud components update` needed — the image comes pre-configured.

## Pipeline Configuration

| Setting | Production | Staging |
|---------|-----------|---------|
| Service Name | `maryland-automation-app` | `maryland-automation-app-staging` |
| CPU | 8 | 4 |
| Memory | 16Gi | 8Gi |
| Min Instances | 1 | 0 (scales to zero) |
| Max Instances | 2 | 1 |
| Trigger | Push to `main` | Merge request |

## Cost Impact

- **Pipeline runs**: GitLab free tier includes 400 CI/CD minutes/month. A build+deploy takes ~3-5 minutes.
- **Staging service**: Uses `min-instances: 0`, so it only costs money when actively serving requests during testing. Scales to zero when idle.
- **Image storage**: GCR charges ~$0.026/GB/month. Each image is tagged with the commit SHA.

## Cleanup

To remove old container images from GCR:

```bash
# List images with digests
gcloud container images list-tags gcr.io/parabolic-clock-457218-f5/maryland-automation-app

# Delete a specific tag
gcloud container images delete gcr.io/parabolic-clock-457218-f5/maryland-automation-app:TAG --quiet
```

To delete the staging service when not needed:

```bash
gcloud run services delete maryland-automation-app-staging --region=us-east1
```
