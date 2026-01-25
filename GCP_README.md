# GCP Deployment Guide - Maryland Property Automation

This guide covers building and deploying the Maryland Property Automation application to Google Cloud Platform using Cloud Build and Cloud Run.

## Prerequisites

- Google Cloud SDK installed and configured
- Docker installed locally (for local testing)
- GCP project with billing enabled
- Required APIs enabled:
  - Cloud Build API
  - Cloud Run API
  - Container Registry API

## Deployment Commands

### 1. Build and Push Docker Image

```bash
gcloud builds submit --tag "gcr.io/parabolic-clock-457218-f5/maryland-automation-app"
```

### 2. Deploy to Cloud Run

```bash
gcloud run deploy "maryland-automation-app" \
  --image "gcr.io/parabolic-clock-457218-f5/maryland-automation-app" \
  --platform managed \
  --region "us-east1" \
  --allow-unauthenticated \
  --timeout "1800" \
  --max-instances "2" \
  --min-instances "1" \
  --cpu "8" \
  --memory "16Gi" \
  --concurrency "10" \
  --cpu-boost \
  --execution-environment gen2
```

## Command Flag Explanations

### Cloud Build Flags

| Flag | Value | Description |
|------|-------|-------------|
| `--tag` | `gcr.io/parabolic-clock-457218-f5/maryland-automation-app` | Container image name and tag in Google Container Registry |

### Cloud Run Deployment Flags

| Flag | Value | Description |
|------|-------|-------------|
| `--image` | `gcr.io/parabolic-clock-457218-f5/maryland-automation-app` | Container image to deploy from Container Registry |
| `--platform` | `managed` | Use fully managed Cloud Run (vs. Cloud Run for Anthos) |
| `--region` | `us-east1` | GCP region for deployment (East Coast US) |
| `--allow-unauthenticated` | - | Allow public access without authentication |
| `--timeout` | `1800` | Maximum request timeout (30 minutes) for long-running batch jobs |
| `--max-instances` | `2` | Maximum number of container instances for auto-scaling |
| `--min-instances` | `1` | Minimum number of container instances (always warm) |
| `--cpu` | `8` | Number of CPU cores per instance (high for processing) |
| `--memory` | `16Gi` | Memory allocation per instance (16 GB for large datasets) |
| `--concurrency` | `10` | Maximum concurrent requests per instance |
| `--cpu-boost` | - | Allocate full CPU during startup for faster cold starts |
| `--execution-environment` | `gen2` | Use second-generation execution environment (better performance) |

## Configuration Rationale

### High Resource Allocation
- **8 CPUs + 16GB RAM**: Handles intensive property data processing and Google Sheets API operations
- **30-minute timeout**: Accommodates large batch processing jobs (162 rows)
- **Min 1 instance**: Always-warm instance for immediate response, reduced baseline cost

### Scaling Configuration
- **Max 2 instances**: Optimized for 2 concurrent users, prevents excessive costs
- **Concurrency 10**: Each instance handles 10 concurrent requests (20 total capacity)
- **CPU boost**: Faster response times for interactive API calls

### Networking & Security
- **Public access**: Allows direct API usage without authentication setup
- **us-east1 region**: Good latency for East Coast users and Google APIs

## Environment Variables

The application uses these environment variables (set in Cloud Run):

```bash
# Google Cloud credentials are automatically provided by Cloud Run
GOOGLE_APPLICATION_CREDENTIALS=/app/app_secrets/parabolic-clock-457218-f5.json

# Application settings
PORT=8080
LOG_LEVEL=INFO
```

## Local Development

### Build Locally
```bash
docker build -t maryland-automation-app .
```

### Run Locally
```bash
docker run -p 8080:8080 \
  -v /path/to/credentials:/app/app_secrets \
  maryland-automation-app
```

## Monitoring and Logs

### View Logs
```bash
gcloud run services logs tail maryland-automation-app --region=us-east1
```

### Monitor Performance
- Cloud Run service metrics in GCP Console
- Custom dashboards for request latency and error rates
- Alerting on high memory usage or timeout errors

## Cost Optimization

### Current Configuration Costs
- **High-spec instances**: ~$0.50-1.00 per hour when active
- **Always-on instances**: 1 minimum instance = ~$12-24/month baseline
- **Request pricing**: ~$0.40 per million requests
- **Total estimated**: ~$15-30/month for typical usage

### Optimization Options
1. **Remove always-on**: Set min-instances to 0, save ~$12-24/month, accept cold starts
2. **Lower CPU/memory**: Use 4 CPU/8GB for moderate workloads, save ~$6-12/month
3. **Reduce concurrency**: Lower to 5 if single-user batches are large

## Troubleshooting

### Common Issues

1. **Build Failures**
   ```bash
   # Check build logs
   gcloud builds log [BUILD_ID]
   ```

2. **Debconf Warnings During Build** (Fixed)
   ```
   debconf: unable to initialize frontend: Dialog
   debconf: falling back to frontend: Noninteractive
   ```
   **Solution**: Dockerfile includes `DEBIAN_FRONTEND=noninteractive` to suppress these harmless warnings.

3. **Deployment Errors**
   ```bash
   # Check service status
   gcloud run services describe maryland-automation-app --region=us-east1
   ```

4. **Memory Issues**
   - Increase `--memory` if jobs fail with OOM errors
   - Monitor memory usage in Cloud Run metrics

5. **Timeout Issues**
   - Increase `--timeout` for very large batch jobs
   - Consider breaking large jobs into smaller chunks

## Security Considerations

### Current Setup
- ✅ Service account authentication for Google APIs
- ✅ HTTPS encryption for all traffic
- ⚠️ Public access enabled (no authentication required)

### Production Recommendations
1. **Enable authentication**: Remove `--allow-unauthenticated` for production
2. **VPC integration**: Deploy to private VPC for internal access only
3. **IAM policies**: Restrict service account permissions to minimum required
4. **API keys**: Implement API key authentication for public endpoints

## Updating the Service

### Quick Update (same image)
```bash
gcloud run services update maryland-automation-app --region=us-east1
```

### Full Rebuild and Deploy
```bash
# Build new image
gcloud builds submit --tag "gcr.io/parabolic-clock-457218-f5/maryland-automation-app"

# Deploy updated image
gcloud run deploy "maryland-automation-app" \
  --image "gcr.io/parabolic-clock-457218-f5/maryland-automation-app" \
  --region "us-east1"
```

## API Endpoints

Once deployed, the service provides these endpoints:

- **Health Check**: `GET /health`
- **Property Data**: `POST /property`
- **Water Bill Data**: `POST /waterbill`
- **Batch Processing**: `POST /batch`
- **Job Status**: `GET /batch/{job_id}`
- **API Documentation**: `GET /` (FastAPI auto-docs)

## Support

For deployment issues:
- Check Cloud Run logs for application errors
- Verify service account permissions for Google Sheets/Drive APIs
- Monitor Cloud Build history for build failures
- Review resource quotas and billing limits