# Brev Dashboards

Interactive Marimo dashboards for the Brev Data Platform.

## Available Dashboards

| Dashboard | Description | Run Command |
|-----------|-------------|-------------|
| [Central Bank Speeches](central_bank_speeches/) | Vector search for central bank speeches | `marimo run central_bank_speeches/dashboard.py` |

## Usage in JupyterHub

These dashboards are pre-installed at `/home/jovyan/dashboards/` in JupyterHub.

1. Start a JupyterHub session
2. Open a terminal
3. Run the dashboard:
   ```bash
   cd ~/dashboards
   marimo run central_bank_speeches/dashboard.py
   ```
4. Access the dashboard at the displayed URL

## Local Development

```bash
# Clone this repository
git clone https://github.com/aerugo/brev-dashboards.git
cd brev-dashboards

# Set environment variables
export WEAVIATE_HOST=localhost
export WEAVIATE_PORT=8080
export WEAVIATE_GRPC_PORT=50051
export NIM_EMBEDDING_ENDPOINT=http://localhost:8000
export LAKEFS_ENDPOINT=http://localhost:8001
export LAKEFS_ACCESS_KEY_ID=your-key
export LAKEFS_SECRET_ACCESS_KEY=your-secret

# Port forward services (if running in Kubernetes)
kubectl port-forward svc/weaviate -n weaviate 8080:8080 &
kubectl port-forward svc/lakefs -n lakefs 8001:8000 &

# Run the dashboard
marimo run central_bank_speeches/dashboard.py
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `WEAVIATE_HOST` | Weaviate service host | `weaviate.weaviate.svc.cluster.local` |
| `WEAVIATE_PORT` | Weaviate HTTP port | `8080` |
| `WEAVIATE_GRPC_PORT` | Weaviate gRPC port | `50051` |
| `NIM_EMBEDDING_ENDPOINT` | NIM embedding service URL | `http://nvidia-nim-embedding.nvidia-nim.svc.cluster.local:8000` |
| `LAKEFS_ENDPOINT` | LakeFS endpoint | `http://lakefs.lakefs.svc.cluster.local:8000` |
| `LAKEFS_ACCESS_KEY_ID` | LakeFS access key | (required) |
| `LAKEFS_SECRET_ACCESS_KEY` | LakeFS secret key | (required) |

## Adding New Dashboards

1. Create a new directory: `mkdir my_dashboard`
2. Add required files:
   - `dashboard.py` - Main Marimo application
   - `utils.py` - Helper functions
   - `README.md` - Documentation
   - `pyproject.toml` - Dependencies
3. Update root `pyproject.toml` if adding new dependencies
4. Update this README with the new dashboard