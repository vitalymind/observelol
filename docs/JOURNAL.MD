# 23.10.2026
- Created PAT in github
- Enabled build and secret APIs in GCP
- Write automation scripts for:
	- GCP APIs enabling
	- cloud builder account creation
	- creating webhook GCP-to-GitHub
- Moved from cloud build trigger to github actions (OIDC/WIF)

# 24.10.2026
- Moved to config.yml from .env
- Updated boostrap script
- Removed github variables, using config with yq
- added infra definitins to config
- created terraform configs
- deployed empty infra to GCP
- Defined folder structure for services and VM configs
- Designed poller sidecar to watch over ingest VM
- Imporved github actions, orginized folder structure and action calls

# 25.10.2026
- Removed poller sidecar
- Using tofu meta-data to pass cloud-init.yml
- cloud-init.yml is responsible of init on VM
- cloud-init runs docker:cli compose up -d --pull always
- Added letEncrypt bootstrap script, save cert to GCS
- Fixed bunch of problems with deployment
- Created proper smoke test using real VM test deployment
- Introduced payload schema validation (axum + serde)
- Optimized data saved to redis