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