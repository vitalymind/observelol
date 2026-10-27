# 22.10.2026
- Figuring out overall architecture
- Planning main project constraints
- Ironing out project stack

# 23.10.2026
- Finalizing architecture
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
- Removed LUA script, using rpush directly
- Moved TTL management to Rust with dashmap
- Several extra modifications, for faster hot loop

# 26.10.2026
- Added test and deploy actions for Worker
- Added vm orchestration and infra definitions
- Changed schema, added replay version
- Adding generated Rust implementation
- Modify build actions to use GH cache
- XGROUP CREATE is handled by Workers, not Dispatcher
- Errors use error_code instead of text
- Clean up and organization of Worker Rust code
- Encodind/Decoding of replays are done according to protobuf schema
- Making action files for contorl CI
- Making deploy orchestration
- configuring otel-lgtm
- Added bare bone control with otel-lgtm stack
- Added simulation scripts
- Added grafana dashboard

# 27.10.2026
- Adding python load tester app wrapping Locust
- Figuring out problem with redis automatic default response timeout 500 ms and related csacading blocking failure
- Moved to medium VM for ingest
- Improved grafana tunnel, multiplexed connection
- Switched to cAdvisor from node_exporter to see into SPU use per container
- 