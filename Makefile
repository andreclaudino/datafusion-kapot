BINARY_NAME := $$(cat Cargo.toml | grep name | head -n 1 | awk '{print $$3}' | sed -r 's/^"|"$$//g')
PROJECT_VERSION := $$(cat Cargo.toml | grep version | head -n 1 | awk '{print $$3}' | sed -r 's/^"|"$$//g')
GIT_REFERENCE := $$(git log -1 --pretty=%h)

release:
	cargo workspace-version update $(PROJECT_VERSION)
	git add kapot examples
	git commit -m "Update version to $(PROJECT_VERSION)"
	git tag v$(PROJECT_VERSION) --force
	git tag $(PROJECT_VERSION) --force 
	git push
	git push --tags --force

publish: release
	cargo publish -p kapot-cli
	cargo publish -p kapot-cache
	cargo publish -p kapot-client
	cargo publish -p kapot-core
	cargo publish -p kapot-executor
	cargo publish -p kapot-scheduler
