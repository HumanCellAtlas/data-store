include ../common.mk

deploy: vendor
	git clean -df chalicelib
	cp -R ../dss ../dss-api.yml chalicelib
	cp "$(GOOGLE_APPLICATION_CREDENTIALS)" chalicelib/gcp-credentials.json
	cp "$(GOOGLE_APPLICATION_SECRETS)" chalicelib/application_secrets.json
	./build_deploy_config.sh $(DSS_DEPLOYMENT_STAGE)
	../scripts/dss-chalice deploy --no-autogen-policy --stage $(DSS_DEPLOYMENT_STAGE) --api-gateway-stage $(DSS_DEPLOYMENT_STAGE)

vendor: vendor.in
	cd vendor; unzip ../vendor.in/*.whl

wheels:
	pip download --dest vendor.in $$(../scripts/find_missing_wheels.py requirements.txt)
	pip wheel --wheel-dir vendor.in vendor.in/*.tar.gz
	rm -f vendor.in/*.tar.gz
