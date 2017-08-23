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
	cd vendor.in; pip download $$($(DSS_HOME)/scripts/find_missing_wheels.py)
	cd vendor.in; pip wheel *.tar.gz
	cd vendor.in; rm -f *.tar.gz
