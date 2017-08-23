include ../common.mk

deploy:
	git clean -df chalicelib vendor
	$(MAKE) vendor.in/*.whl
	cp -R ../dss ../dss-api.yml chalicelib
	cp "$(GOOGLE_APPLICATION_CREDENTIALS)" chalicelib/gcp-credentials.json
	cp "$(GOOGLE_APPLICATION_SECRETS)" chalicelib/application_secrets.json
	./build_deploy_config.sh $(DSS_DEPLOYMENT_STAGE)
	../scripts/dss-chalice deploy --no-autogen-policy --stage $(DSS_DEPLOYMENT_STAGE) --api-gateway-stage $(DSS_DEPLOYMENT_STAGE)

vendor.in/*.whl:
	unzip -o -d vendor $@

.PHONY: vendor.in/*.whl
