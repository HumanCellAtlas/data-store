include ../common.mk

deploy:
	git clean -df chalicelib vendor
	shopt -s nullglob; for wheel in vendor.in/*/*.whl; do unzip -q -o -d vendor $$wheel; done
	cp -R ../dss ../dss-api.yml chalicelib
	cp "$(GOOGLE_APPLICATION_CREDENTIALS)" chalicelib/gcp-credentials.json
	cp "$(GOOGLE_APPLICATION_SECRETS)" chalicelib/application_secrets.json
	chmod -R ugo+rX chalicelib
	./build_deploy_config.sh
	../scripts/dss-chalice deploy --no-autogen-policy --stage $(DSS_DEPLOYMENT_STAGE) --api-gateway-stage $(DSS_DEPLOYMENT_STAGE)
