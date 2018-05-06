include ../common.mk

deploy:
	git clean -df chalicelib vendor
	shopt -s nullglob; for wheel in vendor.in/*/*.whl; do unzip -q -o -d vendor $$wheel; done
	cp -R ../dss ../dss-api.yml chalicelib
	../scripts/get_dss_secret.py gcp-credentials.json chalicelib/gcp-credentials.json
	../scripts/get_dss_secret.py application_secrets.json chalicelib/application_secrets.json
	./build_deploy_config.sh
	../scripts/dss-chalice deploy --no-autogen-policy --stage $(DSS_DEPLOYMENT_STAGE) --api-gateway-stage $(DSS_DEPLOYMENT_STAGE)
