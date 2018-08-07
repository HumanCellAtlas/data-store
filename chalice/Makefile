include ../common.mk

deploy:
	git clean -df chalicelib vendor
	shopt -s nullglob; for wheel in vendor.in/*/*.whl; do unzip -q -o -d vendor $$wheel; done
	cp -R ../dss ../dss-api.yml chalicelib
	aws secretsmanager get-secret-value --secret-id ${DSS_SECRETS_STORE}/${DSS_DEPLOYMENT_STAGE}/gcp-credentials.json | jq -r .SecretString > chalicelib/gcp-credentials.json
	aws secretsmanager get-secret-value --secret-id ${DSS_SECRETS_STORE}/${DSS_DEPLOYMENT_STAGE}/application_secrets.json | jq -r .SecretString > chalicelib/application_secrets.json
	./build_deploy_config.sh
	chalice deploy --no-autogen-policy --stage $(DSS_DEPLOYMENT_STAGE) --api-gateway-stage $(DSS_DEPLOYMENT_STAGE)
