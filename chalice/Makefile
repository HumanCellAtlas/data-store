include ../common.mk

deploy:
	git clean -df chalicelib
	cp -R ../dss ../dss-api.yml ../gcp-credentials.json chalicelib
	./build_deploy_config.sh $(STAGE)
	chalice deploy --no-autogen-policy --stage $(STAGE) --api-gateway-stage $(STAGE)
