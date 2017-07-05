include ../common.mk

deploy:
	git clean -df chalicelib
	cp -R ../dss ../dss-api.yml "$(GOOGLE_APPLICATION_CREDENTIALS)" chalicelib
	./build_deploy_config.sh $(STAGE)
	chalice deploy --no-autogen-policy --stage $(STAGE) --api-gateway-stage $(STAGE)
