include ../common.mk

deploy: dss-sync dss-index

dss-sync dss-index:
	git clean -df $@/domovoilib
	cp -R ../dss ../dss-api.yml ../gcp-credentials.json $@/domovoilib
	./build_deploy_config.sh $@ $(STAGE)
	cd $@; domovoi deploy
	./invoke_lambda.sh $@ $(STAGE) ../tests/sample_s3_bundle_created_event.json

.PHONY: dss-sync dss-index
