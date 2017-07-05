include ../common.mk

deploy: dss-sync dss-index

dss-sync dss-index:
	git clean -df $@/domovoilib
	cp -R ../dss ../dss-api.yml $@/domovoilib
	cp "$(GOOGLE_APPLICATION_CREDENTIALS)" $@/domovoilib/gcp-credentials.json
	./build_deploy_config.sh $@ $(STAGE)
	cd $@; domovoi deploy --stage $(STAGE)
	./invoke_lambda.sh $@ $(STAGE) ../tests/sample_s3_bundle_created_event.json

.PHONY: dss-sync dss-index
