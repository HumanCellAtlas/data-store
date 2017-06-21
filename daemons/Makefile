SHELL=/bin/bash
STAGE ?= dev

deploy: dss-sync dss-index

dss-sync dss-index:
	git clean -df $@/domovoilib
	cp -R ../dss ../dss-api.yml ../gcs-credentials.json $@/domovoilib
	./build_deploy_config.sh $@ $(STAGE)
	if [[ $@ == "dss-index" ]]; \
	    then ./deploy_aws_elasticsearch.sh $@ $(STAGE); \
	fi
	cd $@; domovoi deploy
	./invoke_lambda.sh $@ $(STAGE) ../tests/sample_s3_bundle_created_event.json

[[ $(echo "$lambda_output" | jq -r .FunctionError) == null ]]

logs=$(echo "$lambda_output" | jq -r .LogResult | base64 --decode)


.PHONY: dss-sync dss-index
