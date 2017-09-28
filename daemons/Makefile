include ../common.mk

deploy: dss-sync dss-index dss-gs-event-relay dss-chunked-task

dss-sync dss-index:
	git clean -df $@/domovoilib $@/vendor
	shopt -s nullglob; for wheel in $@/vendor.in/*/*.whl; do unzip -q -o -d $@/vendor $$wheel; done
	cp -R ../dss ../dss-api.yml $@/domovoilib
	cp "$(GOOGLE_APPLICATION_CREDENTIALS)" $@/domovoilib/gcp-credentials.json
	./build_deploy_config.sh $@ $(DSS_DEPLOYMENT_STAGE)
	cd $@; domovoi deploy --stage $(DSS_DEPLOYMENT_STAGE)
	./invoke_lambda.sh $@ $(DSS_DEPLOYMENT_STAGE) \
	    ../tests/daemons/sample_s3_bundle_created_event.json.template \
	    ../tests/daemons/a47b90b2-0967-4fbf-87bc-c6c12db3fedf.2017-07-12T055120.037644Z; \

chainedawslambdatargets := dss-s3-copy-write-metadata

chained-aws-lambda: $(chainedawslambdatargets)
$(chainedawslambdatargets): dss-%:
	rm -rf $@
	cp -rf chained-aws-lambda $@
	chained_aws_lambda.py '$$account_id' \
	  --policy-path $@/policy.json.template \
	  --app-path $@/app.py \
	  --lambda-name dss-$*-$(DSS_DEPLOYMENT_STAGE)

	cat chained-aws-lambda/.chalice/config.json | jq '.app_name="dss-$*"' > $@/.chalice/config.json
	# grant access to $DSS_S3_BUCKET and $DSS_S3_BUCKET_TEST.  in build_deploy_config.sh, this gets replaced with the
	# actual bucket names.
	cat $@/policy.json.template | jq -f s3_enable.filter | sponge $@/policy.json.template
	shopt -s nullglob; for wheel in $@/vendor.in/*/*.whl; do unzip -q -o -d $@/vendor $$wheel; done
	cp -R ../dss $@/domovoilib
	cp "$(GOOGLE_APPLICATION_CREDENTIALS)" $@/domovoilib/gcp-credentials.json
	iam_policy_template=$@/policy.json.template ./build_deploy_config.sh $@ $(DSS_DEPLOYMENT_STAGE)
	cd $@; domovoi deploy --stage $(DSS_DEPLOYMENT_STAGE)

dss-gs-event-relay:
	$(DSS_HOME)/scripts/deploy_gcf.py $@ --entry-point "dss_gs_bucket_events_$(subst -,_,$(DSS_GS_BUCKET))"

.PHONY: dss-sync dss-index $(chainedawslambdatargets) dss-gs-event-relay
