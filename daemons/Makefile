include ../common.mk

deploy: dss-sync dss-index dss-gs-event-relay chained-aws-lambda dss-visitation-sentinel dss-visitation-walker

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

dss-visitation-sentinel dss-visitation-walker:
	git clean -df $@/domovoilib $@/vendor
	shopt -s nullglob; for wheel in $@/vendor.in/*/*.whl; do unzip -q -o -d $@/vendor $$wheel; done
	cp -R ../dss ../dss-api.yml $@/domovoilib
	cp "$(GOOGLE_APPLICATION_CREDENTIALS)" $@/domovoilib/gcp-credentials.json
	./build_deploy_config.sh $@ $(DSS_DEPLOYMENT_STAGE)
	cd $@; domovoi deploy --stage $(DSS_DEPLOYMENT_STAGE)

chainedawslambdatargets := chained-aws-lambda-s3-parallel-copy-supervisor chained-aws-lambda-s3-parallel-copy-worker
dsschainedawslambdatargets := dss-s3-copy-write-metadata
chained-aws-lambda: $(chainedawslambdatargets) $(dsschainedawslambdatargets)

$(chainedawslambdatargets): chained-aws-lambda-%:
	rm -rf $@
	cp -rf chained-aws-lambda $@
	chained_aws_lambda.py '$$account_id' \
	  --enable-s3-parallel-copy-supervisor --s3-parallel-copy-supervisor-lambda-name chained-aws-lambda-s3-parallel-copy-supervisor-$(DSS_DEPLOYMENT_STAGE) \
	  --enable-s3-parallel-copy-worker --s3-parallel-copy-worker-lambda-name chained-aws-lambda-s3-parallel-copy-worker-$(DSS_DEPLOYMENT_STAGE) \
	  --$*-policy-path $@/policy.json.template \
	  --$*-app-path $@/app.py

	cat chained-aws-lambda/.chalice/config.json | jq '.app_name="$@"' > $@/.chalice/config.json
	# blindly enable access to our test S3 bucket.  This is ok since most daemons require it anyway.
	cat $@/policy.json.template | jq -f s3_enable.filter | sponge $@/policy.json.template
	cp -R ../dss $@/domovoilib
	cp "$(GOOGLE_APPLICATION_CREDENTIALS)" $@/domovoilib/gcp-credentials.json
	iam_policy_template=$@/policy.json.template ./build_deploy_config.sh $@ $(DSS_DEPLOYMENT_STAGE)
	cd $@; domovoi deploy --stage $(DSS_DEPLOYMENT_STAGE)

dss-s3-copy-write-metadata: dss-%:
	rm -rf $@
	cp -rf chained-aws-lambda $@
	chained_aws_lambda.py '$$account_id' \
	  --enable-s3-parallel-copy-worker --s3-parallel-copy-worker-lambda-name chained-aws-lambda-s3-parallel-copy-worker-$(DSS_DEPLOYMENT_STAGE) \
	  --policy-path $@/policy.json.template \
	  --app-path $@/app.py \
	  --lambda-name dss-$*-$(DSS_DEPLOYMENT_STAGE)

	cat chained-aws-lambda/.chalice/config.json | jq '.app_name="dss-$*"' > $@/.chalice/config.json
	# grant access to $DSS_S3_BUCKET and $DSS_S3_BUCKET_TEST.  in build_deploy_config.sh, this gets replaced with the
	# actual bucket names.
	cat $@/policy.json.template | \
	  jq -f s3_enable.filter | \
	  jq -f dss-s3-copy-write-metadata.filter | \
	  sponge $@/policy.json.template
	shopt -s nullglob; for wheel in $@/vendor.in/*/*.whl; do unzip -q -o -d $@/vendor $$wheel; done
	cp -R ../dss $@/domovoilib
	cp "$(GOOGLE_APPLICATION_CREDENTIALS)" $@/domovoilib/gcp-credentials.json
	iam_policy_template=$@/policy.json.template ./build_deploy_config.sh $@ $(DSS_DEPLOYMENT_STAGE)
	cd $@; domovoi deploy --stage $(DSS_DEPLOYMENT_STAGE)

dss-gs-event-relay:
	$(DSS_HOME)/scripts/deploy_gcf.py $@ --entry-point "dss_gs_bucket_events_$(subst -,_,$(DSS_GS_BUCKET))"

.PHONY: dss-sync dss-index dss-visitation-sentinel dss-visitation-walker $(chainedawslambdatargets) $(dsschainedawslambdatargets) dss-gs-event-relay
