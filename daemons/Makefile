include ../common.mk

SERIAL_AWS_DAEMONS := \
	dss-sync \
	dss-index \

SERIAL_GCP_DAEMONS :=

PARALLEL_AWS_DAEMONS := \
	dss-admin \
	dss-checkout-sfn \
	dss-gs-copy-sfn \
	dss-gs-copy-write-metadata-sfn \
	dss-s3-copy-sfn \
	dss-s3-copy-write-metadata-sfn \
	dss-visitation \

PARALLEL_GCP_DAEMONS := \
	dss-gs-event-relay

deploy: deploy-serial deploy-parallel
deploy-serial: $(SERIAL_AWS_DAEMONS) $(SERIAL_GCP_DAEMONS)
deploy-parallel: $(PARALLEL_AWS_DAEMONS) $(PARALLEL_GCP_DAEMONS)

$(SERIAL_AWS_DAEMONS) $(PARALLEL_AWS_DAEMONS):
	git clean -df $@/domovoilib $@/vendor
	shopt -s nullglob; for wheel in $@/vendor.in/*/*.whl; do unzip -q -o -d $@/vendor $$wheel; done
	cp -R ../dss ../dss-api.yml $@/domovoilib
	cp "$(GOOGLE_APPLICATION_CREDENTIALS)" $@/domovoilib/gcp-credentials.json
	chmod -R ugo+rX $@/domovoilib
	./build_deploy_config.sh $@ $(DSS_DEPLOYMENT_STAGE)
	cd $@; domovoi deploy --stage $(DSS_DEPLOYMENT_STAGE)
	@if [[ $@ == "dss-sync" || $@ == "dss-index" ]]; then \
		./invoke_lambda.sh $@ $(DSS_DEPLOYMENT_STAGE) \
			../tests/daemons/sample_s3_bundle_created_event.json.template \
			../tests/daemons/a47b90b2-0967-4fbf-87bc-c6c12db3fedf.2017-07-12T055120.037644Z; \
	fi
	@if [[ $@ == "dss-s3-copy-sfn" || %@ == "dss-s3-copy-write-metadata-sfn" || $@ == "dss-checkout" ]]; then \
	    $(DSS_HOME)/scripts/deploy_checkout_lifecycle.py; \
	fi

dss-gs-event-relay:
	$(DSS_HOME)/scripts/deploy_gcf.py $@ --entry-point "dss_gs_bucket_events_$(subst -,_,$(DSS_GS_BUCKET))"

.PHONY: deploy deploy-serial deploy-parallel $(SERIAL_AWS_DAEMONS) $(SERIAL_GCP_DAEMONS) $(PARALLEL_AWS_DAEMONS) $(PARALLEL_GCP_DAEMONS)
