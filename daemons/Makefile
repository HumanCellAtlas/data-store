include ../common.mk

SERIAL_AWS_DAEMONS := \
	dss-sync-sfn \
	dss-notify-v2 \
	dss-operations \
	dss-index

SERIAL_GCP_DAEMONS :=

PARALLEL_AWS_DAEMONS := \
	dss-checkout-sfn \
	dss-gs-copy-sfn \
	dss-gs-copy-write-metadata-sfn \
	dss-s3-copy-sfn \
	dss-s3-copy-write-metadata-sfn \
	dss-scalability-test \
	dss-dlq-reaper \
	dss-sfn-launcher \
	dss-notify \
	dss-events-scribe


PARALLEL_GCP_DAEMONS := \
	dss-gs-event-relay

deploy: deploy-serial deploy-parallel
deploy-serial: $(SERIAL_AWS_DAEMONS) $(SERIAL_GCP_DAEMONS)
deploy-parallel: $(PARALLEL_AWS_DAEMONS) $(PARALLEL_GCP_DAEMONS)

generate-dependencies:
	${DSS_HOME}/scripts/generate_upload_requirements_layer.sh

$(SERIAL_AWS_DAEMONS) $(PARALLEL_AWS_DAEMONS): generate-dependencies
	@for f in $@/*.tf; do \
		echo "Terraforming $@"; \
		if [ -e $$f ]; then \
			rm -rf $@/.terraform/*.tfstate;\
			./build_tf_deploy_config.py $@;\
			(cd $@ ; terraform init);\
			(cd $@ ; TF_VAR_daemon=$@ terraform apply -auto-approve);\
		fi;\
		break;\
	done
	./package_daemon.sh $@
	./build_deploy_config.sh $@
	cd $@ && domovoi deploy --stage $(DSS_DEPLOYMENT_STAGE)
	@if [[ $@ == "dss-sync-sfn" || $@ == "dss-index" || $@ == "dss-notify-v2" ]]; then \
            ./invoke_lambda.sh $@ $(DSS_DEPLOYMENT_STAGE) \
            ../tests/daemons/sample_s3_bundle_created_event.json.template \
            ../tests/daemons/a47b90b2-0967-4fbf-87bc-c6c12db3fedf.2017-07-12T055120.037644Z; \
        fi
	@if [[ $@ == "dss-notify" ]]; then \
	        cd $@ && python -c 'import app; app.deploy_notifier()' ; \
        fi

dss-gs-event-relay:
	$(DSS_HOME)/scripts/deploy_gcf.py $@ --entry-point "dss_gs_bucket_events_$(subst -,_,$(DSS_GS_BUCKET))"

import-test:
	set -e; \
        for daemon in $(SERIAL_AWS_DAEMONS) $(PARALLEL_AWS_DAEMONS); do \
            ./package_daemon.sh $$daemon; \
            python $$daemon/app.py; \
        done
	python dss-gs-event-relay/main.py

.PHONY: deploy deploy-serial deploy-parallel import-test $(SERIAL_AWS_DAEMONS) $(SERIAL_GCP_DAEMONS) $(PARALLEL_AWS_DAEMONS) $(PARALLEL_GCP_DAEMONS)
