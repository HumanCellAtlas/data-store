include ../common.mk

SERIAL_AWS_DAEMONS := \
	dss-sync \
	dss-index

SERIAL_GCP_DAEMONS :=

PARALLEL_AWS_DAEMONS := \
	dss-admin \
	dss-checkout-sfn \
	dss-gs-copy-sfn \
	dss-gs-copy-write-metadata-sfn \
	dss-s3-copy-sfn \
	dss-s3-copy-write-metadata-sfn \
	dss-visitation \
	dss-scalability-test \
	dss-dlq-reaper \
	dss-sfn-launcher \
	dss-notify


PARALLEL_GCP_DAEMONS := \
	dss-gs-event-relay

deploy: deploy-serial deploy-parallel
deploy-serial: $(SERIAL_AWS_DAEMONS) $(SERIAL_GCP_DAEMONS)
deploy-parallel: $(PARALLEL_AWS_DAEMONS) $(PARALLEL_GCP_DAEMONS)

$(SERIAL_AWS_DAEMONS) $(PARALLEL_AWS_DAEMONS) scheduled-ci-build:
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
	@if [[ $@ == "dss-sync" || $@ == "dss-index" ]]; then \
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

.PHONY: deploy deploy-serial deploy-parallel scheduled-ci-build import-test $(SERIAL_AWS_DAEMONS) $(SERIAL_GCP_DAEMONS) $(PARALLEL_AWS_DAEMONS) $(PARALLEL_GCP_DAEMONS)
