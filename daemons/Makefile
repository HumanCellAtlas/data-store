include ../common.mk

deploy: dss-sync dss-index dss-gs-event-relay dss-chunked-task

dss-sync dss-index:
	git clean -df $@/domovoilib $@/vendor
	shopt -s nullglob; for wheel in $@/vendor.in/*/*.whl; do unzip -o -d $@/vendor $$wheel; done
	cp -R ../dss ../dss-api.yml $@/domovoilib
	cp "$(GOOGLE_APPLICATION_CREDENTIALS)" $@/domovoilib/gcp-credentials.json
	./build_deploy_config.sh $@ $(DSS_DEPLOYMENT_STAGE)
	cd $@; domovoi deploy --stage $(DSS_DEPLOYMENT_STAGE)
	./invoke_lambda.sh $@ $(DSS_DEPLOYMENT_STAGE) \
	    ../tests/daemons/sample_s3_bundle_created_event.json.template \
	    ../tests/daemons/a47b90b2-0967-4fbf-87bc-c6c12db3fedf.2017-07-12T055120.037644Z; \

awschunkedtasktargets := $(shell ../scripts/get_all_dss_chunked_task_clients.py)
dss-chunked-task: $(awschunkedtasktargets)
$(awschunkedtasktargets): dss-chunked-task-%:
	rm -rf $@
	cp -rf dss-chunked-task $@
	cat dss-chunked-task/.chalice/config.json | jq '.app_name="$@" | .stages.$(DSS_DEPLOYMENT_STAGE).environment_variables.DSS_CHUNKED_TASK_CLIENT_NAME="$*"' > $@/.chalice/config.json
	shopt -s nullglob; for wheel in $@/vendor.in/*/*.whl; do unzip -o -d $@/vendor $$wheel; done
	cp -R ../dss ../dss-api.yml $@/domovoilib
	cp "$(GOOGLE_APPLICATION_CREDENTIALS)" $@/domovoilib/gcp-credentials.json
	./build_deploy_config.sh $@ $(DSS_DEPLOYMENT_STAGE)
	cd $@; DSS_CHUNKED_TASK_CLIENT_NAME=$* domovoi deploy --stage $(DSS_DEPLOYMENT_STAGE)

dss-gs-event-relay:
	$(DSS_HOME)/scripts/deploy_gcf.py $@ --entry-point "dss_gs_bucket_events_$(subst -,_,$(DSS_GS_BUCKET))"

.PHONY: dss-sync dss-index dss-chunked-task $(awschunkedtasktargets) dss-gs-event-relay
