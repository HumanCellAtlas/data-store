SHELL=/bin/bash
STAGE ?= dev

deploy: dss-sync dss-index

dss-sync dss-index:
	git clean -df $@/domovoilib
	cp -R ../dss ../dss-api.yml $@/domovoilib
	./build_deploy_config.sh $@ $(STAGE)
	if [[ $@ == "dss-index" ]]; \
	    then ./deploy_aws_elasticsearch.sh $@ $(STAGE); \
	fi
	cd $@; domovoi deploy

.PHONY: dss-sync dss-index
