STAGE ?= dev

deploy: dss-sync dss-index

dss-sync dss-index:
	git clean -df $@/domovoilib
	cp -R ../dss ../dss-api.yml $@/domovoilib
	./build_deploy_config.sh $@ $(STAGE)
	cd $@; domovoi deploy

.PHONY: dss-sync dss-index
