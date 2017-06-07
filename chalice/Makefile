deploy:
	git clean -df chalicelib
	cp -R ../dss ../dss-api.yml chalicelib
	./build_deploy_config.sh $(STAGE)
	chalice deploy --no-autogen-policy --stage $(STAGE)
