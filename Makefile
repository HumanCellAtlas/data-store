lint:
	flake8 dss test

test: lint
	python -m unittest discover tests

deploy: chalice/chalicelib
	git clean -df chalice/chalicelib
	cp -R dss dss-api.yml chalice/chalicelib
	(cd chalice; chalice deploy)

.PHONY: test lint
