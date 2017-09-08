include common.mk
MODULES=dss tests

lint:
	flake8 $(MODULES) chalice/*.py daemons/*/*.py

mypy:
	mypy --ignore-missing-imports $(MODULES)

test_srcs := $(wildcard tests/test_*.py)

test: lint mypy
	coverage run --source=dss -m unittest discover tests -v

fast_test: lint mypy $(test_srcs)

$(test_srcs): %.py :
	python -m unittest $@

smoketest:
	scripts/smoketest.py

deploy:
	$(MAKE) -C chalice deploy
	$(MAKE) -C daemons deploy

clean:
	git clean -Xdf chalice daemons $(MODULES)
	git clean -df {chalice,daemons/*}/{chalicelib,domovoilib,vendor}
	git checkout {chalice,daemons/*}/.chalice/config.json

requirements.txt requirements-dev.txt : %.txt : %.txt.in
	[ ! -e .requirements-env ] || exit 1
	echo "# You should not edit this file directly.  Instead, you should edit $<." > $@
	virtualenv .requirements-env
	source .requirements-env/bin/activate && \
	pip install -r $< && \
	pip freeze >> $@
	rm -rf .requirements-env
	scripts/find_missing_wheels.py requirements.txt

requirements-dev.txt : requirements.txt.in

.PHONY: test lint mypy $(test_srcs)
