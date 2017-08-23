SHELL=/bin/bash

ifndef DSS_HOME
$(error Please run "source environment" in the data-store repo root directory before running make commands)
endif

ifeq ($(shell which jq),)
$(error Please install jq using "apt-get install jq" or "brew install jq")
endif

ifeq ($(shell which sponge),)
$(error Please install sponge using "apt-get install moreutils" or "brew install moreutils")
endif

ifeq ($(shell which envsubst),)
$(error Please install envsubst using "apt-get install gettext" or "brew install gettext; brew link gettext")
endif

wheels:
	pip download --dest vendor.in $$($(DSS_HOME)/scripts/find_missing_wheels.py requirements.txt)
	pip wheel --wheel-dir vendor.in vendor.in/*.tar.gz
	rm -f vendor.in/*.tar.gz
