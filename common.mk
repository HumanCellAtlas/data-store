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

ifeq ($(findstring Python 3.7, $(shell python --version 2>&1)),)
$(error Please run make commands from a Python 3.7 virtualenv)
endif


ifeq ($(findstring terraform, $(shell which terraform 2>&1)),)
else ifeq ($(findstring Terraform v0.12.16, $(shell terraform --version 2>&1)),)
$(error You must use Terraform v0.12.16, please check your terraform version.)
endif
