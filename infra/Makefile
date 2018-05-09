DIRS=${shell find ${shell pwd -P}/* -not -path "*/\.*" -type d}
COMPONENTS=${shell basename $(DIRS)}

all: init-all

init-all:
	@for c in $(COMPONENTS); do \
		$(MAKE)	init COMPONENT=$$c; \
	done

apply-all:
	@for c in $(COMPONENTS); do \
		$(MAKE)	apply COMPONENT=$$c; \
	done

destroy-all:
	@for c in $(COMPONENTS); do \
		$(MAKE)	destroy COMPONENT=$$c; \
	done

clean-all:
	@for c in $(COMPONENTS); do \
		$(MAKE)	clean COMPONENT=$$c; \
	done

apply: init
	cd $(COMPONENT); AWS_PROFILE=$(AWS_PROFILE) terraform apply

destroy: init
	cd $(COMPONENT); AWS_PROFILE=$(AWS_PROFILE) terraform destroy

init:
	./build_deploy_config.py
	cd $(COMPONENT); terraform init;

clean:
	cd $(COMPONENT); -rm -rf .terraform

.PHONY: init-all plan-all apply-all clean-all apply destroy init clean
