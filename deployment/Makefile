COMPONENT=
STAGEPATH=${shell cd active && pwd -P}
STAGE=${shell basename $(STAGEPATH)}
DIRS=${shell find $(STAGE)/* -not -path "*/\.*" -type d}
COMPONENTS=${shell basename $(DIRS)}
AWS_PROFILE=${shell cat $(STAGE)/local_variables.tf | jq -r .variable.aws_profile.default}

all: init

init:
	@echo $(STAGE)
	@echo $(COMPONENTS)
	@for c in $(COMPONENTS); do \
		$(MAKE)	init-component STAGE=$(STAGE) COMPONENT=$$c; \
	done

apply:
	@echo $(STAGE)
	@for c in $(COMPONENTS); do \
		$(MAKE)	apply-component STAGE=$(STAGE) COMPONENT=$$c; \
	done

destroy:
	@echo $(STAGE)
	@for c in $(COMPONENTS); do \
		$(MAKE)	destroy-component STAGE=$(STAGE) COMPONENT=$$c; \
	done

clean:
	@echo $(STAGE)
	@for c in $(COMPONENTS); do \
		$(MAKE)	clean-component STAGE=$(STAGE) COMPONENT=$$c; \
	done

init-component:
	@if [[ -e $(STAGE)/backend_config.hcl ]]; then \
		cd $(STAGE)/$(COMPONENT); AWS_PROFILE=$(AWS_PROFILE) terraform init --backend-config=../backend_config.hcl; \
	else \
		cd $(STAGE)/$(COMPONENT); AWS_PROFILE=$(AWS_PROFILE) terraform init; \
	fi

apply-component: init-component
	cd $(STAGE)/$(COMPONENT); AWS_PROFILE=$(AWS_PROFILE) terraform apply

destroy-component: init-component
	cd $(STAGE)/$(COMPONENT); AWS_PROFILE=$(AWS_PROFILE) terraform destroy

clean-component:
	cd $(STAGE)/$(COMPONENT); -rm -rf .terraform

.PHONY: init plan apply clean
