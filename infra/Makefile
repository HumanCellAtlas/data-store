COMPONENTS=${shell find * -not -path "*/\.*" -type d}

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
	@echo $(DIRS)
	@echo $(COMPONENTS)
	cd $(COMPONENT); terraform apply

destroy: init
	cd $(COMPONENT); terraform destroy

init:
	rm -rf $(COMPONENT)/.terraform
	./build_deploy_config.py $(COMPONENT)
	cd $(COMPONENT); terraform init;

clean:
	cd $(COMPONENT); -rm -rf .terraform

.PHONY: init-all plan-all apply-all clean-all apply destroy init clean
