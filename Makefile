install-hooks:
	pre-commit install --install-hooks
.PHONY: install-hooks

lint:
	pre-commit run --all-files
.PHONY: lint
