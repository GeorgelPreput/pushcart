setup:
	@echo "Before continuing, please ensure you have the following tools installed and configured:"
	@echo "  - python"
	@echo "  - pyenv"
	@echo "  - poetry >= 1.6.1"
	@echo ""

	@read -p "Continue? (Y/n)" -n 1 -r; \
	@echo ""; \
	if [[ ! $$REPLY =~ ^[Yy]$$ ]] && [[ -n $$REPLY ]]; then \
		echo "Exiting."; \
		exit 1; \
	fi

	pyenv install --skip-existing 3.11.0
	pyenv local 3.11.0
	which python
	python --version

	curl -sSL https://install.python-poetry.org | python -
	poetry --version

	poetry env use 3.11.0
	poetry install
	. .venv/bin/activate
	.venv/bin/pre-commit install

test:
	-@if [[ -n "$$BASH_VERSION" ]] || [[ -n "$$ZSH_VERSION" ]]; then \
		. .venv/bin/activate; \
	elif [[ -n "$$FISH_VERSION" ]]; then \
		. .venv/bin/activate.fish; \
	else \
		echo "Unsupported shell. Only bash, zsh, and fish are supported."; \
		exit 1; \
	fi

	.venv/bin/pre-commit run --all-files
	poetry update
	poetry build
	poetry install --only-root
	pytest --cov=src tests/

clean:
	-@if command -v deactivate &> /dev/null; then \
		bash -c "source deactivate" || true; \
	fi
	rm -rf .venv
	find . -name ".pytest_cache" -type d -exec rm -rf {} +
	find . -name ".mypy_cache" -type d -exec rm -rf {} +
	find . -name ".ruff_cache" -type d -exec rm -rf {} +
	find . -name "__pycache__" -type d -exec rm -rf {} +
	find . -name "*.egg-info" -type d -exec rm -rf {} +
	find . -name "*.dist-info" -type d -exec rm -rf {} +
	find . -name "*.egg" -type d -exec rm -rf {} +

docs:
	mkdocs build
