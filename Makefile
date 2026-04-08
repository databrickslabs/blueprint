all: clean lint fmt test coverage

# Ensure that all uv commands are locked and don't automatically update the lock file.
export UV_LOCKED := 1
# Ensure that hatchling is pinned when builds are needed.
export UV_BUILD_CONSTRAINT := .build-constraints.txt

UV_RUN := uv run --exact --all-extras
UV_TEST := $(UV_RUN) pytest -n 4 --timeout 30 --durations 20

clean:
	rm -fr .venv clean htmlcov .mypy_cache .pytest_cache .ruff_cache .coverage coverage.xml
	find . -name '__pycache__' -print0 | xargs -0 rm -fr

dev:
	uv sync --all-extras

lint:
	$(UV_RUN) isort . --check-only
	$(UV_RUN) ruff format --check --diff
	$(UV_RUN) ruff check .
	$(UV_RUN) mypy .
	$(UV_RUN) pylint --output-format=colorized -j 0 src

fmt:
	$(UV_RUN) isort .
	$(UV_RUN) ruff format
	$(UV_RUN) ruff check . --fix
	$(UV_RUN) mypy .
	$(UV_RUN) pylint --output-format=colorized -j 0 src

test:
	$(UV_TEST) --cov src --cov-report=xml tests/unit

integration:
	$(UV_TEST) --cov src --cov-report=xml tests/integration

coverage:
	$(UV_TEST) --cov src --cov-report=html tests/unit
	open htmlcov/index.html

build:
	uv build --require-hashes --build-constraints=.build-constraints.txt

lock-dependencies: UV_LOCKED := 0
lock-dependencies:
	uv lock
	$(UV_RUN) --group yq tomlq -r '.["build-system"].requires[]' pyproject.toml | \
	    uv pip compile --generate-hashes --universal --no-header - > build-constraints-new.txt
	mv build-constraints-new.txt .build-constraints.txt

.DEFAULT: all
.PHONY: all clean dev lint fmt test integration coverage build lock-dependencies
