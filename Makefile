JFROG_API_TOKEN ?= DUMMY

CONDA_BUILD_NAME := airflow
CONDA_BUILD_VERSION ?= 1.10.9.wooga
CONDA_TARGET := $(shell CONDA_BUILD_VERSION=$(CONDA_BUILD_VERSION) CONDA_BUILD_NAME=$(CONDA_BUILD_NAME) conda build --python=3.6 --output conda/recipe)
CONDA_PKG := $(notdir $(CONDA_TARGET))

CONDA_PUBLISH_URL ?= dummy

clean: clean-build clean-pyc clean-test ## remove all build, test, coverage and Python artifacts

clean-build: ## remove build artifacts
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +

clean-pyc: ## remove Python file artifacts
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-test: ## remove test and coverage artifacts
	rm -fr .tox/
	rm -f .coverage
	rm -fr htmlcov/

install: clean
	SLUGIFY_USES_TEXT_UNIDECODE=yes pip install -e .[dev]

init: install # for backwards compatibility

release: clean
	python setup.py sdist upload
	python setup.py bdist_wheel upload

conda-release: clean
	CONDA_BUILD_VERSION=$(CONDA_BUILD_VERSION) CONDA_BUILD_NAME=$(CONDA_BUILD_NAME) conda build --channel=defaults --python=3.6 conda/recipe/

conda-build:
	@echo $(CONDA_PKG)

conda-publish: conda-release
	JFROG_API_TOKEN=$(JFROG_API_TOKEN) curl -H 'X-JFrog-Art-Api:${JFROG_API_TOKEN}' -T $(CONDA_TARGET) "${CONDA_PUBLISH_URL}/noarch/${CONDA_PKG}"

test:
	behave
