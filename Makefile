.PHONY: clean clean-pyc docs build-docs

COMMIT_HASH := $(shell git rev-parse --short --verify HEAD)
GIT_BRANCH := $(shell git rev-parse --abbrev-ref HEAD)
LIB_VERSION := $(shell python setup.py --version)
LIB_NAME := $(shell python setup.py --name)
SRC_DIRS := exporters/ bin/
COVERAGE_VERSION := $(shell coverage --version 2>/dev/null)

help:
	@echo clean - clean up previous build
	@echo docs - compile docs and show in the browser
	@echo servedocs - compile docs, show in the browser and watch for changes
	@echo build-lib - builds dataservices toolbox egg on dist/
	@echo build - builds dataservices project egg on dist/
	@echo install-all-deps - install all the project dependencies
	@echo help - show this help

compile:
	python -m compileall ${SRC_DIRS}

test: compile
	nosetests -v tests --with-coverage --cover-package=bin,toolbox --cover-branches


clean-pyc:
	find . -name \*.pyc -delete

clean: clean-pyc
	$(RM) -r build dist *.egg-info
	( cd dumpers; $(RM) -r build dist *.egg-info )


build-lib: clean
	python setup.py bdist_egg


build: clean
	python setupdeploy.py bdist_egg


install-all-deps:
	pip install -r requirements.txt -U


build-docs:
	rm -f docs/dataservices.rst
	rm -f docs/modules.rst
	sphinx-apidoc -o docs/modules/ exporters
	$(MAKE) -C docs clean
	$(MAKE) -C docs html

docs: build-docs
	$(BROWSER) docs/_build/html/index.html

servedocs: docs
	watchmedo shell-command -p '*.rst' -c '$(MAKE) -C docs html' -R -D .
