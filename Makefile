PYTHON ?= python3
VENV ?= .venv
PIP := $(VENV)/bin/pip
PYTHON_VENV := $(VENV)/bin/python

.PHONY: venv install generate test streamlit

venv:
	$(PYTHON) -m venv $(VENV)

install:
	$(PIP) install -U pip
	$(PIP) install -r requirements.txt
	$(PIP) install -e .

generate:
	$(PYTHON_VENV) -m data_generator.generate --customers 10000 --transactions 500000 --batch-size 100000 --seed 42

test:
	$(PYTHON_VENV) -m pytest -q

streamlit:
	$(PYTHON_VENV) -m streamlit run app/app.py
