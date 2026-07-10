PYTHON ?= python3
PYTHONPATH := src
SAMPLE_FILE := data/sample/events.ndjson

.PHONY: sample demo test lint broker-up broker-down produce dashboard clean

sample:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m streaming_analytics.generator --count 250 --seed 42 --include-edge-cases --output $(SAMPLE_FILE)

demo: sample
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m streaming_analytics.local_pipeline --input $(SAMPLE_FILE) --output-dir output

test:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m unittest discover -s tests -v

lint:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m compileall -q src dashboard airflow tests

broker-up:
	docker compose up -d

broker-down:
	docker compose down

produce:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m streaming_analytics.producer --input $(SAMPLE_FILE)

dashboard:
	streamlit run dashboard/app.py

clean:
	rm -rf output checkpoints

