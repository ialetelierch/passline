.PHONY: run schema ingest staging load quality export

run: schema ingest staging load quality export

schema:
	python db/schema.py

ingest:
	python ingestion/ingest.py

staging:
	python ingestion/staging.py

load:
	python db/load.py

quality:
	python quality/checks.py

export:
	python exports/export.py
