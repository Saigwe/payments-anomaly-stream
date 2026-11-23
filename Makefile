PY ?= python
URL = https://data.openml.org/datasets/0000/1597/dataset_1597.pq
SHA256 = b7efcb35a428bbe22347a05d2437d9177bab07ce61e51214a17bec584ad9496d

.PHONY: all download run test clean charts

all: download run test   ## real card transactions (OpenML 1597) -> replay as a stream -> score day 2 -> tests

download:   ## 284,807 transactions, 73 MB Parquet, checked against a pinned sha256
	mkdir -p data/raw
	[ -s data/raw/creditcard.pq ] || curl -sL --retry 5 --retry-all-errors -o data/raw/creditcard.pq $(URL)
	echo "$(SHA256)  data/raw/creditcard.pq" | sha256sum -c -

run:
	$(PY) -m stream.simulate --data data/raw/creditcard.pq --out output

test:
	$(PY) -m pytest -q

clean:
	rm -rf output/stream.duckdb

charts:                                            ## README charts -> docs/charts
	$(PY) charts/make_charts.py
