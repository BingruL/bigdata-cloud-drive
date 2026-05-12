.PHONY: start-full status-full stop-full scale-benchmark large-file-benchmark batch-size-benchmark plot-benchmarks

start-full:
	./scripts/start_full.sh

status-full:
	./scripts/status_full.sh

stop-full:
	./scripts/stop_full.sh

scale-benchmark:
	python scripts/scale_benchmark.py --files 100000 --logs 1000000 --logical-bytes 1GB --csv-out reports/scale-benchmark.csv

large-file-benchmark:
	python scripts/large_file_concurrency_benchmark.py --file-size 100MB --files-per-user 1 --concurrency 2 --csv-out reports/large-file-concurrency.csv

batch-size-benchmark:
	python scripts/batch_size_benchmark.py --rows 2000 --batch-sizes 1,10,100,500,1000,5000 --csv-out reports/batch-size-benchmark.csv

plot-benchmarks:
	python scripts/plot_benchmarks.py
