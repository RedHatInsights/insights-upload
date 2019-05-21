import logging
import os

from prometheus_client import Counter, Summary, Gauge, generate_latest # noqa

# Prometheus Counters
uploads_total = Counter('uploads_total', 'The total amount of uploads')
uploads_valid = Counter('uploads_valid', 'The total amount of valid uploads')
uploads_validated = Counter('uploads_validated_success', 'The total amount of successfully validated uploads')
uploads_invalid = Counter('uploads_invalid', 'Thte total number of invalid uploads')
uploads_invalidated = Counter('uploads_validated_failure', 'The total amount of uploads invalidated by services')
uploads_too_large = Counter('uploads_too_large', 'The total amount of uploads great than max_length')
uploads_unsupported_filetype = Counter('uploads_unsupported_filetype', 'The total amount of uploads not matching mimetype regex')
uploads_handed_off = Counter('uploads_handed_off', 'The total number of uploads handed off')
uploads_produced_to_topic = Counter('uploads_produced_to_queue', 'Total number of messages pushed to the produce_queue for given topic.', ['topic'])
uploads_popped_to_topic = Counter('uploads_popped_to_queue', 'Total number of messages popped from the produce_queue for given topic.', ['topic'])
uploads_file_field = Counter('uploads_file_field', 'Total number of payloads recieved using which form field', ['field'])

# Prometheus Summaries
uploads_write_tarfile = Summary('uploads_write_tarfile_seconds', 'Total seconds it takes to write the tarfile upon upload')
uploads_post_time = Summary('uploads_total_post_seconds', 'Total time it takes to post to upload service')
uploads_process_file_seconds = Summary('uploads_process_file_seconds', 'Total time to handle files once validated by end service')
uploads_send_and_wait_seconds = Summary('uploads_send_and_wait_seconds', 'Total time spend in send_and_wait')
uploads_json_loads = Summary("uploads_json_loads", "Time spent executing json.loads", ["key"])
uploads_json_dumps = Summary("uploads_json_dumps", "Time spent executing json.dumps", ["key"])
uploads_run_in_executor = Summary("uploads_run_in_executor", "Time spent waiting on executor", ["function"])
uploads_httpclient_fetch_seconds = Summary("uploads_httpclient_fetch_seconds", "Time spent waiting on httpclient fetch", ["url"])

# Non Async Functions
uploads_s3_copy_seconds = Summary('uploads_s3_copy_seconds', 'Total time to copy a file from bucket to bucket')
uploads_s3_write_seconds = Summary('uploads_s3_write_seconds', 'Total time to write to a bucket')
uploads_s3_ls_seconds = Summary('uploads_s3_ls_seconds', 'Total time to list a file in S3')
uploads_s3_get_url_seconds = Summary('upload_s3_get_url_seconds', 'Total time to get a presigned url')

# threadpool metrics
uploads_executor_qsize = Gauge("uploads_executor_qsize", "Approximate number of items in the executor queue")
uploads_produce_queue_size = Gauge("uploads_produce_queue_size", "Number of items in the produce queue")

logger = logging.getLogger(__name__)
