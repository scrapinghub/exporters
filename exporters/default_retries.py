from retrying import retry


__all__ = ['retry_short', 'retry_long']


# for operations that shouldn't take longer than a few seconds (e.g. HTTP request)
# will retry after 2s, 4s, 8s, 10s, 10s, 10s ... until the 10th attempt
retry_short = retry(
    wait_exponential_multiplier=1000,
    wait_exponential_max=10000,
    stop_max_attempt_number=10,
)

# for operations that take minutes to finish (e.g. uploading a file)
# will retry after 10s, 20s, 40s, 80s, 2m, ~5m, ~10m, ~20m and then give up.
retry_long = retry(
    wait_exponential_multiplier=5000,
    stop_max_attempt_number=8,
)
