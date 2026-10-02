-- Query the hello-world service from SQL through the adbc_scanner extension.
--
--   export GRAINLIFT_DRIVER=/absolute/path/to/libadbc_driver_grainlift.dylib   # .so on Linux
--   uvx haybarn-cli < examples/query.sql      # Haybarn; the DuckDB CLI also works
--
-- The service must be running (go run .). It accepts anonymous clients; for a
-- token-protected service add 'grainlift.auth.bearer_token'.

INSTALL adbc_scanner FROM community;
LOAD adbc_scanner;

SET VARIABLE hello = adbc_connect({
    'driver': getenv('GRAINLIFT_DRIVER'),
    'entrypoint': 'AdbcDriverGrainliftInit',
    'grainlift.uri': 'http://127.0.0.1:8080',
    'grainlift.target': 'hello'
});

-- adbc_scan sends the quoted SQL to the service and returns Arrow batches to DuckDB.
SELECT * FROM adbc_scan(getvariable('hello')::BIGINT, 'SELECT ''Hello, world!'' AS message');

-- The results are ordinary DuckDB relations: aggregate, join or export them locally.
SELECT count(*) AS numbers, sum(number) AS total
FROM adbc_scan(getvariable('hello')::BIGINT, 'SELECT * FROM numbers(100000)');

SELECT *
FROM adbc_scan(getvariable('hello')::BIGINT, 'SELECT * FROM running_total(2500)')
ORDER BY number DESC
LIMIT 3;

CALL adbc_disconnect(getvariable('hello')::BIGINT);
