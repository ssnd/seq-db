# SeqQL

## Full-text Search

Full-text search in SeqQL enables filtering results based on document tokens. Queries can include exact phrases or
keywords separated by spaces. The behavior depends on the index type; more details on token formation can be found in
the [index types](docs/index.md) documentation. When performing a full-text search, the system automatically selects
results that match the specified text.

## Logical Operators

SeqQL supports logical operators for more precise search filtering. The operators `and`, `or`, and `not` allow combining
and refining queries:

- `and` — used when both conditions need to be true.
- `or` — returns results if at least one condition is met.
- `not` — excludes results that match a specific condition.

Examples:

```plaintext
message:error and level:critical
message:login or message:logout
not level:debug
```

## Wildcards

Wildcards in SeqQL allow for partial matching in search. The language supports the following symbols:

- `*` — replaces any number of characters.

These symbols can be used to search within tokens or parts of tokens. For example, a query on
the [keyword](index.md#keyword) index `source_type:access*` will match all documents starting with `access`.

## Range

SeqQL enables range filtering to limit data by value, particularly useful for numeric fields such as sizes or
timestamps. Ranges are specified using square or round brackets:

- `[a, b]` — includes both ends of the range.
- `(a, b)` — excludes both ends of the range.

Example of range usage:

```plaintext
bytes:(100, 1000]
bytes:[100, 1000)
```

## Pipes

In SeqQL, pipes are used to sequentially process data after the [primary filtering](#full-text-search) stage. Pipes
enable data transformation, enrichment, filtering, aggregation, and formatting of results.

Pipes in SeqQL are divided into three main categories:

1. **Enrichment and Filtering Pipes**
2. **Aggregation Pipes**
3. **Formatting Pipes**

### Enrichment and Filtering Pipes

These pipes operate at the index level, adding new data or filtering information based on certain criteria. They enrich
the original records by creating new fields or modifying existing ones.

Examples:

- `eval` — calculates the value of an expression and creates a new field.
- `where` — filters data based on a specified condition.
- `limit` — limits the number of returned results.

#### where Pipe (in development)

The `where` pipe performs filtering at the index level. It is used to limit results to those that meet a specific
condition.

Example:

```plaintext
source_type:access* | eval KB=bytes/1024 | where KB>100
```

In this example, the `eval` pipe creates a `KB` field, and the `where` pipe limits the results to those with `KB`
greater than 100.

### Aggregation Pipes (in development)

Aggregation pipes are used for statistical operations and data aggregation. They enable counting, summing, averaging,
and other statistical calculations.

Examples:

- `stats` — aggregates data by the specified method (e.g., `count`, `sum`, `avg`).
- `topK` — finds the most frequent values.

#### count Pipe (in development)

The `count` pipe counts the number of records that meet a specified criterion and is often used with other aggregation
pipes to group data.

Example:

```plaintext
source_type:access* | stats count by user
```

In this example, the query counts records for each user.

### Formatting Pipes (in development)

Formatting pipes enable modifications to the final search result presentation after all filtering and aggregation
stages. They work with the final data, selecting or excluding fields, sorting, and organizing results.

#### delete Pipe (in development)

The `delete` pipe removes specified fields from the search result, which is useful when certain data is no longer
needed.

Example:

```plaintext
source_type:access* | delete timestamp
```

In this example, the `timestamp` field will be removed from the result.

#### fields Pipe (in development)

The `fields` pipe retains only the specified fields in the results, excluding all others. This minimizes the volume of
returned data and enhances readability.

Example:

```plaintext
source_type:access* | fields user, KB
```

Here, only the `user` and `KB` fields will be in the final result.

## Query Optimization Recommendations for SeqQL

Optimizing SeqQL queries enables efficient data processing and faster access to results, reducing system load. The
primary goal of optimization is to minimize the amount of data processed at each stage.

Below are key recommendations for optimizing SeqQL queries:

### Maximizing Filtering in the Primary Search Stage

In query construction, filter as much data as possible at the primary search level. Refine search criteria by specifying
fields such as `source_type`, `status`, or `date` to exclude irrelevant data and reduce its volume before processing
with pipes. For example, setting narrow value ranges like time intervals or specific index values,
e.g., `source_type:access* AND date:[2023-01-01, 2023-12-31]`, excludes irrelevant data before further processing.

### Reducing Returned Fields

Specify only the fields you need to reduce data volume and accelerate query processing. Use the `fields` formatting pipe
to select only important fields:

```plaintext
source_type:access* | eval KB=bytes/1024 | where KB > 100 | fields KB, status, timestamp
```

In this example, the final result includes only `KB`, `status`, and `timestamp`, conserving memory and reducing output
time.

### Limiting the Number of Records

If you only need a specific number of records, use `limit` at the filtering stage. This stops processing after reaching
the required number of records, saving resources.

Example using `limit` to avoid processing all available data:

```plaintext
source_type:access* | where status="200" | limit 100
```

This query returns only the first 100 records that meet the conditions, reducing query processing time.
