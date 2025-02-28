---
id: seq-ql
---

# SeqQL

## Full-text Search

Full-text search in SeqQL allows filtering results based on document tokens.
Queries can include exact phrases or keywords separated by spaces.
The behavior depends on the index type; more details on token formation can be found in
the [index types](index-types) documentation.
When performing a full-text search, the system automatically selects results that match the specified text.

Search queries are case-insensitive by default.
To change this behavior, use the `--case-sensitive` flag, but it affects only new documents.

## Logical Operators

SeqQL supports logical operators for more precise search filtering.
The operators `and`, `or`, and `not` allow combining and refining queries:

- `and` — requires both conditions to be true.
- `or` — returns results if at least one condition is met.
- `not` — excludes results that match a specific condition.

Examples:

```seq-ql
message:error and level:critical
message:login or message:logout
not level:debug
```

## Wildcards

Wildcards in SeqQL allow for partial matching in search.
The language supports the following symbols:

- `*` — replaces any number of characters.

These symbols can be used to search within tokens or parts of tokens.
For example, a query on the [keyword](index-types#keyword) index `source_type:access*` will match all documents starting
with `access`.

## Filter `range`

SeqQL enables range filtering to limit data by value, which is particularly useful for numeric fields such as sizes or
timestamps.
Ranges are specified using square or round brackets:

- `[a, b]` — includes both ends of the range.
- `(a, b)` — excludes both ends of the range.

Example of range usage:

```seq-ql
bytes:(100, 1000]
bytes:[100, 1000)
```

## Filter `in`

SeqQL allows using the `in` filter to filter a list of tokens.
The syntax for full-text search is the same as in [full-text search](#full-text-search) and supports all types of string
literals.
Search tokens must be separated by commas.

Example usage of `in`:

```seq-ql
level:error and k8s_namespace:in(default, kube-system) and k8s_pod:in(kube-proxy-*, kube-apiserver-*, kube-scheduler-*)

trace_id:in(123e4567-e89b-12d3-a456-426655440000, '123e4567-e89b-12d3-a456-426655440001', "123e4567-e89b-12d3-a456-426655440002",`123e4567-e89b-12d3-a456-426655440003`)
```

## Pipes

Pipes in SeqQL are used to sequentially process data.
Pipes enable data transformation, enrichment, filtering, aggregation, and formatting of results.

#### `fields` pipe

The `fields` pipe allows removing unnecessary fields from documents in the output.
This is useful when only specific fields are needed, especially when exporting large datasets to reduce network load.

The listed fields may not be present in the [mapping](mapping), and their absence does not affect the performance of
the `fields` pipe.

Example:

```seq-ql
source_type:access* | fields user, KB
```

In the final result, each document will contain only the `user` and `KB` fields.

To remove only specific fields, use the `except` keyword:

```seq-ql
source_type:access* | fields except payload, cookies
```

In this example, the `payload` and `cookies` fields will be excluded from the result.

## Query Optimization Recommendations for SeqQL

Optimizing SeqQL queries enables efficient data processing and faster access to results, reducing system load.
The primary goal of optimization is to minimize the amount of data processed at each stage.

### Maximizing Filtering in the Primary Search Stage

Filter as much data as possible at the primary search level.
Refine search criteria by specifying fields such as `source_type`, `status`, or `date` to exclude irrelevant data and
reduce its volume before processing with pipes.
For example, setting narrow value ranges like time intervals or specific index values,
e.g., `source_type:access* AND date:[2023-01-01, 2023-12-31]`, excludes irrelevant data before further processing.

### Reducing Returned Fields

Specify only the fields you need to reduce data volume and accelerate query processing.
Use the `fields` formatting pipe to select only important fields:

```seq-ql
source_type:access* | fields KB, status, timestamp
```

In this example, the final result includes only `KB`, `status`, and `timestamp`, conserving memory and reducing output
time.
