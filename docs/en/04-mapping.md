---
id: mapping
---

# How we index fields
seq-db doesn't index any fields from the ingested data by default.
Instead, indexing is controlled through a special file called the *mapping file*. 
The mapping file specifies the indexed fields and the used index types. 


## Available mapping types
Below is a description of mapping types seq-db currently supports. 
Note that most of these mapping types can be used with 

### `text` mapping type
This mapping type is used to index fields containing unstructured, natural language text,
providing full-text search capabilities later on. It should be used with human-written messages, descriptions, 
error messages and other free-text fields. 

Example mapping:
```yaml
mapping-list:
# other fields...
- name: "message"
  type: "text"
```


### `keyword` mapping type
The `keyword` mapping type treats the whole value as a single token, without breaking it up. 
It should be used for usernames, ids and categorical data.

Note that `keyword` index should be used with care with high-cardinality values like `trace_id`, `span_id` or 
other unique request identifiers, as the indexing of these fields might blow up the index.

Example mapping:
```yaml
mapping-list:
  # other fields...
  - name: "k8s_pod"
    type: "keyword"
```


### `path` mapping type

This mapping type indexes hierarchical path-like values. 
It  is very similar to the elasticsearch's [path tokenizer](https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-pathhierarchy-tokenizer.html).
When a field is indexed with the `path` index, its value is broken 
into hierarchical terms. 
For example, this input document:
```json
{"path_indexed": "/var/log/app/error.log"}
```

Would be broken into (roughly) the following terms:
```json
["/var", "/var/log", "/var/log/app", "/var/log/app/error.log"]
```

This way, if there are multiple logs with `path_indexed` key that start with `/var/log`, 
these messages would be easily matched by a simple `path_indexed: /var/log` query, without using wildcards.

Example mapping:
```yaml
mapping-list:
  # other fields...
  - name: "request_uri"
    type: "keyword"
```

### `exists` mapping type
Indexes the presence of a field, disregarding its value. 
This mapping type should be used when a field might or might not be
present in a message.
The messages that have this field can be found later on with the `_exists_:key` search query.
Note that the `_exists_` query will work on other types of mapping as well. 

Example mapping:
```yaml
mapping-list:
  # other fields...
  - name: "sparse_field"
    type: "exists"
```

## Object indexing
seq-db can also index logs containing nested structured data. 
In this case, the parent field should have the `object` index type, 
and contain a `mapping-list` object inside, that would specify 
how exactly its nested fields should be indexed.

For example:
```yaml
mapping-list:
- name: "myobject" # key that contains nested json data
  type: "object"
  mapping-list: # mapping for the nested fields
  - type: "keyword"
    name: "nested"
  - type:  "text"
    name: "nestedtext"
```

## Multiple indexing types
A single field can be indexed with multiple types at the same time. 
This allows to combine multiple indexing strategies enabling more flexible
search capabilities.

For instance, a field can be indexed as both `keyword` and `text`, 
allowing both full-text search and exact filtering. 


If multiple indexing types are used, the additional indexing 
type should have titles, that will be used during data search.
Say we have this mapping:
```yaml  
- name: "message"
  types:
  - type: "text"
  - title: "keyword"
    type: "keyword"
```

In this case, the type with an empty `title` will be considered the primary one.
For types with a specified `title`, new fields will be created,
with names consisting of the values of the name and title fields,
joined by a dot.
In the example provided, this would be `message.keyword`. 
See the complete example below for a more detailed description 
of how this message would be indexed.


## Illustrated mapping example 
Let's walk through a practical example. 
We'll define a mapping and analyze how seq-db would index a sample document.

Say we have this example mapping:
```yaml
mapping-list:
    - name: "message"
      types:
      - type: "text"
      - title: "keyword"
        type: "keyword"
        size: 18
    - name: level
      type: keyword
    - name: foo
      type: keyword
    - name: uri
      type: path
```

And we ingest the following document (with id = N):
```json
{
    "message": "hello world",
    "some_number": 1,
    "level": "info",
    "foo": "aaa bbb ccc",
    "uri": "/my/path"
}
```

The document will be indexed as follows:
```json5
{
    "id": "N",
    "tokens": [
        "message:hello", // primary index type
        "message:world",
        "message.keyword:hello world", // secondary index type
        "level:info",
        "foo:aaa bbb ccc",
        "uri:/my",
        "uri:/my/path"
    ]
}
```

Note that:
- We see tokens for the field `message.keyword` even though this field wasn't present in the original document.
These tokens were generated because the message field was indexed with both `text` (primary) and `keyword` (with the `title` set to `keyword`).
- The field `foo` is indexed as a single token: `"foo:aaa bbb ccc"`.
Since it's defined as a keyword, the entire value is indexed rather than being split into individual words.
- The `uri` field, being indexed as a path, is split into multiple tokens representing different levels of the path hierarchy.
- The `some_number` field was not indexed because it was not mentioned in the mapping.
