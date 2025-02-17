---
id: index-types
---

# Index types and mappings

## Index types

There are several index types with different behavior.

### Keyword

Usually used for content like statuses, namespaces, trace IDs, tags or any other data where a full match search is required.

Example of a mapping for a keyword field:

```yaml
mapping-list:
  - name: level
    type: keyword
```

### Path

Used for searching by beginning of a path or full path. For example, the following documents will match query `uri:"/my/path"`:

```json
[
  {"uri:/my/path"},
  {"uri:/my/path/1"},
  {"uri:/my/path/1/"},
  {"uri:/my/path/1/one"},
]
```

Example of a mapping for a path field:

```yaml
mapping-list:
  - name: uri
    type: path
```

### Text

Index for content like error messages or request bodies where full-text search is required.

For example, the search query `message:"error"` will emit all the documents that contain token `error`. And query `message:"error code"` will emit all the documents that contain both `error` and `code`.

Example of a mapping for a text field:

```yaml
mapping-list:
  - name: message
    type: text
```

### Exists

Used when the presence of the field is important and not the value.

For example, for query `_exists_:service` all documents that have a `service` field will be found regardless of the field value.

Example of a mapping for an exists field:

```yaml
mapping-list:
  - name: service
    type: exists
```

## Configuration parameters

* `partial-indexing` - if true, will index only the first part of long fields, otherwise will skip entry if length of field value is greater than threshold.

* `max-token-size` - max size of a single token, default is 72.

* `case-sensitive` - if false, will convert values to lower case.

* constant `consts.MaxTextFieldValueLength` - limits maximum length of the text field value. Current threshold is 32768 bytes.

## Mappings

Mapping with explicit field types: `text`, `keyword`, `path`, `exists`.

### Mapping example

```yaml
mapping-list:
    - name: message
      types:
      - type: text
      - title: keyword
        type: keyword
        size: 18
    - name: level
      type: keyword
    - name: foo
      type: exists
    - name: bar
      type: keyword
    - name: uri
      type: path
```

There is also a `size` field that allows you to specify the maximum size of the input data.

If `size` is not set, the [default](#configuration-parameters) will be used.

## Multiple indexes on single field

There can be multiple index types for single field:

```yaml
mapping-list:
    - name: message
      types:
      - type: text
      - title: keyword
        type: keyword
        size: 18
```

In this case, the type with empty `title` will be default one.

For types with `title` new "implicit" fields will be created: `message.keyword` in our case. Use `message.keyword` to search over message as keyword and `message` to search as text.

The title of implicit field consists of values of `name` and `title` joined together with a dot between them.

## Indexing internals

Let's write a document using [mapping](#mapping-example) (ID of document will be ```id = N```):

```json
{
    "message": "hello world",
    "some_number": 1,
    "level": "info",
    "foo": "aaa bbb ccc",
    "bar": "ddd eee fff",
    "uri": "/my/path"
}
```

Following meta data will be created:

```json
{
    id: N,
    tokens: [
        "_exists_:message",
        "message:hello",
        "message:world",
        "_exists_:message.keyword",
        "message.keyword:hello world",
        "_exists_:level",
        "level:info",
        "_exists_:foo",
        "_exists_:bar",
        "bar:ddd eee fff",
        "_exists_:uri",
        "uri:/my",
        "uri:/my/path",
    ]
}
```

Note that:

* There are additional `_exists_` tokens for each field.
* There are tokens for `message.keyword` field, which wasn't present in the original document, but was in mapping as "implicit" type for `message` field.
* `foo` field has only `_exists_` token.
* Data in the `bar` field indexed as single token because it is `keyword` field.
* There are multiple tokens for `uri` field, a token for each segment of the path.
* Field `some_number` is not indexed, because it is not in the mapping.
