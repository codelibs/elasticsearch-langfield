Elasticsearch LangField Plugin
=======================

## Overview

LangField Plugin provides a useful feature for multi-language enrivonment.

## Version

[Versions in Maven Repository](http://central.maven.org/maven2/org/codelibs/elasticsearch-langfield/)

### Issues/Questions

Please file an [issue](https://github.com/codelibs/elasticsearch-langfield/issues "issue").
(Japanese forum is [here](https://github.com/codelibs/codelibs-ja-forum "here").)

## Installation

### For 5.x

    $ $ES_HOME/bin/elasticsearch-plugin install org.codelibs:elasticsearch-langfield:5.3.0

### For 2.x

    $ $ES_HOME/bin/plugin install org.codelibs/elasticsearch-langfield/2.4.1

## Getting Started

### Create Index for Multi-Languages

First, you need to create index which has fields for multi-languages:

    $ curl -XPUT 'localhost:9200/my_index' -d '
    {
      "mappings" : {
        "my_type" : {
          "properties" : {
            "message" : {
              "type" : "langstring"
            },
            "message_en" : {
              "type" : "string"
            },
            "message_ja" : {
              "type" : "string"
            }
          }
        }
      },
      "settings" : {
        "index" : {
          "number_of_shards" : "5",
          "number_of_replicas" : "0"
        }
      }
    }'

where message\_\* field is for multi-language.
Using message field with "langstring" type, message\_\* fields are stored automaticaly.
The above index-setting JSON is just an example. 
It's better to specify a proper analyzer for message\_\* field.

### Insert Documents

Insert 2 documents for English and Japanese:

    $ curl -XPOST "localhost:9200/my_index/my_type/1" -d '{
      "message":"This is a pen."
    }'
    $ curl -XPOST "localhost:9200/my_index/my_type/2" -d '{
      "message":"これはペンです。"
    }'

message field detects language and then copy the content of message field to a proper message\_\* field.
Check the result with exists filter query:

    $ curl -XPOST "http://localhost:9200/my_index/my_type/_search" -d'
    {
      "query": {
        "filtered": {
          "query": {
            "match_all": {}
          },
          "filter": {
            "exists": {
              "field": "message_en"
            }
          }
        }
      }
    }'
    {
      "took": 3,
      "timed_out": false,
      "_shards": {
        "total": 5,
        "successful": 5,
        "failed": 0
      },
      "hits": {
        "total": 1,
        "max_score": 1,
        "hits": [
          {
            "_index": "my_index",
            "_type": "my_type",
            "_id": "1",
            "_score": 1,
            "_source": {
              "message": "This is a pen."
            }
          }
        ]
      }
    }

Next, check if message_ja field exists:

    $ curl -XPOST "http://localhost:9200/my_index/my_type/_search" -d'
    {
      "query": {
        "filtered": {
          "query": {
            "match_all": {}
          },
          "filter": {
            "exists": {
              "field": "message_ja"
            }
          }
        }
      }
    }'
    {
      "took": 2,
      "timed_out": false,
      "_shards": {
        "total": 5,
        "successful": 5,
        "failed": 0
      },
      "hits": {
        "total": 1,
        "max_score": 1,
        "hits": [
          {
            "_index": "my_index",
            "_type": "my_type",
            "_id": "2",
            "_score": 1,
            "_source": {
              "message": "これはペンです。"
            }
          }
        ]
      }
    }
