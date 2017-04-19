# Hildebrand [![Build Status](https://travis-ci.org/nervous-systems/hildebrand.svg?branch=master)](https://travis-ci.org/nervous-systems/hildebrand)

[![Clojars Project](http://clojars.org/io.nervous/hildebrand/latest-version.svg)](http://clojars.org/io.nervous/hildebrand)

Hildebrand is a high-level client for Amazon's [Dynamo
DB](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Introduction.html),
built on top of [Eulalie](https://github.com/nervous-systems/eulalie).

 - [core.async](https://github.com/clojure/core.async)-based API
 - Targets both Clojure and Clojurescript/Node
 - Survives the [Google Closure compiler's](https://developers.google.com/closure/compiler/) `:advanced` optimizations, for e.g. [Clojurescript AWS Lambda functions](https://github.com/nervous-systems/cljs-lambda)
 - Exposes advanced Dynamo features, including the [Dynamo Streams](https://github.com/nervous-systems/hildebrand/wiki/hildebrand.streams.channeled#get-records) service
 - Plain EDN representations of Dynamo tables, items, queries, and their components: [conditional writes](https://github.com/nervous-systems/hildebrand/wiki/Conditional-Operations), [atomic updates](https://github.com/nervous-systems/hildebrand/wiki/hildebrand.core#examples-4), [filters](https://github.com/nervous-systems/hildebrand/wiki/hildebrand.channeled#examples), and so on.

## Documentation

- The [API
introduction](https://github.com/nervous-systems/hildebrand/wiki/)
on the wiki is a good place to start.
- [Introducing Hildebrand](
https://nervous.io/clojure/aws/dynamo/hildebrand/2015/06/01/hildebrand/), a blog
post, has a bunch of usage examples in it.  The namespace layout has
changed since (`hildebrand` -> `hildebrand.core`)

## Examples

### [Querying](https://github.com/nervous-systems/hildebrand/wiki/hildebrand.channeled#query)

```clojure
(require '[hildebrand.channeled :refer [query!]])

(async/into []
  (query! creds :games
          {:user-id [:= "moea"]}
          {:filter [:< [:score] 50]
           :sort :desc
           :limit 10}
          {:chan (async/chan 10 (map :score))}))
;; => [15 10]
```

### [Querying](https://github.com/nervous-systems/hildebrand/wiki/hildebrand.channeled#query) + [Batched Deletes](https://github.com/nervous-systems/hildebrand/wiki/hildebrand.channeled#batching-deletes)

```clojure
(require '[hildebrand.channeled :refer [query! batching-deletes]])

(let [[results errors]
      (->> (query! creds :games
                   {:user-id [:= "moea"]
                    :game-title [:begins-with "Super"]}
                   {:filter [:< [:score] 100]
                    :limit  100})
           (async/split map?))
      {delete-chan :in-chan} (batching-deletes creds {:table :games})]
  (async/pipe results delete-chan))
```

###


# Clojurescript

All of the functionality (barring the synchronous convenience functions) is
exposed via Clojurescript.  The implementation specifically targets
[Node](https://nodejs.org/), and uses
[lein-npm](https://github.com/RyanMcG/lein-npm) for declaring its dependency on
[bignumber.js](https://github.com/MikeMcl/bignumber.js/).  The wiki contains [more information about number
handling](https://github.com/nervous-systems/hildebrand/wiki#numbers), which is
the only substantial difference from the Clojure implementation.

The specific use-case I had in mind for Node support is [writing AWS Lambda
functions in
Clojurescript](https://nervous.io/clojure/clojurescript/aws/lambda/node/lein/2015/07/05/lambda/).

See the [Eulalie
README](https://github.com/nervous-systems/eulalie#clojurescript) for other
Node-relevant details.

# Development

Most of the integration tests expect an instance of [DynamoDB
Local](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.html).
If the `LOCAL_DYNAMO_URL` environment variable isn't set, those tests will be
skipped.

A couple of the tests expect to get capacity information back from Dynamo, and
so can't run against a local instance.  If `AWS_ACCESS_KEY` and `AWS_SECRET_KEY`
are set, these tests'll try to connect and interact with a table (in Dynamo's
default region, `us-east-1`).

Assuming a local Node install, `lein cljsbuild once test-none` will run the
Clojurescript tests.  `test-advanced` will run the tests under `:optimizations`
`:advanced`.

[Contributions welcomed](https://github.com/nervous-systems/hildebrand/issues).

# See Also
 * [Faraday](https://github.com/ptaoussanis/faraday) - An excellent, synchronous Dynamo client built on the official AWS client library.  Hildebrand's approach to type handling was largely copied from Faraday.

# License

hildebrand is free and unencumbered public domain software. For more
information, see http://unlicense.org/ or the accompanying UNLICENSE
file.
