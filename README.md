# hildebrand [![Build Status](https://travis-ci.org/nervous-systems/hildebrand.svg?branch=master)](https://travis-ci.org/nervous-systems/hildebrand)

[![Clojars Project](http://clojars.org/io.nervous/hildebrand/latest-version.svg)](http://clojars.org/io.nervous/hildebrand)

Hildebrand is high-level, asynchronous client for [Amazon's Dynamo
DB](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Introduction.html).
Built on top of [eulalie](https://github.com/nervous-systems/eulalie), it works
with both Clojure and Clojurescript/Node.

Advanced Dynamo features, including the [DynamoDB Streams
API](http://aws.amazon.com/dynamodb/faqs/#triggers) are fully implemented.

## Documentation

- The [API
introduction](https://github.com/nervous-systems/hildebrand/wiki/API-Introduction)
on the wiki is a good place to start.
- [Introducing Hildebrand](
https://nervous.io/clojure/aws/dynamo/hildebrand/2015/06/08/hildebrand/), a blog
post, has a bunch of pre-1.0 usage examples in it - the namespace layout has
changed slightly, but the ideas are the same.

## Clojurescript

All of the functionality (barring the synchronous convenience functions) is
exposed via Clojurescript.  The implementation specifically targets
[Node](https://nodejs.org/), and uses
[lein-npm](https://github.com/RyanMcG/lein-npm) for declaring its dependencies.

The specific use-case I had in mind for Node support is [writing AWS Lambda
functions in
Clojurescript](https://nervous.io/clojure/clojurescript/aws/lambda/node/lein/2015/07/05/lambda/).  Please see the [eulalie README](https://github.com/nervous-systems/eulalie) for more details.

[Contributions welcomed](https://github.com/nervous-systems/hildebrand/issues).

## See Also
 * [Faraday](https://github.com/ptaoussanis/faraday) - An excellent, synchronous Dynamo client built on the official AWS client library.  Hildebrand's approach to type handling was largely copied from Faraday.

## License

hildebrand is free and unencumbered public domain software. For more
information, see http://unlicense.org/ or the accompanying UNLICENSE
file.
