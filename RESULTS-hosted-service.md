# RESULTS: hosted service

**Not run. Skipped deliberately.**

The fourth target was to be a hosted queue or KV service, reachable without
credentials and without payment.

No such service was used, because every candidate I could identify requires
creating something before it will accept a write:

- Cloudflare Workers KV, Deta, Upstash, Vercel KV, DynamoDB, Firestore: all
  require an account and an API token.
- The "no signup" KV services I am aware of (kvdb.io and similar) still require
  creating a bucket or a token first, which is account creation by another name.

The task's constraints are explicit: do not sign up for anything, do not enter
credentials, do not create accounts. Creating a bucket or minting a token to get
a usable endpoint falls inside that prohibition, so this target was skipped
rather than worked around.

There is also a second reason it would have been a weak target even if a
credential-free endpoint existed. lincheck's value comes from fault injection
against processes it controls: killing a node, freezing it, cutting it off from
its peers. Against a hosted service none of that is available. The run would
reduce to a concurrency soak with no faults, which the harness would report as a
fault set of `(none)` and which proves very little. The interesting question
about a hosted KV is what it does during a failover, and a black-box client
cannot cause one.

If a credential-free endpoint were available, the adapter needed would be small:
`Setup` a no-op, `Clients` returning HTTP clients, `Faults` returning an empty
slice, and `DocumentedModel` quoting the vendor's consistency page. That is the
point of keeping the adapter interface to eight methods.
