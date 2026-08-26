# Data conversion

Temporal lets you install custom [data converters](https://docs.temporal.io/dataconversion) — for encryption, compression, or custom formats. How they interact with this package:

## Payload codecs: fully supported

Byte-level `PayloadCodec`s (encryption, compression) operate below this package and pass through untouched. Install them on both client and worker as usual; every surface round-trips:

- workflow arguments and results
- signals (deferred completions, mailbox offers)
- queries (state cells, deferred state)
- update payloads and responses
- typed-failure details riding `ApplicationFailure`s

The test suite pins an end-to-end "encryption" codec across all of these.

```ts
const dataConverter = { payloadCodecs: [new MyEncryptionCodec()] };
// give it to both Client and Worker options
```

## Payload converters: keep JSON handling

Format-level `PayloadConverter`s must keep JSON handling (the standard composite-converter pattern): every payload this package produces is **schema-encoded JSON**, so a JSON-less converter cannot carry these workflows. Adding converters for your own types alongside the JSON one is fine — they simply won't see this package's payloads.

## Where the schemas do the work

Because every boundary value is schema-encoded before it reaches Temporal's converter, the rich-type story lives in your schemas, not the converter: `DateTime`, branded types, unions, `Redacted` — declare them in the definition and they round-trip typed. You rarely need a custom payload converter at all.
