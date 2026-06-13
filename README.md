# image-harvester

Parallel harvester for public-domain document images served over the
[IIIF](https://iiif.io) Presentation/Image APIs.

Each shard reads a slice of a work list, fetches manifest + page images through a
configurable edge proxy, transcodes to WebP (libvips, Pillow fallback), and
writes to S3-compatible object storage. Runs are idempotent — a `.done` marker
per item is checked before work starts, so re-runs skip finished items.

## Usage

```
python worker.py --id <id> --manifest <manifest-url> --title "<title>"   # single item
python worker.py --shard <n> --total <m>                                 # sharded batch
```

## Configuration (environment)

| var | meaning |
|-----|---------|
| `EDGE_PROXY` | prefix for proxied fetches (`.../fetch?url=`) |
| `R2_ENDPOINT` / `R2_KEY` / `R2_SECRET` | S3-compatible object store |
| `CF_ACCOUNT` / `CF_D1_DB` / `CF_D1_TOKEN` | index database |
| `LIST_BUCKET` / `LIST_KEY` | work list location |
| `DST_BUCKET` | destination bucket |
| `LIB_NAME` | collection label |

Only public-domain / no-known-copyright material. Polite rate via the edge proxy.
