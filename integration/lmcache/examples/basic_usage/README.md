# Basic Usage

Use `NativeIOEngine` standalone, without LMCache.

## What's Covered

- Sync single-key read/write/exists
- Sync batch operations
- Async single-key and batch operations with asyncio
- Concurrent async writes via `asyncio.gather`

## Prerequisites

```bash
cd integration/lmcache
pip install .
```

## Run

```bash
python examples/basic_usage/basic_usage.py
```

## Expected Output

```
[sync]  Single key read/write ... OK
[sync]  Exists check ........... OK
[sync]  Batch operations ....... OK
[async] Single key read/write .. OK
[async] Concurrent writes ...... OK
[async] Batch read ............. OK

All examples passed!
```
