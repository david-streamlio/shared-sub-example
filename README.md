# How to Run

1. Start Apache Pulsar using Docker

```bash
 docker run --name pulsar -it \
-p 6650:6650 \
-p 8080:8080 \
--mount source=pulsardata,target=/pulsar/data \
--mount source=pulsarconf,target=/pulsar/conf \
apachepulsar/pulsar:4.0.4 \
bin/pulsar standalone
```

2. If you want to run the asyncio version, then use the following command.

```bash
 poetry run hypercorn shared_sub.asyncio:app --reload
```

3. If you want to run the process version, then use the following command.

```bash
poetry run python shared_sub/process.py
```
