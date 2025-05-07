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

2. Run the Application using hyercorn

```bash
 poetry run hypercorn shared_sub.main:app --reload
```

3. Publish messages to the topic

```bash
docker exec pulsar \
  bin/pulsar-client produce -n 10 -m "Foo bar" public/default/test
```