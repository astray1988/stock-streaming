# Dev environment set up
## For MacOS

1. Create a docker VM with 2 cpus and 2G memory.

```
docker-machine create --driver virtualbox --virtualbox-cpu-count 2 --virtualbox-memory 2048 stock
```

2. Running shell to start docker containers(Kafka, Cassandra, Redis, Zookeeper)

```
./local-setup.sh stock
```