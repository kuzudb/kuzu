## Build

```
docker build -t kuzu-self-hosted-benchmark-runner .
```

## Start Container

```
docker run  --name self-hosted-benchmark-runner-<X> --detach --restart=always\
            -e GITHUB_ACCESS_TOKEN=<YOUR_GITHUB_ACCESS_TOKEN>\
            -e MACHINE_NAME=<NAME_OF_THE_PHYSICAL_MACHINE>\
            -e JWT_TOKEN=<YOUR_JWT_TOKEN>\
            -e BENCHMARK_SERVER_URL=http://<YOUR_SERVER_ADDRESS>/api/post_results\
            -v <PATH_TO_RAW_CSV_FILES>:/csv\
            -v <PATH_TO_SERIALIZED_DATASET>:/serialized\
            --memory=<MEMORY_LIMIT> --cpuset-cpus=<RANGE_OF_CPU_CORES>\
            kuzu-self-hosted-benchmark-runner
```
