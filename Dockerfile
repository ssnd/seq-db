FROM gitlab-registry.***REMOVED***/docker/ubuntu-minbase:jammy

WORKDIR /seq-db

COPY ./bin/linux-amd64/seq-db ./bin/linux-amd64/distribution ./
COPY ./tests/data/mappings/logging-new.yaml ./default.yaml

ENTRYPOINT ["./seq-db"]
