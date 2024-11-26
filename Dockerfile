FROM gitlab-registry.***REMOVED***/docker/ubuntu-minbase:bionic

WORKDIR /seq-db

COPY ./bin/amd64/seq-db ./bin/amd64/distribution ./

ENTRYPOINT ["./seq-db"]
