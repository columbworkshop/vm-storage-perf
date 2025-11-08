FROM golang:1.25.1-alpine3.22 AS build-env
WORKDIR /app
COPY . .
RUN go mod download
RUN go build -o cx-ceph-rgw-exporter .

FROM alpine:3.22
WORKDIR /app
COPY --from=build-env /app/cx-ceph-rgw-exporter .
EXPOSE 9290
CMD ["./cx-ceph-rgw-exporter"]