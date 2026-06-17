# Builds the tpuf-node query server as a small static binary. Used by the node0,
# node1, node2 services in docker-compose's "lb" profile.
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /tpuf-node ./cmd/tpuf-node

FROM gcr.io/distroless/static-debian12
COPY --from=build /tpuf-node /tpuf-node
EXPOSE 8080
ENTRYPOINT ["/tpuf-node"]
