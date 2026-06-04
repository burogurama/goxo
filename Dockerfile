# The goxo base image: the engine binary as the entrypoint. Agent images extend
# it (FROM goxo:latest), add their handler, descriptor set, and definition, and
# set GOXO_HANDLER/GOXO_FDSET. It also ships the OXO healthcheck probe and
# symlinks ostorlab to it, so OXO's injected `ostorlab agent healthcheck`
# resolves to the probe — no Python ostorlab CLI in the image.
FROM golang:1.22-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /goxo . \
 && CGO_ENABLED=0 go build -o /healthcheck ./cmd/healthcheck

FROM debian:bookworm-slim
COPY --from=build /goxo /usr/local/bin/goxo
COPY --from=build /healthcheck /usr/local/bin/healthcheck
RUN ln -s healthcheck /usr/local/bin/ostorlab
EXPOSE 5000
ENTRYPOINT ["/usr/local/bin/goxo"]
