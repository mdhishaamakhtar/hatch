# Builds the image for one command in cmd/: docker build --build-arg CMD=api .
FROM golang:1.26-alpine AS build
ARG CMD
WORKDIR /src
COPY . .
# The caches are shared by every image's build, so building them all compiles
# the common code once.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/app ./cmd/${CMD}

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
ENTRYPOINT ["/app"]
