# snout-realtime's image: one static binary, nothing else.
#
#   podman build -f realtime/Containerfile -t snout-realtime .
#
# The context is the stack workspace root, for its lockfile (in the SnoutData monorepo,
# `packages/stack`). FROM scratch: the server needs no shell, no certificates (it speaks plain
# TCP to the project databases on the host's network) and no libc beyond what musl links in.
FROM docker.io/library/rust:1.98.1-alpine AS build
RUN apk add --no-cache musl-dev
WORKDIR /src
COPY . .
RUN cargo build --locked --release -p snout-realtime \
	&& cp target/release/snout-realtime /snout-realtime

FROM scratch
COPY --from=build /snout-realtime /snout-realtime
USER 1000:1000
EXPOSE 4000
ENTRYPOINT ["/snout-realtime"]
