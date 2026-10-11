FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build --release -p ss13relay \
	&& mkdir /out \
	&& cp target/release/ss13relay /out/ \
	&& cp "$(find target/release/build -name libsteam_api.so | head -1)" /out/

FROM debian:bookworm-slim

RUN apt-get update \
	&& apt-get install -y --no-install-recommends ca-certificates curl lib32gcc-s1 \
	&& rm -rf /var/lib/apt/lists/* \
	&& useradd --system --home-dir /var/lib/ss13relay --create-home ss13relay
COPY --from=build /out/ss13relay /usr/local/bin/
COPY --from=build /out/libsteam_api.so /usr/local/lib/
COPY docker-entrypoint.sh /usr/local/bin/
RUN ldconfig
USER ss13relay
ENV HOME=/var/lib/ss13relay
VOLUME /var/lib/ss13relay
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
