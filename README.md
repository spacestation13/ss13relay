# ss13relay

A sidecar for Space Station 13 servers that lets players using the Steam build of the SS13 launcher connect over [Steam Datagram Relay](https://partner.steamgames.com/doc/features/multiplayer/steamdatagramrelay), Valve's relay network, which can improve their routing to the server.

The relay logs into Steam as an anonymous game server, accepts Steam P2P connections from the launcher, and forwards each one to DreamDaemon as a normal TCP connection. Before every connection it announces itself to the server's [SS13Lib](../ss13lib), which recognises relayed players by the relay's address, requires them to authenticate with an SS13Hub ticket, and gets their real IP from the hub.

## Requirements

- The game server runs SS13Lib with relay support enabled. See SS13Lib's README, "Relay Support".
- Outbound UDP. No inbound ports are needed.
- amd64 Linux (Steam's `steamclient` is x86_64 only).

## Configuration

Environment variables (`SS13RELAY_*`) or a TOML file (`SS13RELAY_CONFIG`, default `/etc/ss13relay/config.toml`).

| Setting | Default |
|---|---|---|
| `targets` | required | DreamDaemon addresses: `127.0.0.1:1337`, or `dreamdaemon:1337` in Docker. Comma-separated in the environment variable. |
| `secret` | required | Shared with the server's `SS13LIB_RELAY_SECRET`. Any long random string, e.g. `openssl rand -hex 24` |
| `steam_app_id` | `5287320` | The relays can only serve users using the given app ID |
| `source_address` | `127.0.0.2` for loopback targets |
| `send_rate` | `1048576` | Bytes/s per connection, throughput collapses above 1.08 MB/s. |
| `ice` | `true` | Allow direct UDP routes alongside the relays; Steam picks whichever is faster. Shares the server's IP with players. |
| `gslt` | anonymous | A Steam game server login token, for a stable SteamID. |

`SS13RELAY_LOG` sets the log filter (default `info,steamnet=warn`).

## Running

Docker is the primary way to run it, see [`deploy/docker-compose.yml`](deploy/docker-compose.yml). On first start the container fetches Steam's redistributable libraries into its volume with SteamCMD.

## Development

- `crates/ss13relay`: the daemon.
- `crates/ss13relay-proto`: the wire protocol and Steam plumbing shared with the launcher.
- `crates/ss13relay-devclient`: a stand-in for the launcher's relay proxy. Run it with Steam open (`--relay <steamid> --port <port> [--app <id>]`), then point DreamSeeker at the address it prints.
