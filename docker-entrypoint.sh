#!/bin/sh
# Fetch steamclient + libsteamwebrtc (Steamworks SDK Redist, app 1007) into the state volume on
# first start. They're never baked into the image.
set -eu

SDK64="$HOME/.steam/sdk64"
if [ ! -f "$SDK64/steamclient.so" ] || [ ! -f "$SDK64/libsteamwebrtc.so" ]; then
    echo "fetching Steamworks SDK redistributable..."
    mkdir -p "$HOME/steamcmd" "$SDK64"
    curl -fsSL https://steamcdn-a.akamaihd.net/client/installer/steamcmd_linux.tar.gz \
        | tar -xz -C "$HOME/steamcmd"
    "$HOME/steamcmd/steamcmd.sh" +force_install_dir "$HOME/sdkredist" \
        +login anonymous +app_update 1007 validate +quit
    cp "$HOME/sdkredist/linux64/steamclient.so" "$HOME/sdkredist/linux64/libsteamwebrtc.so" "$SDK64/"
fi

exec ss13relay "$@"
