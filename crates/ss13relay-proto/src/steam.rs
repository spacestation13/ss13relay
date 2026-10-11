//! Steam networking settings both ends must agree on

use std::ffi::CStr;
use std::os::raw::c_char;
use std::time::{Duration, Instant};

use steamworks::networking_types::{
    NetworkingAvailability, NetworkingAvailabilityResult, NetworkingConfigEntry,
    NetworkingConfigValue,
};
use steamworks::sys;

/// Steam's relays cap a connection at roughly 1.08 MB/s and throughput collapses above that.
/// The networking library has no bandwidth estimation, so the rate is pinned (min = max) just under the cap.
pub const DEFAULT_SEND_RATE: i32 = 1024 * 1024;

/// Direct (ICE) candidate types to share: private and public addresses, no TURN relays.
const ICE_PRIVATE_AND_PUBLIC: i32 = 2 | 4;
const ICE_DISABLED: i32 = 0;

const SEND_BUFFER_SIZE: i32 = 4 * 1024 * 1024;

#[derive(Debug, Clone, Copy)]
pub struct LinkConfig {
    /// Bytes per second, applied as both the minimum and maximum send rate.
    pub send_rate: i32,
    /// Allow direct UDP routes alongside Steam's relays; Steam picks whichever pings better.
    /// Needs `libsteamwebrtc` beside `steamclient`, and shares IP candidates with the peer.
    pub ice: bool,
}

impl Default for LinkConfig {
    fn default() -> Self {
        Self {
            send_rate: DEFAULT_SEND_RATE,
            ice: true,
        }
    }
}

impl LinkConfig {
    /// Per-connection options, for listen sockets and outgoing connections.
    pub fn connection_options(&self) -> Vec<NetworkingConfigEntry> {
        vec![
            NetworkingConfigEntry::new_int32(NetworkingConfigValue::SendRateMin, self.send_rate),
            NetworkingConfigEntry::new_int32(NetworkingConfigValue::SendRateMax, self.send_rate),
            NetworkingConfigEntry::new_int32(
                NetworkingConfigValue::SendBufferSize,
                SEND_BUFFER_SIZE,
            ),
            NetworkingConfigEntry::new_int32(
                NetworkingConfigValue::P2PTransportICEEnable,
                self.ice_value(),
            ),
        ]
    }

    pub fn apply_globally(&self) {
        use sys::ESteamNetworkingConfigValue as V;
        // SAFETY: the networking utils interface is a process-wide singleton once SteamAPI (or
        // the game server API) is initialised, which callers do before building connections.
        unsafe {
            let utils = sys::SteamAPI_SteamNetworkingUtils_SteamAPI_v004();
            for (key, value) in [
                (V::k_ESteamNetworkingConfig_SendRateMin, self.send_rate),
                (V::k_ESteamNetworkingConfig_SendRateMax, self.send_rate),
                (V::k_ESteamNetworkingConfig_SendBufferSize, SEND_BUFFER_SIZE),
                (
                    V::k_ESteamNetworkingConfig_P2P_Transport_ICE_Enable,
                    self.ice_value(),
                ),
            ] {
                if !sys::SteamAPI_ISteamNetworkingUtils_SetGlobalConfigValueInt32(utils, key, value)
                {
                    tracing::warn!(?key, value, "failed to set global Steam networking config");
                }
            }
        }
    }

    fn ice_value(&self) -> i32 {
        if self.ice {
            ICE_PRIVATE_AND_PUBLIC
        } else {
            ICE_DISABLED
        }
    }
}

/// Pump callbacks until the relay network is usable.
pub fn wait_for_relay_network(
    mut run_callbacks: impl FnMut(),
    status: impl Fn() -> NetworkingAvailabilityResult,
    timeout: Duration,
) -> Result<Duration, String> {
    let start = Instant::now();
    loop {
        run_callbacks();
        match status() {
            Ok(NetworkingAvailability::Current) => return Ok(start.elapsed()),
            other if start.elapsed() > timeout => {
                return Err(format!(
                    "relay network not ready after {timeout:?}: {other:?}"
                ));
            }
            _ => std::thread::sleep(Duration::from_millis(20)),
        }
    }
}

/// This machine's ping location, for peers to estimate latency to it without connecting
/// (`EstimatePingTimeFromLocalHost`). `None` until relay pings have been measured.
pub fn local_ping_location() -> Option<String> {
    // SAFETY: the location struct and buffer outlive both calls.
    unsafe {
        let utils = sys::SteamAPI_SteamNetworkingUtils_SteamAPI_v004();
        let mut location: sys::SteamNetworkPingLocation_t = std::mem::zeroed();
        let age = sys::SteamAPI_ISteamNetworkingUtils_GetLocalPingLocation(utils, &mut location);
        if age < 0.0 {
            return None;
        }
        let mut buf = [0 as c_char; 1024];
        sys::SteamAPI_ISteamNetworkingUtils_ConvertPingLocationToString(
            utils,
            &location,
            buf.as_mut_ptr(),
            buf.len() as _,
        );
        let text = CStr::from_ptr(buf.as_ptr()).to_string_lossy().into_owned();
        (!text.is_empty()).then_some(text)
    }
}

/// Estimated round trip in milliseconds from this machine to a peer's ping location (as produced
/// by [`local_ping_location`]), routed through Steam's relays. `None` if the location doesn't
/// parse or this machine hasn't measured its own relay pings yet.
pub fn estimate_ping_ms(location: &str) -> Option<u32> {
    let location = std::ffi::CString::new(location).ok()?;
    // SAFETY: the string and location struct outlive both calls.
    unsafe {
        let utils = sys::SteamAPI_SteamNetworkingUtils_SteamAPI_v004();
        let mut parsed: sys::SteamNetworkPingLocation_t = std::mem::zeroed();
        if !sys::SteamAPI_ISteamNetworkingUtils_ParsePingLocationString(
            utils,
            location.as_ptr(),
            &mut parsed,
        ) {
            return None;
        }
        let ms = sys::SteamAPI_ISteamNetworkingUtils_EstimatePingTimeFromLocalHost(utils, &parsed);
        u32::try_from(ms).ok()
    }
}

unsafe extern "C" fn debug_output(
    kind: sys::ESteamNetworkingSocketsDebugOutputType,
    msg: *const c_char,
) {
    // SAFETY: Steam passes a valid NUL-terminated string for the duration of the call.
    let msg = unsafe { CStr::from_ptr(msg) }.to_string_lossy();
    use sys::ESteamNetworkingSocketsDebugOutputType as T;
    match kind {
        T::k_ESteamNetworkingSocketsDebugOutputType_Bug
        | T::k_ESteamNetworkingSocketsDebugOutputType_Error => {
            tracing::error!(target: "steamnet", "{msg}")
        }
        T::k_ESteamNetworkingSocketsDebugOutputType_Important
        | T::k_ESteamNetworkingSocketsDebugOutputType_Warning => {
            tracing::warn!(target: "steamnet", "{msg}")
        }
        T::k_ESteamNetworkingSocketsDebugOutputType_Msg => {
            tracing::info!(target: "steamnet", "{msg}")
        }
        _ => tracing::debug!(target: "steamnet", "{msg}"),
    }
}

/// Route Steam networking diagnostics into `tracing` (target `steamnet`), including info-level
/// messages such as route selection (which relay, or direct).
pub fn install_debug_output(verbose: bool) {
    use sys::ESteamNetworkingSocketsDebugOutputType as T;
    let level = if verbose {
        T::k_ESteamNetworkingSocketsDebugOutputType_Verbose
    } else {
        T::k_ESteamNetworkingSocketsDebugOutputType_Msg
    };
    // SAFETY: `debug_output` matches the expected callback signature.
    unsafe {
        let utils = sys::SteamAPI_SteamNetworkingUtils_SteamAPI_v004();
        sys::SteamAPI_ISteamNetworkingUtils_SetDebugOutputFunction(
            utils,
            level,
            Some(debug_output),
        );
    }
}
