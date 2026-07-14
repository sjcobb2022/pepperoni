// use pepperoni::mock::{MockLease, MockPg};
// use pepperoni::proxy::{run_proxy, ProxyHandle};
// use pepperoni::{Config, Ctx, State};

pub mod lease;

use lease::LeaseGrant;

use std::env;
use std::error::Error;

// use std::time::{Duration, Instant};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // let node_id = NodeId(env::args().nth(1).unwrap_or_else(|| "node-a".to_string()));

    // Where this proxy will listen.
    // let listen_addr = env::args()
    //     .nth(1)
    //     .unwrap_or_else(|| "127.0.0.1:8081".to_string())
    //     .parse()?;

    // Where the local postgres instance is
    // let postgres_addr = env::args()
    //     .nth(2)
    //     .unwrap_or_else(|| "127.0.0.1:5432".to_string())
    //     .parse()?;

    // let proxy_handle = ProxyHandle::new();

    Ok(())

    // tokio::spawn(run_proxy(listen_addr, postgres_addr, proxy_handle.clone()));
}
//     let mut state = State::Init;
//
//     let cfg = Config {
//         // The id of the current node.
//         id: node_id,
//
//         // primary configuration
//         // How long a lease should last
//         lease_ttl: Duration::from_secs(6), // 100%
//         // How early before the lease ends should we try and renew it.
//         renew_margin: Duration::from_millis(600), // 10%
//         // How often we refresh our state
//         timeout: Duration::from_millis(300), // 5%
//
//         // proxy draining config
//         // how long should we wait to drain active connections
//         drain_timeout: Duration::from_secs(3), // lease_ttl/2
//         // How often should check to see if we have drained the connections.
//         drain_refresh: Duration::from_millis(300), // 10%
//
//         // watchdog config
//         // the length before the watchdog needs to be pet again
//         watchdog_timeout: Duration::from_secs(12), // 2*lease_ttl
//     };
//
//     let mut ctx = Ctx {
//         pg: MockPg,
//         lease: MockLease::new(NodeId("a".to_string())),
//         cfg,
//     };
//
//     loop {
//         let now = Instant::now();
//
//         let just_started_pg =
//             matches!(&state, State::Init) || matches!(&state, State::Promoting { .. });
//
//         let next = state.tick(&mut ctx, now).await;
//
//         // Only open the proxy when we know our next state will be a stable one.
//         if just_started_pg && matches!(next, State::Standby { .. } | State::Leader { .. }) {
//             proxy_handle.open();
//         }
//
//         if matches!(next, State::Demoting) {
//             // TODO: Perhaps we do not need to explicitly reject them,
//             // but instead hold them in a queue?
//             // That queue can then be drained when it is toggled. However this does come with issues
//             // if postgres becomes fully unresponsive then the queue will just hold connections
//             // which will not be closed if watchdog then kills the system.
//             proxy_handle.close(); // reject incoming connections.
//         }
//
//         state = next;
//
//         tokio::time::sleep(ctx.cfg.timeout).await;
//     }
// }
