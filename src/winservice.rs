// Windows service entrypoint.
//
// On Windows the agent runs as a native service (LocalSystem, auto-start) — so
// it survives reboots without an interactive logon and shows no console
// window, unlike the old scheduled-task model. The SCM launches the registered
// binPath (`lymon-agent.exe --service`); `run()` hands control to the service
// dispatcher, which calls `service_main`. We report RUNNING, then run the
// normal agent (`crate::run_agent`) on a tokio runtime until the SCM asks us to
// stop.

use std::ffi::OsString;
use std::sync::mpsc;
use std::sync::OnceLock;
use std::time::Duration;

use anyhow::Result;
use windows_service::service::{
    ServiceControl, ServiceControlAccept, ServiceExitCode, ServiceState, ServiceStatus, ServiceType,
};
use windows_service::service_control_handler::{self, ServiceControlHandlerResult};
use windows_service::service_dispatcher;

const SERVICE_NAME: &str = "LymonAgent";
const SERVICE_TYPE: ServiceType = ServiceType::OWN_PROCESS;

// The macro-generated `ffi_service_main` takes no args, so we stash the parsed
// config path here for `service_main` to pick up.
static CONFIG_PATH: OnceLock<Option<String>> = OnceLock::new();

windows_service::define_windows_service!(ffi_service_main, service_main);

/// Enter the SCM dispatcher. Blocks until the service stops.
pub fn run(config_path: Option<String>) -> Result<()> {
    let _ = CONFIG_PATH.set(config_path);
    service_dispatcher::start(SERVICE_NAME, ffi_service_main)?;
    Ok(())
}

fn service_main(_args: Vec<OsString>) {
    if let Err(e) = run_service() {
        // Tracing may not be up yet if we failed before run_agent; last resort.
        eprintln!("lymon-agent service error: {e:?}");
    }
}

fn run_service() -> Result<()> {
    let (stop_tx, stop_rx) = mpsc::channel::<()>();

    // The control handler runs on an SCM thread; it just signals the stop
    // channel. Stop + Shutdown end the service; Interrogate is a no-op ack.
    let handler = move |control| match control {
        ServiceControl::Stop | ServiceControl::Shutdown => {
            let _ = stop_tx.send(());
            ServiceControlHandlerResult::NoError
        }
        ServiceControl::Interrogate => ServiceControlHandlerResult::NoError,
        _ => ServiceControlHandlerResult::NotImplemented,
    };
    let status_handle = service_control_handler::register(SERVICE_NAME, handler)?;

    let status = |state: ServiceState, accept: ServiceControlAccept| ServiceStatus {
        service_type: SERVICE_TYPE,
        current_state: state,
        controls_accepted: accept,
        exit_code: ServiceExitCode::Win32(0),
        checkpoint: 0,
        wait_hint: Duration::default(),
        process_id: None,
    };

    status_handle.set_service_status(status(
        ServiceState::Running,
        ServiceControlAccept::STOP | ServiceControlAccept::SHUTDOWN,
    ))?;

    // Run the agent on its own runtime; abort it when the SCM asks us to stop.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let cfg = CONFIG_PATH.get().cloned().flatten();
    let agent = rt.spawn(async move {
        if let Err(e) = crate::run_agent(cfg).await {
            tracing::error!(error = %e, "agent exited with error");
        }
    });

    let _ = stop_rx.recv(); // block until Stop/Shutdown
    agent.abort();

    status_handle
        .set_service_status(status(ServiceState::Stopped, ServiceControlAccept::empty()))?;
    Ok(())
}
