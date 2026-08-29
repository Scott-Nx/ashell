use super::{ReadOnlyService, Service, ServiceEvent};
use dbus::{BluetoothDbus, BluezObjectManagerProxy};
use iced::{
    Subscription, Task,
    futures::{SinkExt, Stream, StreamExt, channel::mpsc::Sender, stream::pending, stream_select},
    stream::channel,
};
use inotify::{Inotify, WatchMask};
use log::{debug, error, info, warn};
use std::{any::TypeId, collections::HashMap, io::ErrorKind, ops::Deref, pin::Pin};
use tokio::process::Command;
use zbus::zvariant::{OwnedObjectPath, OwnedValue};

mod dbus;

type EventStream = Pin<Box<dyn Stream<Item = ()> + Send>>;

#[derive(PartialEq, Eq, Debug, Clone)]
pub enum BluetoothState {
    Unavailable,
    Active,
    Inactive,
}

#[derive(Debug, Clone)]
pub struct BluetoothDevice {
    pub name: String,
    pub battery: Option<u8>,
    pub path: OwnedObjectPath,
    pub connected: bool,
    pub paired: bool,
}

#[derive(Debug, Clone)]
pub struct BluetoothData {
    pub state: BluetoothState,
    pub devices: Vec<BluetoothDevice>,
    pub discovering: bool,
}

#[derive(Debug, Clone)]
pub struct BluetoothService {
    conn: zbus::Connection,
    data: BluetoothData,
}

impl Deref for BluetoothService {
    type Target = BluetoothData;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

#[derive(Debug, Clone)]
pub enum BluetoothCommand {
    Toggle,
    StartDiscovery,
    PairDevice(OwnedObjectPath),
    ConnectDevice(OwnedObjectPath),
    DisconnectDevice(OwnedObjectPath),
    RemoveDevice(OwnedObjectPath),
}

enum State {
    Init,
    Active(zbus::Connection),
    Error,
}

impl BluetoothService {
    async fn initialize_data(conn: &zbus::Connection) -> anyhow::Result<BluetoothData> {
        let bluetooth = BluetoothDbus::new(conn).await?;

        let state = bluetooth.state()?;
        let rfkill_soft_block = BluetoothService::check_rfkill_soft_block().await?;

        let state = match state {
            BluetoothState::Unavailable => BluetoothState::Unavailable,
            BluetoothState::Active if rfkill_soft_block => BluetoothState::Inactive,
            state => state,
        };
        let devices = bluetooth.devices()?;
        let discovering = bluetooth.discovering();

        Ok(BluetoothData {
            state,
            devices,
            discovering,
        })
    }

    fn relevant_property_changed(message: zbus::Result<zbus::Message>) -> Option<()> {
        let message = message.ok()?;
        let (interface, changed, invalidated): (String, HashMap<String, OwnedValue>, Vec<String>) =
            message.body().deserialize().ok()?;

        let has_changed = |properties: &[&str]| {
            properties.iter().any(|property| {
                changed.contains_key(*property)
                    || invalidated
                        .iter()
                        .any(|invalidated| invalidated == property)
            })
        };

        match interface.as_str() {
            "org.bluez.Adapter1" => has_changed(&["Powered", "Discovering"]),
            "org.bluez.Device1" => has_changed(&["Connected"]),
            "org.bluez.Battery1" => has_changed(&["Percentage"]),
            _ => false,
        }
        .then_some(())
    }

    async fn events(conn: &zbus::Connection) -> anyhow::Result<impl Stream<Item = ()> + use<>> {
        let bluez = BluezObjectManagerProxy::new(conn).await?;

        let interface_changed = stream_select!(
            bluez.receive_interfaces_added().await?.map(|_| {}),
            bluez.receive_interfaces_removed().await?.map(|_| {}),
        )
        .boxed();

        let properties_changed = zbus::MessageStream::for_match_rule(
            zbus::MatchRule::builder()
                .msg_type(zbus::message::Type::Signal)
                .sender("org.bluez")?
                .interface("org.freedesktop.DBus.Properties")?
                .member("PropertiesChanged")?
                .arg0ns("org.bluez")?
                .path_namespace("/org/bluez")?
                .build(),
            conn,
            None,
        )
        .await?
        .filter_map(|message| async move { BluetoothService::relevant_property_changed(message) })
        .map(|_| {})
        .boxed();

        let rfkill = BluetoothService::listen_rfkill_soft_block_changes().await?;

        Ok(Box::pin(stream_select!(
            interface_changed,
            properties_changed,
            rfkill,
        )))
    }

    async fn start_listening(state: State, output: &mut Sender<ServiceEvent<Self>>) -> State {
        match state {
            State::Init => match zbus::Connection::system().await {
                Ok(conn) => {
                    let data = BluetoothService::initialize_data(&conn).await;

                    match data {
                        Ok(data) => {
                            info!("Bluetooth service initialized");

                            let _ = output
                                .send(ServiceEvent::Init(BluetoothService {
                                    data,
                                    conn: conn.clone(),
                                }))
                                .await;

                            State::Active(conn)
                        }
                        Err(err) => {
                            error!("Failed to initialize bluetooth service: {err}");

                            State::Error
                        }
                    }
                }
                Err(err) => {
                    error!("Failed to connect to system bus: {err}");

                    State::Error
                }
            },
            State::Active(conn) => {
                info!("Listening for bluetooth events");

                match BluetoothService::events(&conn).await {
                    Ok(mut events) => {
                        while events.next().await.is_some() {
                            if let Ok(data) = BluetoothService::initialize_data(&conn).await {
                                let _ = output.send(ServiceEvent::Update(data)).await;
                            }
                        }

                        State::Active(conn)
                    }
                    Err(err) => {
                        error!("Failed to listen for bluetooth events: {err}");
                        State::Error
                    }
                }
            }
            State::Error => {
                error!("Bluetooth service error");

                let _ = pending::<u8>().next().await;
                State::Error
            }
        }
    }

    async fn spawn_rfkill(binary: &str, args: &[&str]) -> std::io::Result<std::process::Output> {
        let mut command = Command::new(binary);
        for arg in args {
            command.arg(arg);
        }
        command.output().await
    }

    async fn run_rfkill_command(args: &[&str]) -> std::io::Result<std::process::Output> {
        BluetoothService::spawn_rfkill("rfkill", args).await
    }

    pub async fn check_rfkill_soft_block() -> anyhow::Result<bool> {
        let output = match BluetoothService::run_rfkill_command(&["list", "bluetooth"]).await {
            Ok(output) => output,
            Err(err) if err.kind() == ErrorKind::NotFound => {
                warn!("rfkill binary not found, assuming bluetooth is not soft blocked");
                return Ok(false);
            }
            Err(err) => return Err(err.into()),
        };

        let output = String::from_utf8(output.stdout)?;

        Ok(output.contains("Soft blocked: yes"))
    }

    pub async fn listen_rfkill_soft_block_changes() -> anyhow::Result<EventStream> {
        let inotify = Inotify::init()?;

        match inotify.watches().add("/dev/rfkill", WatchMask::MODIFY) {
            Ok(_) => {
                let buffer = [0; 512];
                Ok(inotify.into_event_stream(buffer)?.map(|_| {}).boxed())
            }
            Err(err) if err.kind() == ErrorKind::NotFound => {
                warn!("/dev/rfkill not found, disabling rfkill change notifications for bluetooth");
                Ok(pending().boxed())
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn toggle_power(conn: &zbus::Connection, power: bool) -> anyhow::Result<()> {
        let bluetooth = BluetoothDbus::new(conn).await?;

        bluetooth.set_powered(power).await?;

        Ok(())
    }
}

impl ReadOnlyService for BluetoothService {
    type UpdateEvent = BluetoothData;
    type Error = ();

    fn update(&mut self, event: Self::UpdateEvent) {
        self.data = event;
    }

    fn subscribe() -> Subscription<ServiceEvent<Self>> {
        Subscription::run_with(TypeId::of::<Self>(), |_| {
            channel(100, async |mut output| {
                let mut state = State::Init;

                loop {
                    state = BluetoothService::start_listening(state, &mut output).await;
                }
            })
        })
    }
}

impl Service for BluetoothService {
    type Command = BluetoothCommand;

    fn command(&mut self, command: Self::Command) -> Task<ServiceEvent<Self>> {
        match command {
            BluetoothCommand::Toggle => {
                let conn = self.conn.clone();

                if self.data.state == BluetoothState::Unavailable {
                    Task::none()
                } else {
                    let mut data = self.data.clone();

                    Task::perform(
                        async move {
                            let powered = data.state == BluetoothState::Active;
                            debug!("Toggling bluetooth power to: {}", !powered);
                            let res = BluetoothService::toggle_power(&conn, !powered).await;

                            if res.is_ok() {
                                data.state = if powered {
                                    BluetoothState::Inactive
                                } else {
                                    BluetoothState::Active
                                }
                            }

                            data
                        },
                        ServiceEvent::Update,
                    )
                }
            }
            BluetoothCommand::StartDiscovery => {
                let conn = self.conn.clone();
                Task::perform(
                    async move {
                        let bluetooth = BluetoothDbus::new(&conn).await;
                        if let Ok(bluetooth) = bluetooth {
                            let _ = bluetooth.start_discovery().await;

                            // Auto-stop after 15 seconds
                            tokio::time::sleep(tokio::time::Duration::from_secs(15)).await;
                            let _ = bluetooth.stop_discovery().await;
                        }
                        BluetoothService::initialize_data(&conn)
                            .await
                            .unwrap_or_else(|_| BluetoothData {
                                state: BluetoothState::Unavailable,
                                devices: vec![],
                                discovering: false,
                            })
                    },
                    ServiceEvent::Update,
                )
            }
            BluetoothCommand::PairDevice(device_path) => {
                let conn = self.conn.clone();
                Task::perform(
                    async move {
                        let bluetooth = BluetoothDbus::new(&conn).await;
                        if let Ok(bluetooth) = bluetooth {
                            debug!("Pairing device: {:?}", device_path);
                            let _ = bluetooth.pair_device(&device_path).await;
                        }
                        BluetoothService::initialize_data(&conn)
                            .await
                            .unwrap_or_else(|_| BluetoothData {
                                state: BluetoothState::Unavailable,
                                devices: vec![],
                                discovering: false,
                            })
                    },
                    ServiceEvent::Update,
                )
            }
            BluetoothCommand::ConnectDevice(device_path) => {
                let conn = self.conn.clone();
                Task::perform(
                    async move {
                        let bluetooth = BluetoothDbus::new(&conn).await;
                        if let Ok(bluetooth) = bluetooth {
                            debug!("Connecting device: {:?}", device_path);
                            let _ = bluetooth.connect_device(&device_path).await;
                        }
                        BluetoothService::initialize_data(&conn)
                            .await
                            .unwrap_or_else(|_| BluetoothData {
                                state: BluetoothState::Unavailable,
                                devices: vec![],
                                discovering: false,
                            })
                    },
                    ServiceEvent::Update,
                )
            }
            BluetoothCommand::DisconnectDevice(device_path) => {
                let conn = self.conn.clone();
                Task::perform(
                    async move {
                        let bluetooth = BluetoothDbus::new(&conn).await;
                        if let Ok(bluetooth) = bluetooth {
                            debug!("Disconnecting device: {:?}", device_path);
                            let _ = bluetooth.disconnect_device(&device_path).await;
                        }
                        BluetoothService::initialize_data(&conn)
                            .await
                            .unwrap_or_else(|_| BluetoothData {
                                state: BluetoothState::Unavailable,
                                devices: vec![],
                                discovering: false,
                            })
                    },
                    ServiceEvent::Update,
                )
            }
            BluetoothCommand::RemoveDevice(device_path) => {
                let conn = self.conn.clone();
                Task::perform(
                    async move {
                        let bluetooth = BluetoothDbus::new(&conn).await;
                        if let Ok(bluetooth) = bluetooth {
                            debug!("Removing device: {:?}", device_path);
                            let _ = bluetooth.remove_device(&device_path).await;
                        }
                        BluetoothService::initialize_data(&conn)
                            .await
                            .unwrap_or_else(|_| BluetoothData {
                                state: BluetoothState::Unavailable,
                                devices: vec![],
                                discovering: false,
                            })
                    },
                    ServiceEvent::Update,
                )
            }
        }
    }
}
