use std::collections::HashMap;

use zbus::{
    proxy,
    zvariant::{OwnedObjectPath, OwnedValue},
};

use super::{BluetoothDevice, BluetoothState};

type InterfaceProperties = HashMap<String, OwnedValue>;
type ManagedObjects = HashMap<OwnedObjectPath, HashMap<String, InterfaceProperties>>;

const ADAPTER_INTERFACE: &str = "org.bluez.Adapter1";
const DEVICE_INTERFACE: &str = "org.bluez.Device1";
const BATTERY_INTERFACE: &str = "org.bluez.Battery1";

pub struct BluetoothDbus<'a> {
    pub bluez: BluezObjectManagerProxy<'a>,
    pub adapter: Option<AdapterProxy<'a>>,
    adapter_path: Option<OwnedObjectPath>,
    managed_objects: ManagedObjects,
}

impl BluetoothDbus<'_> {
    pub async fn new(conn: &zbus::Connection) -> anyhow::Result<Self> {
        let bluez = BluezObjectManagerProxy::new(conn).await?;
        let managed_objects = bluez.get_managed_objects().await?;
        let adapter_path = managed_objects
            .iter()
            .find_map(|(key, item)| item.contains_key(ADAPTER_INTERFACE).then(|| key.clone()));

        let adapter = if let Some(adapter) = adapter_path.clone() {
            Some(AdapterProxy::builder(conn).path(adapter)?.build().await?)
        } else {
            None
        };

        Ok(Self {
            bluez,
            adapter,
            adapter_path,
            managed_objects,
        })
    }

    fn property<T>(properties: &InterfaceProperties, name: &str) -> Option<T>
    where
        T: TryFrom<OwnedValue>,
    {
        properties.get(name)?.try_clone().ok()?.try_into().ok()
    }

    fn adapter_properties(&self) -> Option<&InterfaceProperties> {
        self.adapter_path
            .as_ref()
            .and_then(|path| self.managed_objects.get(path))
            .and_then(|interfaces| interfaces.get(ADAPTER_INTERFACE))
    }

    pub fn powered(&self) -> Option<bool> {
        self.adapter_properties()
            .and_then(|properties| Self::property(properties, "Powered"))
    }

    pub async fn set_powered(&self, value: bool) -> zbus::Result<()> {
        if let Some(adapter) = &self.adapter {
            adapter.set_powered(value).await?;
        }

        Ok(())
    }

    pub fn state(&self) -> anyhow::Result<BluetoothState> {
        if self.adapter_path.is_none() {
            return Ok(BluetoothState::Unavailable);
        }

        let powered = self
            .powered()
            .ok_or_else(|| anyhow::anyhow!("missing or invalid Adapter1.Powered property"))?;

        Ok(if powered {
            BluetoothState::Active
        } else {
            BluetoothState::Inactive
        })
    }

    pub async fn start_discovery(&self) -> zbus::Result<()> {
        if let Some(adapter) = &self.adapter {
            adapter.start_discovery().await?;
        }
        Ok(())
    }

    pub async fn stop_discovery(&self) -> zbus::Result<()> {
        if let Some(adapter) = &self.adapter {
            adapter.stop_discovery().await?;
        }
        Ok(())
    }

    pub fn discovering(&self) -> bool {
        self.adapter_properties()
            .and_then(|properties| Self::property(properties, "Discovering"))
            .unwrap_or(false)
    }

    pub fn devices(&self) -> anyhow::Result<Vec<BluetoothDevice>> {
        let mut devices = Vec::new();

        for (device_path, interfaces) in &self.managed_objects {
            let Some(device_properties) = interfaces.get(DEVICE_INTERFACE) else {
                continue;
            };

            let name = Self::property(device_properties, "Alias").ok_or_else(|| {
                anyhow::anyhow!("missing or invalid Device1.Alias property for {device_path:?}")
            })?;
            let connected = Self::property(device_properties, "Connected").ok_or_else(|| {
                anyhow::anyhow!("missing or invalid Device1.Connected property for {device_path:?}")
            })?;
            let paired = Self::property(device_properties, "Paired").ok_or_else(|| {
                anyhow::anyhow!("missing or invalid Device1.Paired property for {device_path:?}")
            })?;

            let battery = if connected {
                interfaces
                    .get(BATTERY_INTERFACE)
                    .map(|properties| {
                        Self::property(properties, "Percentage").ok_or_else(|| {
                            anyhow::anyhow!(
                                "missing or invalid Battery1.Percentage property for {device_path:?}"
                            )
                        })
                    })
                    .transpose()?
            } else {
                None
            };

            devices.push(BluetoothDevice {
                name,
                battery,
                path: device_path.clone(),
                connected,
                paired,
            });
        }

        Ok(devices)
    }

    pub async fn pair_device(&self, device_path: &OwnedObjectPath) -> zbus::Result<()> {
        let device = DeviceProxy::builder(self.bluez.inner().connection())
            .path(device_path)?
            .build()
            .await?;

        device.pair().await
    }

    pub async fn connect_device(&self, device_path: &OwnedObjectPath) -> zbus::Result<()> {
        let device = DeviceProxy::builder(self.bluez.inner().connection())
            .path(device_path)?
            .build()
            .await?;

        device.connect().await
    }

    pub async fn disconnect_device(&self, device_path: &OwnedObjectPath) -> zbus::Result<()> {
        let device = DeviceProxy::builder(self.bluez.inner().connection())
            .path(device_path)?
            .build()
            .await?;

        device.disconnect().await
    }

    pub async fn remove_device(&self, device_path: &OwnedObjectPath) -> zbus::Result<()> {
        if let Some(adapter) = &self.adapter {
            adapter.remove_device(device_path.as_ref()).await?;
        }
        Ok(())
    }
}

#[proxy(
    default_service = "org.bluez",
    default_path = "/",
    interface = "org.freedesktop.DBus.ObjectManager"
)]
pub trait BluezObjectManager {
    fn get_managed_objects(&self) -> zbus::Result<ManagedObjects>;

    #[zbus(signal)]
    fn interfaces_added(&self) -> Result<()>;

    #[zbus(signal)]
    fn interfaces_removed(&self) -> Result<()>;
}

#[proxy(
    default_service = "org.bluez",
    default_path = "/org/bluez/hci0",
    interface = "org.bluez.Adapter1"
)]
pub trait Adapter {
    #[zbus(property)]
    fn set_powered(&self, value: bool) -> zbus::Result<()>;

    fn start_discovery(&self) -> zbus::Result<()>;

    fn stop_discovery(&self) -> zbus::Result<()>;

    fn remove_device(&self, device: zbus::zvariant::ObjectPath<'_>) -> zbus::Result<()>;
}

#[proxy(default_service = "org.bluez", interface = "org.bluez.Device1")]
trait Device {
    fn pair(&self) -> zbus::Result<()>;

    fn connect(&self) -> zbus::Result<()>;

    fn disconnect(&self) -> zbus::Result<()>;
}
