use crate::error::{Error, Result};
use fred::clients::Client;
use fred::interfaces::ClientLike;
use fred::types::{ClusterHash, CustomCommand, Value};

pub(crate) async fn ensure_group(client: &Client, stream_key: &str, group: &str) -> Result<()> {
    let cmd = CustomCommand::new_static("XGROUP", ClusterHash::FirstKey, false);
    let res = client
        .custom::<Value, _>(
            cmd,
            vec![
                Value::from("CREATE"),
                Value::from(stream_key),
                Value::from(group),
                Value::from("0"),
                Value::from("MKSTREAM"),
            ],
        )
        .await;
    match res {
        Ok(_) => Ok(()),
        Err(e) => {
            let msg = format!("{e}");
            if msg.contains("BUSYGROUP") {
                Ok(())
            } else {
                Err(Error::Redis(e))
            }
        }
    }
}
