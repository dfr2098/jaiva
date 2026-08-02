use async_trait::async_trait;
use tracing::info;

use crate::{
    engine::{DataPacket, OutputSender, PacketContent, Processor, ProcessorContext},
    error::FlowError,
};

pub struct LogRecords;

#[async_trait]
impl Processor for LogRecords {
    async fn execute(
        &self,
        packet: DataPacket,
        context: &ProcessorContext,
        output: &OutputSender,
    ) -> Result<(), FlowError> {
        if let Some(message) = packet.attributes.get("error.message") {
            info!(
                processor_id = %context.processor_id,
                packet_id = %packet.id,
                error_processor = packet.attributes.get("error.processor").map(String::as_str),
                error = %message,
                "failure"
            );
        }
        match &packet.content {
            PacketContent::Records(records) => {
                for record in records {
                    info!(
                        processor_id = %context.processor_id,
                        packet_id = %packet.id,
                        record = %record,
                        "record"
                    );
                }
            }
            PacketContent::Encoded { media_type, bytes } => {
                info!(
                    processor_id = %context.processor_id,
                    packet_id = %packet.id,
                    media_type,
                    byte_count = bytes.len(),
                    content = %String::from_utf8_lossy(bytes),
                    "encoded content"
                );
            }
        }

        output.success(packet).await
    }
}
