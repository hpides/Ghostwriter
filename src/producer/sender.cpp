#include "../../include/rembrandt/producer/sender.h"
#include "../../include/rembrandt/protocol/rembrandt_protocol_generated.h"

#include <memory>

Sender::Sender(MessageAccumulator &message_accumulator) :
    message_accumulator_(message_accumulator) {}

void Sender::Run() {
  while (true) {
    // TODO
  }
}

void Sender::SendOutline(Batch *batch) {
  flatbuffers::FlatBufferBuilder builder(128);
  auto send_outline = Rembrandt::Protocol::CreateSendOutline(
      builder,
      batch->getTopic(),
      batch->getPartition(),
      batch->getNumMessages(),
      batch->getSize());
  auto message = Rembrandt::Protocol::CreateBaseMessage(
      builder,
      Rembrandt::Protocol::Message_SendOutline,
      send_outline.Union());
  builder.Finish(message);
  const flatbuffers::DetachedBuffer buffer = builder.Release();
  cli
}
