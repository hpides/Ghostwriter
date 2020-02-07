#include <functional>
#include <iostream>
#include <stdexcept>
#include "../../include/rembrandt/network/utils.h"
#include "../../include/rembrandt/protocol/rembrandt_protocol_generated.h"
#include "../../include/rembrandt/network/ucx/endpoint.h"
#include "../../include/rembrandt/producer/sender.h"
#include "../../include/rembrandt/producer/message_accumulator.h"

Sender::Sender(UCP::Client &client, MessageAccumulator &message_accumulator)
    : client_(client),
      message_accumulator_(message_accumulator) {}

void Sender::Start(UCP::Endpoint &ep) {
  if (!running) {
    running = true;
    thread_ = std::thread(&Sender::Run, this, std::ref(ep));
  } else {
    std::cout << "Sender already running.\n";
  }
}

void Sender::Stop() {
  if (running) {
    running = false;
    thread_.join();
  } else {
    std::cout << "Sender not running.\n";
  }
}

void Sender::Run(UCP::Endpoint &ep) {
  while (running) {
    std::unique_ptr<BatchDeque> batches = message_accumulator_.Drain();
    for (Batch *batch: *batches) {
      Send(batch, ep);
    }
  }
}

void Sender::Send(Batch *batch, UCP::Endpoint &endpoint) {
  std::cout << "Sending batch\n";
  std::function<void(void *, ucs_status_t)>
      cb = [&accumulator = message_accumulator_, b = batch](void *request,
                                                            ucs_status_t status) {
    std::cout << "Triggered CB\n";
    accumulator.Free(b);
  };
  ucs_status_ptr_t status_ptr = endpoint.put(batch->getBuffer(),
                                             batch->getSize(),
                                             endpoint.GetRemoteAddress(),
                                             print_cb);
  if (status_ptr == NULL) {
    std::cout << "Request completed immediately\n";
    message_accumulator_.Free(batch);
    return;
  }

  if (UCS_PTR_IS_ERR(status_ptr)) {
    throw std::runtime_error("Runtime error!\n");
  }
  ucs_status_t status;
  do {
    ucp_worker_progress(client_.GetWorker().GetWorkerHandle());
    status = ucp_request_check_status(status_ptr);
  } while (status == UCS_INPROGRESS);

  /* This request may be reused so initialize it for next time */
  ucp_request_free(status_ptr);

  // TODO: Handle errors
  message_accumulator_.Free(batch);
  if (status != UCS_OK) {
    throw std::runtime_error("Failed sending\n");
  }
  std::cout << "Done sending\n";
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
  builder.FinishSizePrefixed(message);
  const flatbuffers::DetachedBuffer buffer = builder.Release();

}
