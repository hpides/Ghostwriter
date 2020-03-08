#include "rembrandt/network/flat_buffers_message.h"

FlatBuffersMessage::FlatBuffersMessage(std::unique_ptr<flatbuffers::DetachedBuffer> detached_buffer) :
    detached_buffer_(std::move(detached_buffer)) {}