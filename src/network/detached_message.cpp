#include "rembrandt/network/detached_message.h"

DetachedMessage::DetachedMessage(std::unique_ptr<char> buffer, size_t size) : buffer_(std::move(buffer)), size_(size) {}
