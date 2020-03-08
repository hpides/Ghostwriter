#include "rembrandt/network/basic_message.h"

BasicMessage::BasicMessage(std::unique_ptr<char> buffer, size_t size) : buffer_(std::move(buffer)), size_(size) {}
