#include "rembrandt/network/message.h"

Message::Message(std::unique_ptr<char> buffer, size_t size) : buffer_(std::move(buffer)), size_(size) {};

