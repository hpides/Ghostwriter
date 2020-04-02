#include "rembrandt/network/attached_message.h"

AttachedMessage::AttachedMessage(char *buffer, size_t size) : buffer_(buffer), size_(size) {}