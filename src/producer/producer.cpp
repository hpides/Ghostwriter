#include <rembrandt/producer/producer.h>

Producer::Producer(UCP::Context &context) : client_(context),
                                            message_accumulator_(16, 16),
                                            sender_(client_, message_accumulator_) {}

void Producer::Start() {// TODO: Configure inputs
  char *IPADDRESS = (char *) "192.168.5.31";
  /* Initialize the UCX required objects */
  UCP::Endpoint &ep = client_.GetConnection(IPADDRESS, 13350);
  ucp_rkey_h rkey = client_.RegisterRemoteMemory(ep, IPADDRESS, 13351);
  printf("%p", rkey);

  sender_.Start(ep);
}

void Producer::Stop() {
  sender_.Stop();
}

void Producer::Send(TopicPartition topic_partition,
                    void *buffer,
                    size_t length) {
  message_accumulator_.Append(topic_partition, (char *) buffer, length);
}

int main(int argc, char *argv[]) {
  UCP::Context context = UCP::Context(true);
  Producer producer = Producer(context);
  producer.Start();
  TopicPartition topic_partition(1, 1);
  for (uint32_t i = 0; i < 100; i++) {
    if (i % 10 == 0) {
      printf("Iteration: %d\n", i);
    }
    producer.Send(topic_partition, &i, sizeof(i));
  }
  producer.Stop();
}