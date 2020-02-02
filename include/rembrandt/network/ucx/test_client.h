#ifndef REMBRANDT_SRC_NETWORK_UCX_TEST_CLIENT_H_
#define REMBRANDT_SRC_NETWORK_UCX_TEST_CLIENT_H_

#include "client.h"

class TestClient : public Client {
 public:
 private:
  void SendTest(ucp_ep_h &ep);
};

#endif //REMBRANDT_SRC_NETWORK_UCX_TEST_CLIENT_H_
