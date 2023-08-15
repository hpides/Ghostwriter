#ifndef REMBRANDT_INCLUDE_REMBRANDT_UTILS_H_
#define REMBRANDT_INCLUDE_REMBRANDT_UTILS_H_

#include <utility>
#include <arpa/inet.h>
#include <boost/functional/hash.hpp>

typedef std::pair<uint32_t, uint32_t> TopicPartition;

namespace std {
template<>
class hash<sockaddr_in6> {
 public:
  size_t operator()(const sockaddr_in6 &sockaddr_in6) const {
    size_t result = 0;
    boost::hash_combine(result, sockaddr_in6.sin6_family);
    boost::hash_combine(result, sockaddr_in6.sin6_addr.s6_addr);
    boost::hash_combine(result, sockaddr_in6.sin6_port);
    return result;
  }
};
}
#endif //REMBRANDT_INCLUDE_REMBRANDT_UTILS_H_
