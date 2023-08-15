#ifndef REMBRANDT_INCLUDE_REMBRANDT_UTILS_H_
#define REMBRANDT_INCLUDE_REMBRANDT_UTILS_H_

#include <utility>
#include <arpa/inet.h>
#include <boost/functional/hash.hpp>

typedef std::pair<uint32_t, uint32_t> TopicPartition;

namespace std {
template<>
class hash<sockaddr_in> {
 public:
  size_t operator()(const sockaddr_in &sockaddr_in) const {
    size_t result = 0;
    boost::hash_combine(result, sockaddr_in.sin_family);
    boost::hash_combine(result, sockaddr_in.sin_addr.s_addr);
    boost::hash_combine(result, sockaddr_in.sin_port);
    return result;
  }
};
}
#endif //REMBRANDT_INCLUDE_REMBRANDT_UTILS_H_
