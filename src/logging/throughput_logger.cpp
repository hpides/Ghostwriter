#include "rembrandt/logging/throughput_logger.h"

#include <filesystem>
#include <boost/bind.hpp>
#include <boost/function.hpp>

#define ONE_GIGABYTE 1024l * 1024 *1024

ThroughputLogger::ThroughputLogger(atomic_long &counter, std::string dir, int event_size) :
    event_size_(event_size), counter_(counter) {
  log_file_.open(dir + "/log.csv");
  if (!log_file_) {
    throw std::runtime_error("Unable to open log file!\n");
  }
}

ThroughputLogger::~ThroughputLogger() {
  if (log_file_.is_open()) {
    log_file_.close();
  }
}

void ThroughputLogger::Start() {
  boost::asio::io_context io;
  boost::asio::steady_timer t(io, boost::asio::chrono::seconds(1));
  t.async_wait(boost::bind(&ThroughputLogger::RunOnce, this, boost::asio::placeholders::error, &t));
  running_ = true;
  thread_ = std::thread(&boost::asio::io_context::run, io);
}

void ThroughputLogger::RunOnce(const boost::system::error_code &,
                               boost::asio::steady_timer *t) {
  while (running_) {
    long current_rate = counter_ - previous_value_;
    double throughput_in_gbps = (double) current_rate * event_size_ / ONE_GIGABYTE;
    log_file_ << std::fixed << std::setprecision(3) << throughput_in_gbps << "\n";
    t->expires_at(t->expiry() + boost::asio::chrono::seconds(1));
    t->async_wait(boost::bind(&ThroughputLogger::RunOnce, this, boost::asio::placeholders::error, &t));
  }
}

void ThroughputLogger::Stop() {
  running_ = false;
  thread_.join();
}