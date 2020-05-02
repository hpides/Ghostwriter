#include "rembrandt/logging/throughput_logger.h"

#include <filesystem>
#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <iomanip>

#define ONE_MEGABYTE 1000l * 1000
#define ONE_GIGABYTE ONE_MEGABYTE * 1000

ThroughputLogger::ThroughputLogger(std::atomic<long> &counter, std::string dir, std::string filename, int event_size) :
    event_size_(event_size), counter_(counter), previous_value_(0), running_(false) {
  std::filesystem::path filepath = std::filesystem::path(dir) / std::filesystem::path(filename + ".csv");
  std::string filestring = filepath.generic_string();
  log_file_.open(filepath, std::ofstream::out | std::ofstream::trunc);
  if (!log_file_) {
    throw std::runtime_error("Unable to open log file!\n");
  }
  log_file_ << "Number of Messages\tThroughput in MB/s\n";
  log_file_.flush();
}

ThroughputLogger::~ThroughputLogger() {
  if (running_) {
    Stop();
  }
  if (log_file_.is_open()) {
    log_file_.close();
  }
}

void ThroughputLogger::Start() {
  thread_ = std::thread(&ThroughputLogger::Run, this);
}

void ThroughputLogger::Run() {

  boost::asio::io_context io;
  boost::asio::steady_timer t(io, boost::asio::chrono::seconds(1));
  running_ = true;
  t.async_wait(boost::bind(&ThroughputLogger::RunOnce, this, boost::asio::placeholders::error, &t));
  io.run();
}

void ThroughputLogger::RunOnce(const boost::system::error_code &,
                               boost::asio::steady_timer *t) {
  if (running_) {
    long current_value = counter_;
    long current_rate = current_value - previous_value_;
    double throughput_in_mbps = (double) current_rate * event_size_ / (ONE_MEGABYTE);
    log_file_ << current_rate << "\t" << std::fixed << std::setprecision(3) << throughput_in_mbps << "\n";
    log_file_.flush();
    previous_value_ = current_value;
    t->expires_at(t->expiry() + boost::asio::chrono::seconds(1));
    t->async_wait(boost::bind(&ThroughputLogger::RunOnce, this, boost::asio::placeholders::error, t));
  }
}

void ThroughputLogger::Stop() {
  running_ = false;
  if (thread_.joinable()) {
    thread_.join();
  }
}