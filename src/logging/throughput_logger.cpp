#include "rembrandt/logging/throughput_logger.h"

#include <filesystem>
#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <iomanip>

#define KiB 1024l
#define MiB 1024l * KiB

ThroughputLogger::ThroughputLogger(std::atomic<size_t> &counter, std::string dir, std::string filename, int event_size) :
    event_size_(event_size), counter_(counter), previous_value_(0), running_(false) {
  std::filesystem::path filepath = std::filesystem::path(dir) / std::filesystem::path(filename + ".csv");
  std::string filestring = filepath.generic_string();
  log_file_.open(filepath, std::ofstream::out | std::ofstream::trunc);
  if (!log_file_) {
    throw std::runtime_error("Unable to open log file!\n");
  }
  log_file_ << "Number of Messages\tThroughput in MiB/s\n";
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
  while (running_) {
    t.wait();
    RunOnce();
    t.expires_at(t.expiry() + boost::asio::chrono::seconds(1));
  }
}

void ThroughputLogger::RunOnce() {
  long current_value = counter_;
  long current_rate = current_value - previous_value_;
  double throughput_in_mibps = (double) current_rate * event_size_ / (MiB);
  log_file_ << current_rate << "\t" << std::fixed << std::setprecision(3) << throughput_in_mibps << "\n";
  log_file_.flush();
  previous_value_ = current_value;
}

void ThroughputLogger::Stop() {
  running_ = false;
  if (thread_.joinable()) {
    thread_.join();
  }
}