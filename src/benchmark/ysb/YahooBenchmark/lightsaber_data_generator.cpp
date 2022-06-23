#include <iostream>
#include <cstring>
#include <omp.h>
#include <chrono>
#include <random>
#include <bitset>
#include <algorithm>
#include <atomic>
#include <array>
#include <assert.h>
#include <sstream>
#include <fstream>

using namespace std;

struct record {
  long timestamp;
  long padding_0;
  __uint128_t user_id;
  __uint128_t page_id;
  __uint128_t ad_id;
  long ad_type;
  long event_type;
  __uint128_t ip_address;
  __uint128_t padding_1;
  __uint128_t padding_2;
};


void generate(record& data, size_t ad_id, size_t page_id, size_t user_id, size_t index)
{
  auto ts = std::chrono::system_clock::now().time_since_epoch();
  data.timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(ts).count();
  data.user_id = (__uint128_t) user_id;
  data.page_id = (__uint128_t) page_id;
  data.ad_id = (__uint128_t) ad_id;
  data.ad_type = index % 5;
  data.event_type = index % 3;
  data.ip_address = 0x01020304;
}


int main(int argc, char *argv[])
{
  cout << "usage filePath tupleCnt" << endl;

  char* filePath = "data.bin";
  size_t tupleCnt = 1000;

  if(argc == 3)
  {
    filePath = argv[1];
    tupleCnt = atoi(argv[2]);
    cout << "using parameter filepath=" << filePath << " tupleCnt=" << tupleCnt << endl;
  }
  else
  {
    cout << "not enough parameters" << endl;
  }

  std::string path(filePath);
  path = path.substr(0,path.find("."));
  path.append(".bin");

  std::random_device rd;  //Will be used to obtain a seed for the random number engine
  std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
  size_t* ad_ids = new size_t[tupleCnt];
  size_t* page_ids = new size_t[tupleCnt];
  size_t* user_ids = new size_t[tupleCnt];
  std::uniform_int_distribution<size_t> ads_distribution(0, 1000);
  std::uniform_int_distribution<size_t> distr(0, 1000000);

  record* recs = new record[tupleCnt];
  for(size_t i = 0; i < tupleCnt; i++)
  {
    ad_ids[i] = ads_distribution(gen);
    page_ids[i] = distr(gen);
    user_ids[i] = distr(gen);
    generate(recs[i], ad_ids[i], page_ids[i], user_ids[i], i);
  }

      //write to file
  cout << "writing file" << path << endl;
  std::ofstream ofp(path, std::ios::out | std::ios::binary);
  ofp.write(reinterpret_cast<const char*>(recs), tupleCnt * sizeof(record));
  ofp.close();
}