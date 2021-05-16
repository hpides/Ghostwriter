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


void shuffle(record* array, size_t n)
{
  if (n > 1)
  {
    size_t i;
    for (i = 0; i < n - 1; i++)
    {
      size_t j = i + rand() / (RAND_MAX / (n - i) + 1);
      record t = array[j];
      array[j] = array[i];
      array[i] = t;
    }
  }
}

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
  cout << "usage filePath tupleCnt threadCnt" << endl;

  char* filePath = "file.bin";
  size_t tupleCnt = 1000;
  size_t threadCnt = 1;

  if(argc == 4)
  {
    filePath = argv[1];
    tupleCnt = atoi(argv[2]);
    threadCnt = atoi(argv[3]);
    cout << "using parameter filepath=" << filePath << " tupleCnt=" << tupleCnt << " threadCnt=" << threadCnt << endl;
  }
  else
  {
    cout << "not enough parameters" << endl;
  }

  std::vector<string> fileNames;
  for(size_t i = 0; i < threadCnt; i++)
  {
    std::string path(filePath);
    path = path.substr(0,path.find("."));
    path.append(to_string(i));
    path.append(".bin");
    fileNames.push_back(path);
    cout << "fileName " << i << "=" << path << endl;
  }

  std::random_device rd;  //Will be used to obtain a seed for the random number engine
  std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
  size_t* ad_ids = new size_t[tupleCnt];
  size_t* page_ids = new size_t[tupleCnt];
  size_t* user_ids = new size_t[tupleCnt];
  std::uniform_int_distribution<size_t> ads_distribution(0, 1000);
  std::uniform_int_distribution<size_t> distr(0, 1000000);

  record** recs = new record*[threadCnt];
  for(size_t i = 0; i < threadCnt; i++)
  {
    ad_ids[i] = ads_distribution(gen);
    page_ids[i] = distr(gen);
    user_ids[i] = distr(gen);
    recs[i] = new record[tupleCnt];
  }

#pragma omp parallel num_threads(threadCnt)
  {
#pragma omp for
    for(size_t th = 0; th < threadCnt; th++)
    {
      for(size_t i = 0; i < tupleCnt; i++)
      {
        generate(recs[omp_get_thread_num()][i], ad_ids[i], page_ids[i], user_ids[i], i);
        //cout << "thread=" << omp_get_thread_num() << " put in " << i << " value=" << recs[omp_get_thread_num()][i].event_type << endl;
      }

//      shuffle(recs[omp_get_thread_num()], tupleCnt);

      //write to file
      cout << "writing file" << fileNames[omp_get_thread_num()] << endl;
      std::ofstream ofp(fileNames[omp_get_thread_num()], std::ios::out | std::ios::binary);
      ofp.write(reinterpret_cast<const char*>(recs[omp_get_thread_num()]), tupleCnt * sizeof(record));
      ofp.close();

      //DBG read first 10
      //		record* inRecs = new record[tupleCnt];
      //		std::ifstream ifp(fileNames[omp_get_thread_num()], std::ios::in | std::ios::binary);
      //		ifp.read(reinterpret_cast<char*>(inRecs), tupleCnt * sizeof(record));
      //		for(size_t i = 0; i < tupleCnt; i++)
      //		{
      //			cout << "eventtype " << i << "=" << inRecs[i].event_type << endl;
      //		}
      //		ifp.close();
    }

  }
}