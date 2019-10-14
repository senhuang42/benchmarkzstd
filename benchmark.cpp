#ifndef ZSTD_STATIC_LINKING_ONLY
#define ZSTD_STATIC_LINKING_ONLY
#endif
#ifndef ZDICT_STATIC_LINKING_ONLY
#define ZDICT_STATIC_LINKING_ONLY
#endif

#include "zstd-finalnewpath/lib/zstd.h"
#include <algorithm>
#include <dirent.h>
#include <fstream>
#include <iostream>
#include <map>
#include <numeric>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <sys/resource.h>
#include <sys/time.h>
#include <tuple>
#include <unordered_map>
#include <vector>
#include <thread>
// g++ -std=c++11 benchmark.cpp zstd-baseline/lib/libzstd.a -o zstdbase && g++ -std=c++11 benchmark.cpp zstd-finalnewpath/lib/libzstd.a -o zstdnew

using namespace std;

vector<string> BASEFILES = {
    "dickens",
    "mozilla",
    "mr",
    "nci",
    "ooffice",
    "osdb",
    "samba",
    "reymont",
    "webster",
    "sao",
    "xml",
    "x-ray"
};
vector<int> DICTSIZES = {
    2000,
    8000,
    16000,
    32000,
    48000,
    64000,
    86000,
    112640,
    140000,
    180000
};
vector<int> COMPLEVELS = {1, 2, 3, 4, 5};

vector<int> FILESIZES = {
    16000,
    32000,
    64000,
    128000,
    256000,
    512000,
    1000000,
    2000000,
    4000000,
    6000000
};

namespace std {

template <class T> inline void hash_combine(std::size_t &seed, T const &v) {
  seed ^= hash<T>()(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
}

template <class Tuple, size_t Index = std::tuple_size<Tuple>::value - 1>
struct HashValueImpl {
  static void apply(size_t &seed, Tuple const &tuple) {
    HashValueImpl<Tuple, Index - 1>::apply(seed, tuple);
    hash_combine(seed, get<Index>(tuple));
  }
};

template <class Tuple> struct HashValueImpl<Tuple, 0> {
  static void apply(size_t &seed, Tuple const &tuple) {
    hash_combine(seed, get<0>(tuple));
  }
};

template <typename... TT> struct hash<std::tuple<TT...>> {
  size_t operator()(std::tuple<TT...> const &tt) const {
    size_t seed = 0;
    HashValueImpl<std::tuple<TT...>>::apply(seed, tt);
    return seed;
  }
};
} // namespace std

struct BenchContext{
  char** fileNames;
  size_t nbFileNames;
  void** dstBuffers;
  size_t* dstCapacities;
  void** srcBuffers;
  size_t* srcSizes;
  size_t totalSrcSize;
};

struct Stats {
  Stats(double mean, double median, double stddev, double ci)
      : mean(mean), median(median), stddev(stddev), ci(ci) {}
  double mean;
  double median;
  double stddev;
  double ci;
  void print() { cout << "mean: " << mean << " stddev: " << stddev << " ci: " << ci << endl; }
};

void printStats(vector<Stats> stats, bool verbose) {
  for (int i = 0; i < stats.size(); ++i) {
    if (i == 0 && verbose) {
      cout << "compressedSize: ";
      stats[i].print();
    } else if (i == 1 && verbose) {
      cout << "srcSize: ";
      stats[i].print();
    } else if (i == 2) {
      cout << "timeSpent: ";
      stats[i].print();
    } else if (i == 3) {
      cout << "MBPS: ";
      stats[i].print();
    } else if (i == 4) {
      cout << "RATIO: ";
      stats[i].print();
    }
  }
}

// stats are 0: compressed size, 1: src size, 2, time to compress
vector<Stats> computeStats(const vector<double> &compressedSizes,
                           const vector<double> &srcSizes,
                           const vector<double> &allTimes,
                           const vector<double> &mbpses,
                           const vector<double> &ratios) {
  vector<vector<double>> thingstocompute;
  vector<Stats> ret;
  thingstocompute.push_back(compressedSizes);
  thingstocompute.push_back(srcSizes);
  thingstocompute.push_back(allTimes);
  thingstocompute.push_back(mbpses);
  thingstocompute.push_back(ratios);

  for (auto &v : thingstocompute) {
    double sum = std::accumulate(v.begin(), v.end(), 0.0);
    double mean = sum / v.size();
    std::vector<double> diff(v.size());
    std::transform(v.begin(), v.end(), diff.begin(),
                   [mean](double x) { return x - mean; });
    double sq_sum =
        std::inner_product(diff.begin(), diff.end(), diff.begin(), 0.0);
    double stdev = std::sqrt(sq_sum / (v.size()-1));
    double ci = 1.96* stdev / std::sqrt((double)v.size());
    // TODO: include median
    Stats statobj(mean, 0.0, stdev, ci);
    ret.push_back(statobj);
  }
  return ret;
}
/*
vector<string> getFilesInDirectory(string dirstr) {
  DIR *dir;
  vector<string> ret;
  struct dirent *ent;
  if ((dir = opendir(dirstr.c_str())) != NULL) {
    while ((ent = readdir(dir)) != NULL) {
      string str = string(ent->d_name);
      if (str.size() > 3) {
        ret.push_back(str);
      }
    }
    closedir(dir);
  } else {
    perror("");
  }
  return ret;
}*/

struct intermediateResultValue {
  intermediateResultValue(double a, size_t b, size_t c, double d, double e)
      : mbps(a), compressedSizeTotal(b), origSizeTotal(c), ratio(d),
        totalTime(e) {}
  intermediateResultValue()
      : mbps(0), compressedSizeTotal(0), origSizeTotal(0), ratio(0.0),
        totalTime(0) {}
  void print() {
    cout << mbps << " " << compressedSizeTotal << " " << origSizeTotal << " "
         << ratio << " " << totalTime << endl;
  }
  double mbps;
  size_t compressedSizeTotal;
  size_t origSizeTotal;
  double ratio;
  double totalTime;
};

// key : {filename, dictsize, complevel} value: {mbps, compressedSize, origSize,
// ratio, totalTime (spent on all runs)}
unordered_map<tuple<string, size_t, int>, intermediateResultValue>
    SEPARATERESULTS;

// key : {dictsize, complevel} value : {mbps, ratio}
map<pair<int, int>, vector<double>> CONSOLIDATEDRESULTS;

string BASEDIRECTORY;
int NUMREPS;
int WARMUPREPS;

#define FILENAMES_LIMIT (1000000)
#define MIN_FILENAME_LENGTH (1)

/* Util*/
static double Util_getTime()
{
    struct timeval t;
    struct timezone tzp;
    gettimeofday(&t, &tzp);
    return t.tv_sec + t.tv_usec*1e-6;
}
static char* Util_concat(char* a, char* b)
{
    char* c = (char*) malloc(strlen(a) + strlen(b) + 1);
    strcpy(c, a);
    strcat(c, b);
    return c;
}
static size_t Util_fileNamesFromDirPath(char* dirPath, char** fileNames)
{
    struct dirent* entry;
    DIR* dir = opendir(dirPath);
    size_t nbFileNames = 0;
    while ((entry = readdir(dir)) && nbFileNames < FILENAMES_LIMIT) {
        if (strlen(entry->d_name) > MIN_FILENAME_LENGTH) {
            fileNames[nbFileNames++] = Util_concat(dirPath, entry->d_name);
        }
    }
    closedir(dir);
    return nbFileNames;
}
static size_t Util_fileSize(char* fileName) {
    FILE* f = fopen(fileName, "r");
    fseek(f, 0, SEEK_END);
    size_t size = ftell(f);
    fclose(f);
    return size;
}
static void Util_readFile(char* fileName, void* buffer, size_t size)
{
    FILE* f = fopen(fileName, "r");
    fread(buffer, sizeof(char), size, f);
    fclose(f);
}
static void Util_initCompressionBuffers(char** fileNames,
    size_t nbFileNames, void** dstBuffers, size_t* dstCapacities,
    void** srcBuffers, size_t* srcSizes)
{
    size_t i;
    for (i = 0; i < nbFileNames; ++i) {
        srcSizes[i] = Util_fileSize(fileNames[i]);
        srcBuffers[i] = (char*)malloc(sizeof(char) * srcSizes[i]);
        Util_readFile(fileNames[i], srcBuffers[i], srcSizes[i]);
        dstCapacities[i] = ZSTD_compressBound(srcSizes[i]);
        dstBuffers[i] = (char*)malloc(sizeof(char) * dstCapacities[i]);
    }
}

/* Create */
static BenchContext* BenchContext_create(char* dirPath)
{
    if (dirPath == NULL) return NULL;
    BenchContext* benchContext = (BenchContext*) malloc(sizeof(BenchContext));
    benchContext->fileNames = (char**)malloc(sizeof(char*) * 1000000);
    benchContext->nbFileNames = Util_fileNamesFromDirPath(dirPath,
        benchContext->fileNames);

    benchContext->dstBuffers = (void**)malloc(sizeof(void*) * benchContext->nbFileNames);
    benchContext->dstCapacities = (size_t*)malloc(sizeof(size_t) * benchContext->nbFileNames);
    benchContext->srcBuffers = (void**)malloc(sizeof(void*) * benchContext->nbFileNames);
    benchContext->srcSizes = (size_t*)malloc(sizeof(size_t) * benchContext->nbFileNames);
    Util_initCompressionBuffers(benchContext->fileNames,
        benchContext->nbFileNames, benchContext->dstBuffers,
        benchContext->dstCapacities, benchContext->srcBuffers,
        benchContext->srcSizes);
    size_t i;
    size_t size = 0;
    for (i = 0; i < benchContext->nbFileNames; ++i) {
        size += benchContext->srcSizes[i];
    }
    benchContext->totalSrcSize = size;
    return benchContext;
}

/* Free */
static void BenchContext_freeFileNames(char** fileNames, size_t nbFileNames)
{
    size_t i;
    for (i = 0; i < nbFileNames; ++i) {
        free(fileNames[i]);
    }
    free(fileNames);
}
static void BenchContext_freeCompressionBuffers(void** dstBuffers,
    size_t* dstCapacities, void** srcBuffers, size_t* srcSizes, size_t nbFileNames)
{
    size_t i;
    for (i = 0; i < nbFileNames; ++i) {
        free(dstBuffers[i]);
        free(srcBuffers[i]);
    }
    free(dstBuffers);
    free(dstCapacities);
    free(srcBuffers);
    free(srcSizes);
}
static void BenchContext_free(BenchContext* benchContext)
{
    if (benchContext == NULL) return;
    BenchContext_freeFileNames(benchContext->fileNames,
        benchContext->nbFileNames);
    BenchContext_freeCompressionBuffers(benchContext->dstBuffers,
        benchContext->dstCapacities, benchContext->srcBuffers,
        benchContext->srcSizes, benchContext->nbFileNames);
    free(benchContext);
}

static bool NODICT = false;

#define MAX_FILENAMES 100000


void benchWarmups(BenchContext* benchContext, void* dictBuf, size_t dictSize, int compLevel) {
  auto cctx = ZSTD_createCCtx();
  auto cdict = ZSTD_createCDict(dictBuf, dictSize, compLevel);
  for (int u = 1; u <= WARMUPREPS; ++u) {
    vector<double> statsForThisDirectoryInOneRun(5, 0.0);
    size_t totalCompressedSize = 0, totalSrcSize = benchContext->totalSrcSize;
    double timeBefore, timeElapsed;
    if (NODICT) {
      size_t i = 0;
      timeBefore = Util_getTime();
      for (; i < benchContext->nbFileNames; ++i) {
        totalCompressedSize += ZSTD_compressCCtx(cctx, benchContext->dstBuffers[i],
                              benchContext->dstCapacities[i],
                              benchContext->srcBuffers[i],
                              benchContext->srcSizes[i], compLevel);
      }
      timeElapsed = Util_getTime() - timeBefore;
    } else {
      size_t i = 0;
      timeBefore = Util_getTime();
      for (; i < benchContext->nbFileNames; ++i) {
        totalCompressedSize += ZSTD_compress_usingCDict(cctx,
                              benchContext->dstBuffers[i],
                              benchContext->dstCapacities[i],
                              benchContext->srcBuffers[i],
                              benchContext->srcSizes[i],
                              cdict);
      }
      timeElapsed = Util_getTime() - timeBefore;
    }
  }
  ZSTD_freeCCtx(cctx);
  ZSTD_freeCDict(cdict);
}

vector<Stats> benchADirectory(BenchContext* benchContext, void* dictBuf, size_t dictSize, int compLevel) {
  // a bunch of reps to get our stats results
  vector<double> totalTimes;
  vector<double> compSizes;
  vector<double> srcSizes;
  vector<double> mbpses;
  vector<double> ratios;
  auto cctx = ZSTD_createCCtx();
  auto cdict = ZSTD_createCDict(dictBuf, dictSize, compLevel);
  benchWarmups(benchContext, dictBuf, dictSize, compLevel);
  for (int u = 1; u <= NUMREPS; ++u) {
    vector<double> statsForThisDirectoryInOneRun(5, 0.0);
    size_t totalCompressedSize = 0, totalSrcSize = benchContext->totalSrcSize;
    double timeBefore, timeElapsed;
    if (NODICT) {
      size_t i = 0;
      timeBefore = Util_getTime();
      for (; i < benchContext->nbFileNames; ++i) {
        totalCompressedSize += ZSTD_compressCCtx(cctx, benchContext->dstBuffers[i],
                              benchContext->dstCapacities[i],
                              benchContext->srcBuffers[i],
                              benchContext->srcSizes[i], compLevel);
      }
      timeElapsed = Util_getTime() - timeBefore;
    } else {
      size_t i = 0;
      timeBefore = Util_getTime();
      for (; i < benchContext->nbFileNames; ++i) {
        totalCompressedSize += ZSTD_compress_usingCDict(cctx,
                              benchContext->dstBuffers[i],
                              benchContext->dstCapacities[i],
                              benchContext->srcBuffers[i],
                              benchContext->srcSizes[i],
                              cdict);
      }
      timeElapsed = Util_getTime() - timeBefore;
    }
    totalTimes.push_back((double)timeElapsed);
    compSizes.push_back((double)totalCompressedSize);
    srcSizes.push_back((double)totalSrcSize);
    mbpses.push_back((double)totalSrcSize / 1000000 / timeElapsed);
    ratios.push_back((double)totalSrcSize / (double)totalCompressedSize);
  }
  ZSTD_freeCCtx(cctx);
  ZSTD_freeCDict(cdict);
  auto aggResults =
      computeStats(compSizes, srcSizes, totalTimes, mbpses, ratios);
  //printStats(aggResults, false);
  return aggResults;
}

void writeToAFile(string location, int filesize, int res, vector<Stats> stats) {
  ofstream myfile;
  myfile.open(location.c_str(), ios::app);
  myfile << filesize << "," << stats[res].mean << "," << stats[res].ci
         << "\n";
}

void writeToAFileRatio(string location, int filesize, int res, vector<Stats> stats) {
  ofstream myfile;
  myfile.open(location.c_str(), ios::app);
  myfile << filesize << "," << stats[res].mean << "\n";
}

void benchParallel(const char* locationToSave, bool dict, string baseFilename) {
  string locationTosv = locationToSave;
  cout << "Working on: " << baseFilename << endl;
  for (auto dictSize : DICTSIZES) {
      //cout << "dictSize: " << dictSize << endl;
      string fulldictpath = BASEDIRECTORY + std::string("/dicts2/") + baseFilename +
          "dict" + to_string(dictSize);
      char *dictbuf = new char[dictSize];
      char *pathbuf = strdup(fulldictpath.c_str());
      Util_readFile(pathbuf, dictbuf, dictSize);

      for (auto compLevel : COMPLEVELS) {
        //cout << "compLevel: " << compLevel << endl;
        for (auto filesize : FILESIZES) {
          //cout << "filesize: " << filesize << endl;
          string dirname = BASEDIRECTORY + string("data/") + baseFilename +
                           to_string(filesize) + string("/");
          char *dirbuf = strdup(dirname.c_str());
          auto bc = BenchContext_create(dirbuf);
          auto res = benchADirectory(bc, dictbuf, dictSize, compLevel);
          // save the results
          writeToAFile(locationTosv + baseFilename + "_" + to_string(dictSize) + "_" + to_string(compLevel) + "_" + "mbps", filesize, 3, res);
          writeToAFileRatio(locationTosv + baseFilename + "_" + to_string(dictSize) + "_" + to_string(compLevel) + "_" + "ratio", filesize, 4, res);
          BenchContext_free(bc);
          free(dirbuf);
        }
      }

      delete[] pathbuf;
      free(dictbuf);
  }
}

void benchAll(const char *locationToSave, bool dict) {
  // results are: file, filesize, dictsize, compressionlevel -> compSize,
  // srcSize, totalTime, mbps, ratio
  
  vector<thread> threads;
  for (const auto& each : BASEFILES) {
    threads.push_back(thread([=]() { return benchParallel(locationToSave, dict, each); }));
  }

  for (auto& t : threads) {
    t.join();
  }
}

int main(int argc, char **argv) {
  if (strlen(argv[5]) > 2) {
    NODICT = true;
  }
  cout << "Benchmarking...\n";
  cout << "Doing " << argv[1] << " warmup repetitions\n";
  cout << "Doing " << argv[2] << " benchmark repetitions\n";
  WARMUPREPS = stoi(argv[1]);
  NUMREPS = stoi(argv[2]);
  BASEDIRECTORY = argv[3];
  benchAll(argv[4], false);
  // saveResults(argv[4]);
}

/*
void consolidateResults() {
  cout << "Consolidating..." << endl;
  size_t allTotalSize = 0;
  // first make map of : (dictSize, compLevel) -> (totalSizes, totalCompSizes,
  // totalTime); one entry per (dictSize, compLevel pair) say we have files x,
  // y, z that took a, b, c time to compress average decomperssion speed is
  // still sizeof(x+y+Z) / (a+b+c) so we get rid of filenames here essentially
  for (auto &each : SEPARATERESULTS) {
    each.second.print();
    auto key = make_pair(get<1>(each.first), get<2>(each.first));
    vector<double> tempResult = {(double)each.second.origSizeTotal,
                                 (double)each.second.compressedSizeTotal,
                                 (double)each.second.totalTime};
    auto it = CONSOLIDATEDRESULTS.find(key);
    if (it == CONSOLIDATEDRESULTS.end()) {
      CONSOLIDATEDRESULTS.emplace(std::move(key), std::move(tempResult));
    } else {
      it->second[0] += tempResult[0];
      it->second[1] += tempResult[1];
      it->second[2] += tempResult[2];
    }
  }

  cout << "Finalizing..." << endl;
  // then make map of : (dictSize, compLevel) -> (mbps, ratio)
  for (auto &each : CONSOLIDATEDRESULTS) {
    double mbps = (each.second[0] / 1000000) / each.second[2];
    double ratio = (each.second[0] / each.second[1]);
    vector<double> res = {mbps, ratio};
    each.second.push_back(mbps);
    each.second.push_back(ratio);

    cout << "size: " << each.first.first << " level: " << each.first.second
         << " --- " << mbps << " mbps, and " << ratio << " ratio" << endl;
  }
}*/