#include "zstd.h"
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
#include <algorithm>

// g++ -std=c++11 benchmark.cpp zstd-baseline/lib/libzstd.a

using namespace std;

vector<string> BASEFILES = {
  /*"dickens",
  "mozilla",
  "mr",
  "nci",
  "ooffice",
  "osdb",
  "samba",
  "reymont",*/
  "webster",
  //"sao",
  //"xml",
  //"x-ray"
};
vector<int> DICTSIZES = {
     2000,
    // 8000,
    // 16000,
    // 32000,  48000,
    // 64000,
    // 86000,
    112640,
    // 140000, 180000
};
vector<int> COMPLEVELS = {-1};

// in kilobytes
vector<int> FILESIZES = {
  64000,
  //128000,
  //256000,
  //512000,
  //1000000,
  //2000000,
  4000000,
  //6000000,
};


namespace std {

template <class T> inline void hash_combine(std::size_t &seed, T const &v) {
  seed ^= hash<T>()(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
}

// Recursive template code derived from Matthieu M.
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

struct Stats {
  Stats(double mean, double median, double stddev)
      : mean(mean), median(median), stddev(stddev) {}
  double mean;
  double median;
  double stddev;
  void print() {
    cout << "mean: " << mean << " stddev: " << stddev << endl;
  }
};

// stats are 0: compressed size, 1: src size, 2, time to compress
vector<Stats> computeStats(const vector<double>& compressedSizes, const vector<double>& srcSizes, const vector<double>& allTimes, const vector<double>& mbpses, const vector<double>& ratios) {
  vector<vector<double>> thingstocompute;
  vector<Stats> ret;
  thingstocompute.push_back(compressedSizes);
  thingstocompute.push_back(srcSizes);
  thingstocompute.push_back(allTimes);
  thingstocompute.push_back(mbpses);
  thingstocompute.push_back(ratios);

  for (auto& v : thingstocompute) {
    double sum = std::accumulate(v.begin(), v.end(), 0.0);
    double mean = sum / v.size();
    std::vector<double> diff(v.size());
    std::transform(v.begin(), v.end(), diff.begin(), [mean](double x) { return x - mean; });
    double sq_sum = std::inner_product(diff.begin(), diff.end(), diff.begin(), 0.0);
    double stdev = std::sqrt(sq_sum / v.size());

    // TODO: include median
    Stats statobj(mean, 0.0, stdev);
    ret.push_back(statobj);
  }
  return ret;
}

vector<string> getFilesInDirectory(string dirstr) {
  DIR *dir;
  vector<string> ret;
  struct dirent *ent;
  if ((dir = opendir(dirstr.c_str())) != NULL) {
    /* print all the files and directories within directory */
    while ((ent = readdir(dir)) != NULL) {
      string str = string(ent->d_name);
      if (str.size() > 3) {
        ret.push_back(str);
      }
    }
    closedir(dir);
  } else {
    /* could not open directory */
    perror("");
  }
  return ret;
}

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

static double Util_getTime() {
  struct timeval t;
  struct timezone tzp;
  gettimeofday(&t, &tzp);
  return t.tv_sec + t.tv_usec * 1e-6;
}

static size_t UTIL_fileSize(const char *fileName) {
  FILE *f = fopen(fileName, "r");
  fseek(f, 0, SEEK_END);
  size_t size = ftell(f);
  fclose(f);
  return size;
}

static void Util_readFile(const char *fileName, void *buffer, size_t size) {
  FILE *f = fopen(fileName, "r");
  fread(buffer, sizeof(char), size, f);
  fclose(f);
}

void fileToBufs(string baseFilename, int dictSize, char *databuf,
                char *dictbuf) {}

void saveResults(const char *savefile) {
  cout << "Saving results..." << endl;
  for (const auto &each : CONSOLIDATEDRESULTS) {
    size_t dictSize = each.first.first;
    int compLevel = each.first.second;
    double mbps = each.second[3];
    double ratio = each.second[4];
    printf("At dictisze:%lu, compLevel:%s -> %lf mbps, %lf ratio, \n", dictSize,
           compLevel, mbps, ratio);
  }
}

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
}

void benchOneFile(string basefilename, string fullfilepath, size_t dictsize, int compLevel, vector<double>& rawCompSize, vector<double>& rawSrcSize, vector<double>& rawTotalTime, vector<double>& rawMbps, vector<double>& rawRatio) {

  // full data and dict buffers=
  size_t datasize = UTIL_fileSize(fullfilepath.c_str());
  char *databuf = new char[datasize];
  Util_readFile(fullfilepath.c_str(), databuf, datasize);

  string fulldictpath = BASEDIRECTORY + std::string("/dicts/") + basefilename +
                        "dict" + to_string(dictsize);
  char *dictbuf = new char[dictsize];
  Util_readFile(fulldictpath.c_str(), dictbuf, dictsize);

  // begin compressions
  ZSTD_CDict *cdict = ZSTD_createCDict(dictbuf, dictsize, compLevel);
  ZSTD_CCtx *cctx = ZSTD_createCCtx();
  vector<double> compCollection, srcCollection, timeCollection, mbpsCollection, ratioCollection;
  double totalTime, timeBefore, timeIncrement;
  size_t i = 1, compressedSize, compressedSizeTotal = 0, srcSizeTotal = 0;
  for (; i <= NUMREPS; ++i) {
    size_t dstsize = ZSTD_compressBound(datasize);
    char *dstbuf = new char[dstsize];
    timeBefore = Util_getTime();
    compressedSize = ZSTD_compress_usingCDict(cctx, dstbuf, dstsize, databuf,
                                              datasize, cdict);
    timeIncrement = Util_getTime() - timeBefore;

    if (i > WARMUPREPS) {
      timeCollection.push_back(timeIncrement);
      compCollection.push_back((double)compressedSize);
      srcCollection.push_back((double)datasize);
      mbpsCollection.push_back((double)datasize/1000000/timeIncrement);
      ratioCollection.push_back((double)datasize/(double)compressedSize);
    }
    delete[] dstbuf;
  }

  auto stats = computeStats(compCollection, srcCollection, timeCollection, mbpsCollection, ratioCollection);
  auto compMean = stats[0].mean;
  auto srcMean = stats[1].mean;
  auto timeMean = stats[2].mean;
  auto mbps = stats[3].mean;
  auto ratio = stats[4].mean;

  rawCompSize.push_back(compMean);
  rawSrcSize.push_back(srcMean);
  rawTotalTime.push_back(timeMean);
  rawMbps.push_back(mbps);
  rawRatio.push_back(ratio);
  // average out all the runs
  //totalTime = totalTime / (NUMREPS - WARMUPREPS); // avg time to compress
  //compressedSizeTotal = compressedSizeTotal / (NUMREPS - WARMUPREPS);
  //srcSizeTotal = srcSizeTotal / (NUMREPS - WARMUPREPS);
  //rawCompSize.push_back(compressedSizeTotal);
  //rawSrcSize.push_back(srcSizeTotal);
  //rawTotalTime.push_back(totalTime);
  // compressedSize will always be the same for given dictsize/complevel, so we
  // just use the last one just for fun
  //double mbps = (double)datasize / 1000000 / totalTime;
  //double ratio = (double)datasize / compressedSize;

  //intermediateResultValue val(mbps, compressedSize, datasize, ratio, totalTime);
  //tuple<string, size_t, int> key = make_tuple(fullfilepath, dictsize, compLevel);
  //SEPARATERESULTS.emplace(key, val);

  delete[] databuf;
  delete[] dictbuf;
  ZSTD_freeCCtx(cctx);
  ZSTD_freeCDict(cdict);
}

vector<Stats> benchADirectory(string dirname, string baseFilename, size_t dictSize, int compLevel) {
  vector<string> files = getFilesInDirectory(dirname);
  vector<double> rawCompSize; // compSizeTotal, srcSizeTotal, totalTimeTaken
  vector<double> rawSrcSize;
  vector<double> rawTotalTime;
  vector<double> rawMbps;
  vector<double> rawRatio;
  for (auto filename : files) {
    string fullfilepath = dirname + filename;
    benchOneFile(baseFilename, fullfilepath, dictSize, compLevel, rawCompSize, rawSrcSize, rawTotalTime, rawMbps, rawRatio);
  }

  auto stats = computeStats(rawCompSize, rawSrcSize, rawTotalTime, rawMbps, rawRatio);

  // TODO: make this printing into a function
  for (int i = 0; i < stats.size(); ++i) {
    if (i == 0) {
      cout << "compressedSize: ";
      stats[i].print();
    } else if (i == 1) {
      cout << "srcSize: ";
      stats[i].print();
    } else if (i == 2) {
      cout << "timeSpent: ";
      stats[i].print();
    } else if (i == 3) {
      cout << "MBPS: ";
      stats[i].print();
    } else {
      cout << "RATIO: ";
      stats[i].print();
    }
  }
  return stats;
}

void benchAll() {
  for (auto baseFilename : BASEFILES) {
    cout << baseFilename << endl;
    for (auto dictSize : DICTSIZES) {
      cout << dictSize << endl;
      for (auto compLevel : COMPLEVELS) {
        cout << compLevel << endl;
        for (auto filesize : FILESIZES) {
          string dirname = BASEDIRECTORY + string("/data/") + baseFilename + to_string(filesize) + string("/");
          cout << dirname;
          benchADirectory(dirname, baseFilename, dictSize, compLevel);
        }
      }
    }
  }
  //consolidateResults();
}

int main(int argc, char **argv) {
  cout << "Benchmarking...\n";
  cout << "Doing " << argv[1] << " warmup repetitions\n";
  cout << "Doing " << argv[2] << " benchmark repetitions\n";
  WARMUPREPS = stoi(argv[1]);
  NUMREPS = stoi(argv[2]);
  BASEDIRECTORY = argv[3];
  benchAll();
  saveResults(argv[4]);
}
