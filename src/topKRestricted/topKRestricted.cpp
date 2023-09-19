#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <queue>
#include <random>
#include <stdexcept>
#include <string>
#include <sys/resource.h>
#include <unordered_map>
#include <vector>

namespace fs = std::filesystem;

constexpr int CHUNK_SIZE = 20 * 1024 * 1024;
constexpr int MEMORY_LIMIT = 30 * 1024 * 1024;
constexpr int FILE_SIZE = 500 * 1024 * 1024;

static int partition(std::vector<int> &data, int start, int end) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<int> dis(start, end);

  int pivotIndex = dis(gen);
  std::swap(data[pivotIndex], data[end]);
  int pivot = data[end];

  int i = start - 1, j = start;
  while (j < end) {
    if (data[j] > pivot) {
      i++;
      std::swap(data[i], data[j]);
    }
    j++;
  }

  std::swap(data[i + 1], data[end]);
  return i + 1;
}

static int helper(std::vector<int> &data, int k, int start, int end) {
  if (start == end) {
    return start;
  }

  int mid = partition(data, start, end);
  int index = mid - start + 1;

  if (index == k) {
    return mid;
  } else if (index > k) {
    return helper(data, k, start, mid - 1);
  } else {
    return helper(data, k - index, mid + 1, end);
  }
}

std::vector<int> topKUsingPartition(std::vector<int> &data, int k, int end) {
  if (k >= end) {
    return std::vector<int>{data.cbegin(), data.cbegin() + end + 1};
  }

  int index = helper(data, k, 0, end - 1);
  return std::vector<int>{data.cbegin(), data.cbegin() + index + 1};
}

fs::path createTempfile() {
  fs::path tempDir = fs::temp_directory_path();
  fs::path tempFile = tempDir / "temp_file.bin";

  std::random_device rd;
  std::mt19937 gen{rd()};
  std::uniform_int_distribution<int> dis(1, 1000);

  std::ofstream file{tempFile, std::ios::binary | std::ios::out | std::ios::trunc};

  if (!file.is_open()) {
    throw std::runtime_error("cannot read temp file");
  }

  std::vector<int> data(FILE_SIZE / sizeof(int));
  for (int i = 0; i < data.size(); i++) {
    data[i] = dis(gen);
  }

  file.write(reinterpret_cast<char *>(data.data()), data.size() * sizeof(int));
  file.close();

  return tempFile;
}

std::vector<int> topKUsingHeapWithRestrictedMemory(fs::path &p, int k) {
  if (!fs::exists(p)) {
    std::string info = "Error: " + p.string() + " does not exist";
    throw std::runtime_error(info);
  }

  std::ifstream file{p, std::ios_base::in};
  if (!file.is_open()) {
    std::string info = "Error: cannot open " + p.string();
    throw std::runtime_error(info);
  }

  std::vector<int> ans(k, 0);
  std::priority_queue<int, std::vector<int>, std::greater<int>> heap{};

  // In here, we maintain 20M int buffer for ints
  std::vector<int> buffer(CHUNK_SIZE / sizeof(int));

  while (file.read(reinterpret_cast<char *>(buffer.data()), CHUNK_SIZE)) {
    std::streamsize bytesRead = file.gcount();
    if (bytesRead > 0) {
      int numElements = bytesRead / sizeof(int);
      for (int i = 0; i < numElements; i++) {
        if (heap.size() < k) {
          heap.push(buffer[i]);
        } else {
          if (buffer[i] > heap.top()) {
            heap.pop();
            heap.push(buffer[i]);
          }
        }
      }
    }
  }
  file.close();

  for (int i = k - 1; i >= 0; i--) {
    ans[i] = heap.top();
    heap.pop();
  }

  return ans;
}

std::vector<int> topKUsingPartitionWithRestrictedMemory(fs::path &p, int k) {
  if (!fs::exists(p)) {
    std::string info = "Error: " + p.string() + " does not exist";
    throw std::runtime_error(info);
  }

  std::ifstream file{p, std::ios_base::in};
  if (!file.is_open()) {
    std::string info = "Error: cannot open " + p.string();
    throw std::runtime_error(info);
  }

  std::vector<int> buffer(CHUNK_SIZE / sizeof(int));
  std::vector<int> nums{};

  while (file.read(reinterpret_cast<char *>(buffer.data()), CHUNK_SIZE)) {
    std::streamsize bytesRead = file.gcount();
    if (bytesRead > 0) {
      int numElements = bytesRead / sizeof(int);
      auto partialK = topKUsingPartition(buffer, k, numElements);
      for (int i = 0; i < partialK.size(); i++) {
        nums.push_back(partialK[i]);
      }
    }
  }
  file.close();

  return topKUsingPartition(nums, k, nums.size());
}

int main(int argc, char *argv[]) {
  auto tempFile = createTempfile();

  struct rlimit limit;
  limit.rlim_cur = MEMORY_LIMIT;
  limit.rlim_max = MEMORY_LIMIT;

  if (setrlimit(RLIMIT_AS, &limit) != 0) {
    throw std::runtime_error("cannot set memory limit");
  }

  double totalSecondsForHeap = 0, totalSecondsForSelect;

  for (int i = 0; i < 10; i++) {
    std::cout << "\n=== Running using heap\n";
    auto start = std::chrono::high_resolution_clock::now();
    auto ansHeap = topKUsingHeapWithRestrictedMemory(tempFile, 100);
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end - start;
    double seconds = elapsed.count();
    std::cout << "Elapsed: " << seconds << " s" << std::endl;
    totalSecondsForHeap += seconds;

    std::cout << "\n=== Running using select\n";
    start = std::chrono::high_resolution_clock::now();
    auto ansSelect = topKUsingPartitionWithRestrictedMemory(tempFile, 100);
    end = std::chrono::high_resolution_clock::now();
    elapsed = end - start;
    seconds = elapsed.count();
    std::cout << "Elapsed: " << seconds << " s" << std::endl;
    totalSecondsForSelect += seconds;
  }

  std::cout << "\n=== Result\n";
  std::cout << "Heap average time: " << totalSecondsForHeap / 10.0 << " s\n";
  std::cout << "Select average time: " << totalSecondsForSelect / 10.0 << " s\n";

  fs::remove(tempFile);

  return 0;
}
