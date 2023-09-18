#include "topKBasic.hpp"

#include <functional>
#include <queue>
#include <vector>

static int partition(std::vector<int> &data, int start, int end) {
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

std::vector<int> topKUsingPartition(std::vector<int> &data, int k) {
  int index = helper(data, k, 0, data.size() - 1);
  return std::vector<int>{data.cbegin(), data.cbegin() + index + 1};
}

std::vector<int> topKUsingHeap(const std::vector<int> &data, int k) {
  std::vector<int> ans(k, 0);

  std::priority_queue<int, std::vector<int>, std::greater<int>> heap{};

  for (int i = 0; i < k; i++) {
    heap.push(data[i]);
  }

  for (int i = k; i < data.size(); i++) {
    if (data[i] > heap.top()) {
      heap.pop();
      heap.push(data[i]);
    }
  }

  for (int i = k - 1; i >= 0; i--) {
    ans[i] = heap.top();
    heap.pop();
  }

  return ans;
}
