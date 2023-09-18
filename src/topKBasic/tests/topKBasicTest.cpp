#include "topKBasic.hpp"

#include <algorithm>
#include <functional>
#include <gtest/gtest.h>
#include <random>
#include <vector>

static std::vector<int> generateRandomNumber() {
  constexpr int length = 100000;

  std::vector<int> data{};

  std::random_device rd;
  std::mt19937 gen{rd()};

  std::uniform_int_distribution<int> dis(1, 1000);

  for (int i = 0; i < length; i++) {
    data.push_back(dis(gen));
  }

  return data;
}

TEST(topKBasicUsingPartition, randomTest) {
  constexpr int times = 500;

  std::random_device rd;
  std::mt19937 gen{rd()};
  std::uniform_int_distribution<int> dis(1, 1000);

  for (int i = 0; i < times; i++) {
    auto data = generateRandomNumber();
    std::vector<int> copy{data};

    int k = dis(gen);
    auto ans = topKUsingPartition(copy, k);
    sort(ans.begin(), ans.end(), std::greater<int>());

    sort(data.begin(), data.end(), std::greater<int>());
    data.erase(data.begin() + k, data.end());

    ASSERT_EQ(ans, data);
  }
}

TEST(topKBasicUsingHeap, randomTest) {
  constexpr int times = 500;

  std::random_device rd;
  std::mt19937 gen{rd()};
  std::uniform_int_distribution<int> dis(1, 1000);

  for (int i = 0; i < times; i++) {
    auto data = generateRandomNumber();

    int k = dis(gen);

    auto ans = topKUsingHeap(data, k);
    sort(ans.begin(), ans.end(), std::greater<int>());

    sort(data.begin(), data.end(), std::greater<int>());
    data.erase(data.begin() + k, data.end());

    ASSERT_EQ(ans, data);
  }
}
