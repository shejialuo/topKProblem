enable_testing()

add_executable(
  topKRestrictedTest
  topKRestrictedTest.cpp
)

target_link_libraries(
  topKRestrictedTest
  GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(topKRestrictedTest)
