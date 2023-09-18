enable_testing()

add_executable(
  topKBasicTest
  topKBasicTest.cpp
)

target_include_directories(topKBasicTest PRIVATE ../)

target_link_libraries(
  topKBasicTest
  topKBasic
  GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(topKBasicTest)
