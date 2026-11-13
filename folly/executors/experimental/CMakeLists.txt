cmake_minimum_required(VERSION 3.20)
project(folly_affinity_executor_experiment LANGUAGES CXX)

# Standalone test project; does not alter Folly's normal build or public defaults.
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
find_package(folly CONFIG REQUIRED)
find_package(GTest REQUIRED)
find_package(Threads REQUIRED)
get_filename_component(FOLLY_EXPERIMENTAL_SOURCE_ROOT
  "${CMAKE_CURRENT_LIST_DIR}/../../.." ABSOLUTE)

add_executable(affinity_executor_test test/AffinityExecutorTest.cpp)
target_include_directories(affinity_executor_test BEFORE PRIVATE
  "${FOLLY_EXPERIMENTAL_SOURCE_ROOT}")
target_link_libraries(affinity_executor_test PRIVATE
  Folly::folly GTest::gtest_main Threads::Threads)
target_compile_options(affinity_executor_test PRIVATE -Wall -Wextra)
enable_testing()
add_test(NAME affinity_executor_test COMMAND affinity_executor_test)
set_tests_properties(affinity_executor_test PROPERTIES TIMEOUT 60)
