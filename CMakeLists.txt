cmake_minimum_required(VERSION 3.9)
project(redistabular VERSION 1.0.0 DESCRIPTION "Redis Tabular module")
option(COVERAGE "Add code coverage on unit tests." OFF)
set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/CMakeScripts)

message("MODULE PATH ${CMAKE_MODULE_PATH}")

if (COVERAGE)
  set(CMAKE_BUILD_TYPE "Debug")
  include("CodeCoverage")
  APPEND_COVERAGE_COMPILER_FLAGS()

#set(COVERAGE_EXCLUDES '${PROJECT_BINARY_DIR}/tests/*' '*/test/*')
  SETUP_TARGET_FOR_COVERAGE(
      NAME test-coverage
      EXECUTABLE /usr/bin/python ${CMAKE_SOURCE_DIR}/tests/tabular.py
      DEPENDENCIES build/redistabular.so
  )
endif()

add_library(redistabular SHARED
    src/module.c
    src/redismodule.h
)
# This to add -fPIC
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# This to remove the lib prefix
set_target_properties(redistabular PROPERTIES PREFIX "")

enable_testing()
add_test(tests python ${CMAKE_SOURCE_DIR}/tests/tabular.py)
