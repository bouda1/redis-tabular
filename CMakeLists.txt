##
## BSD 3-Clause License
##
## Copyright (c) 2018, David Boucher
## All rights reserved.
##
## Redistribution and use in source and binary forms, with or without
## modification, are permitted provided that the following conditions are met:
##
## * Redistributions of source code must retain the above copyright notice,
##   this list of conditions and the following disclaimer.
##
## * Redistributions in binary form must reproduce the above copyright notice,
##   this list of conditions and the following disclaimer in the documentation
##   and/or other materials provided with the distribution.
##
## * Neither the name of the copyright holder nor the names of its
##   contributors may be used to endorse or promote products derived from
##   this software without specific prior written permission.
##
## THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
## AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
## IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
## ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
## LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
## CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
## SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
## INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
## CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
## ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
## POSSIBILITY OF SUCH DAMAGE.
##
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
