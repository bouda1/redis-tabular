cmake_minimum_required(VERSION 3.9)
project(redistabular VERSION 1.0.0 DESCRIPTION "Redis Tabular module")

add_library(redistabular SHARED
    module.c
    redismodule.h
)
# This to add -fPIC
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# This to remove the lib prefix
set_target_properties(redistabular PROPERTIES PREFIX "")
