cmake_minimum_required(VERSION 3.16)
project(cloud_file_service CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

add_compile_options(-Wall -Wextra)

add_executable(fileservice src/main.cpp)
add_executable(fileservice_epoll src/epoll_server.cpp)
