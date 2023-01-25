cmake_minimum_required(VERSION 3.14)
project(crdbdriver)

include(FetchContent)
find_package(PostgreSQL REQUIRED)
find_package(Protobuf REQUIRED)

FetchContent_Declare(
    taocpp-taopq
    GIT_REPOSITORY https://github.com/taocpp/taopq
    GIT_TAG main
)
FetchContent_MakeAvailable(taocpp-taopq)

include_directories(./../..)
include_directories(./../../..)
include_directories(${Protobuf_INCLUDE_DIRS})

add_library(CRDB client.cc client.h)

target_link_libraries(CRDB taocpp::taopq)
add_executable(main_test main_test.cc)
target_link_libraries(main_test PRIVATE CRDB)
set_property(TARGET main_test PROPERTY CXX_STANDARD 17)

add_executable(benchClient benchClient.cc)
target_link_libraries(benchClient PRIVATE CRDB)
set_property(TARGET benchClient PROPERTY CXX_STANDARD 17)
