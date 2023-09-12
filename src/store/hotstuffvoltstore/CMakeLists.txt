cmake_minimum_required(VERSION 3.14)
project(hotstuffvolt)

include(FetchContent)
find_package(PostgreSQL REQUIRED)
FetchContent_Declare(
   taocpp-taopq
   GIT_REPOSITORY https://github.com/taocpp/taopq
   GIT_TAG main
)
FetchContent_MakeAvailable(taocpp-taopq)


add_executable(hotstuffvolt main.cc)
target_link_libraries(hotstuffvolt taocpp::taopq)
set_property(TARGET hotstuffvolt PROPERTY CXX_STANDARD 20)
