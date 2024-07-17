cmake_minimum_required(VERSION 3.14)
project(hotstuffpg)

include(FetchContent)
find_package(PostgreSQL REQUIRED)
FetchContent_Declare(
   taocpp-taopq
   GIT_REPOSITORY https://github.com/taocpp/taopq
   GIT_TAG main
)
FetchContent_MakeAvailable(taocpp-taopq)


add_executable(hotstuffpg main.cc)
target_link_libraries(hotstuffpg taocpp::taopq)
set_property(TARGET hotstuffpg PROPERTY CXX_STANDARD 20)
