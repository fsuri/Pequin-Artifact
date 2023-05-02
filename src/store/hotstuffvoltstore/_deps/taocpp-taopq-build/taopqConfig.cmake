get_filename_component(taopq_CMAKE_DIR "${CMAKE_CURRENT_LIST_FILE}" PATH)
include(CMakeFindDependencyMacro)

list(APPEND CMAKE_MODULE_PATH ${taopq_CMAKE_DIR})

find_package(PostgreSQL REQUIRED MODULE)
list(REMOVE_AT CMAKE_MODULE_PATH -1)

if(NOT TARGET taocpp::taopq)
  include("${taopq_CMAKE_DIR}/taopqTargets.cmake")
endif()

set(taopq_LIBRARIES taocpp::taopq)
