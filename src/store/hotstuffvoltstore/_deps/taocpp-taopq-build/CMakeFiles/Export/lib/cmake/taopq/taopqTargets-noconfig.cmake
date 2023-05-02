#----------------------------------------------------------------
# Generated CMake target import file.
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "taocpp::taopq" for configuration ""
set_property(TARGET taocpp::taopq APPEND PROPERTY IMPORTED_CONFIGURATIONS NOCONFIG)
set_target_properties(taocpp::taopq PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_NOCONFIG "CXX"
  IMPORTED_LOCATION_NOCONFIG "${_IMPORT_PREFIX}/lib/libtaopq.a"
  )

list(APPEND _IMPORT_CHECK_TARGETS taocpp::taopq )
list(APPEND _IMPORT_CHECK_FILES_FOR_taocpp::taopq "${_IMPORT_PREFIX}/lib/libtaopq.a" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
