//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// exception.cpp
//
// Identification: src/common/exception.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "common/exception.h"

namespace peloton_peloton {

std::ostream &operator<<(std::ostream &os, const peloton_peloton::Exception &e) {
  os << e.exception_message_.c_str();
  return os;
}

}  // namespace peloton
