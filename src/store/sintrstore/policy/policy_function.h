/***********************************************************************
 *
 * Copyright 2024 Austin Li <atl63@cornell.edu>
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 **********************************************************************/

#ifndef _SINTR_POLICY_FUNCTION_H_
#define _SINTR_POLICY_FUNCTION_H_

#include "store/sintrstore/policy/policy.h"

#include <string>

namespace sintrstore {

// a policy function takes in the key and value and returns a new policy object
typedef std::function<Policy *(const std::string &, const std::string &)> policy_function;

// a policy id function takes in the key and value and returns a policy id
typedef std::function<uint64_t(const std::string &, const std::string &)> policy_id_function;

// function that takes in a policy function name and returns the corresponding policy function
inline policy_id_function GetPolicyIdFunction(const std::string &policy_function_name) {
  if (policy_function_name == "basic_id") {
    return [](const std::string &key, const std::string &value) -> uint64_t {
      return 0;
    };
  }
  else {
    Panic("Unknown policy function name %s", policy_function_name.c_str());
  }
}

} // namespace sintrstore

#endif /* _SINTR_POLICY_FUNCTION_H_ */
