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

#ifndef _SINTR_POLICY_CLIENT_H_
#define _SINTR_POLICY_CLIENT_H_

#include "store/sintrstore/policy/policy.h"

#include <set>
#include <vector>

namespace sintrstore {

// a policy client serves as a wrapper around the abstract policy class
// it is used to track the current policy for a transaction
// assume that all policies are of the same type
class PolicyClient {
 public:
  PolicyClient() : policy(nullptr) {};
  ~PolicyClient();

  // does endorsements satisfy this PolicyClient object?
  bool IsSatisfied(const std::set<uint64_t> &endorsements) const;
  // add a policy to the current transaction policies
  void AddPolicy(const Policy *other);
  // what client ids does potentialEndorsements need to get this policy satisfied?
  std::vector<int> DifferenceToSatisfied(const std::set<uint64_t> &potentialEndorsements) const;
  // is this policy implied by other?
  bool IsImpliedBy(const Policy *other) const;
  void Reset();

 private:
  Policy *policy;
};

} // namespace sintrstore

#endif /* _SINTR_POLICY_CLIENT_H_ */
