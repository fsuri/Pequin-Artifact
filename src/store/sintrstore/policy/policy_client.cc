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

#include "store/sintrstore/policy/policy_client.h"
#include "lib/assert.h"

namespace sintrstore {

PolicyClient::~PolicyClient() {
  Reset();
}

bool PolicyClient::IsSatisfied(const std::set<uint64_t> &endorsements) const {
  // empty policy is by default satisfied
  if (policy == nullptr) {
    return true;
  }
  return policy->IsSatisfied(endorsements);
}

void PolicyClient::AddPolicy(const Policy *other) {
  UW_ASSERT(other != nullptr);
  if (policy == nullptr) {
    policy = other->Clone();
  }
  else {
    UW_ASSERT(policy->Type() == other->Type());
    policy->MergePolicy(other);
  }
}

std::vector<int> PolicyClient::DifferenceToSatisfied(const std::set<uint64_t> &potentialEndorsements) const {
  // empty policy is automatically satisfied
  if (policy == nullptr) {
    return std::vector<int>();
  }
  return policy->DifferenceToSatisfied(potentialEndorsements);
}

bool PolicyClient::IsImpliedBy(const Policy *other) const {
  UW_ASSERT(other != nullptr);
  // empty policy is always implied by other
  if (policy == nullptr) {
    return true;
  }
  UW_ASSERT(other->Type() == policy->Type());
  return policy->IsImpliedBy(other);
}

void PolicyClient::Reset() {
  delete policy;
  policy = nullptr;
}

} // namespace sintrstore
