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
  for (const auto &typePolicy : currPolicies) {
    if (!typePolicy.second->IsSatisfied(endorsements)) {
      return false;
    }
  }
  return true;
}

void PolicyClient::AddPolicy(const Policy *policy) {
  UW_ASSERT(policy != nullptr);
  if (currPolicies.find(policy->Type()) == currPolicies.end()) {
    currPolicies[policy->Type()] = policy->Clone();
  }
  else {
    currPolicies[policy->Type()]->MergePolicy(policy);
  }
}

std::vector<int> PolicyClient::DifferenceToPolicy(const Policy *other) const {
  UW_ASSERT(other != nullptr);
  if (currPolicies.find(other->Type()) == currPolicies.end()) {
    return other->GetMinSatisfyingSet();
  }
  else {
    return currPolicies.at(other->Type())->DifferenceToPolicy(other);
  }
}

bool PolicyClient::IsOtherWeaker(const Policy *other) const {
  UW_ASSERT(other != nullptr);
  if (currPolicies.find(other->Type()) == currPolicies.end()) {
    return false;
  }
  else {
    return currPolicies.at(other->Type())->IsOtherWeaker(other);
  }
}

void PolicyClient::Reset() {
  for (auto &typePolicy : currPolicies) {
    delete typePolicy.second;
  }
  currPolicies.clear();
}

} // namespace sintrstore
