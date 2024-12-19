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

#include "store/sintrstore/policy/policy.h"
#include "lib/assert.h"

namespace sintrstore {

bool Policy::IsSatisfied(const std::set<uint64_t> &endorsements) const {
  for (const auto &typePolicy : mergedPolicies) {
    if (!typePolicy.second->IsSatisfied(endorsements)) {
      return false;
    }
  }
  return true;
}

void Policy::MergePolicy(const Policy *other) {
  UW_ASSERT(other != nullptr);
  // if other is not same type, cannot directly merge it
  if (this->Type() != other->Type()) {
    if (mergedPolicies.find(other->Type()) == mergedPolicies.end()) {
      mergedPolicies[other->Type()] = other->Clone();
    }
    else {
      mergedPolicies[other->Type()]->MergePolicy(other);
    }
  }
}

std::vector<int> Policy::DifferenceToPolicy(const Policy *other) const {
  UW_ASSERT(other != nullptr);
  // if other is not same type, check mergedPolicies
  if (this->Type() != other->Type()) {
    if (mergedPolicies.find(other->Type()) == mergedPolicies.end()) {
      return other->GetMinSatisfyingSet();
    }
    else {
      return mergedPolicies.at(other->Type())->DifferenceToPolicy(other);
    }
  }
}

void Policy::SerializeToProtoMessage(proto::EndorsementPolicyMessage *msg) const {
  for (const auto &typePolicy : mergedPolicies) {
    typePolicy.second->SerializeToProtoMessage(msg);
  }
}

void Policy::Reset() {
  mergedPolicies.clear();
}

} // namespace sintrstore
