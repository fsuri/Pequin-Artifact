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

#include "store/sintrstore/policy/weight_policy.h"
#include "store/sintrstore/policy/policy-proto.pb.h"
#include "lib/assert.h"

namespace sintrstore {

WeightPolicy::WeightPolicy() : weight(0) {}
WeightPolicy::WeightPolicy(uint64_t weight) : weight(weight) {}
WeightPolicy::WeightPolicy(const WeightPolicy &other) : weight(other.weight) {}

void WeightPolicy::operator= (const WeightPolicy &other) {
  weight = other.weight;
}

bool WeightPolicy::operator== (const WeightPolicy &other) const {
  return weight == other.weight;
}
bool WeightPolicy::operator!= (const WeightPolicy &other) const {
  return !(*this == other);
}
bool WeightPolicy::operator< (const WeightPolicy &other) const {
  return weight < other.weight;
}
bool WeightPolicy::operator> (const WeightPolicy &other) const {
  return !(other < *this);
}
bool WeightPolicy::operator<= (const WeightPolicy &other) const {
  return (*this < other) || (*this == other);
}
bool WeightPolicy::operator>= (const WeightPolicy &other) const {
  return (other <= *this);
}

std::string WeightPolicy::Type() const {
  return type;
}

Policy *WeightPolicy::Clone() const {
  return new WeightPolicy(*this);
}

uint64_t WeightPolicy::GetWeight() const {
  return weight;
}

bool WeightPolicy::IsSatisfied(const std::set<uint64_t> &endorsements) const {
  return endorsements.size() >= weight;
}

void WeightPolicy::MergePolicy(const Policy *other) {
  UW_ASSERT(other != nullptr);
  UW_ASSERT(type == other->Type());

  const WeightPolicy *otherWeightPolicy = static_cast<const WeightPolicy *>(other);
  if (otherWeightPolicy->GetWeight() > weight) {
    weight = otherWeightPolicy->GetWeight();
  }
}

std::vector<int> WeightPolicy::GetMinSatisfyingSet() const {
  std::vector<int> minSatisfyingSet;
  for (uint64_t i = 0; i < weight; i++) {
    minSatisfyingSet.push_back(-1);
  }
  return minSatisfyingSet;
}

bool WeightPolicy::IsOtherWeaker(const Policy *other) const {
  UW_ASSERT(other != nullptr);
  UW_ASSERT(type == other->Type());

  const WeightPolicy *otherWeightPolicy = static_cast<const WeightPolicy *>(other);
  return *otherWeightPolicy < *this;
}

void WeightPolicy::SerializeToProtoMessage(proto::PolicyObject *msg) const {
  proto::WeightPolicyMessage weightPolicyMsg;
  weightPolicyMsg.set_weight(weight);
  msg->set_policy_type(proto::PolicyObject::WEIGHT_POLICY);
  weightPolicyMsg.SerializeToString(msg->mutable_policy_data());
}

void WeightPolicy::Reset() {
  weight = 0;
}

} // namespace sintrstore
