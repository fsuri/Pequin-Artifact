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

#include "store/sintrstore/endorsement_policy.h"

#include <algorithm>

namespace sintrstore {

EndorsementPolicy::EndorsementPolicy() : weight(0) {}
EndorsementPolicy::EndorsementPolicy(uint64_t weight) : 
  weight(weight) {}
EndorsementPolicy::EndorsementPolicy(const std::set<uint64_t> &access_control_list) : 
  access_control_list(access_control_list) {}
EndorsementPolicy::EndorsementPolicy(uint64_t weight, const std::set<uint64_t> &access_control_list) : 
  weight(weight), access_control_list(access_control_list) {}
EndorsementPolicy::EndorsementPolicy(const proto::EndorsementPolicyMessage &endorsePolicyMsg) {
  if (endorsePolicyMsg.has_weight()) {
    weight = endorsePolicyMsg.weight();
  }
  for (const auto &client_id : endorsePolicyMsg.access_control_list()) {
    access_control_list.insert(client_id);
  }
}
EndorsementPolicy::~EndorsementPolicy() {}

void EndorsementPolicy::operator= (const EndorsementPolicy &other) {
  weight = other.weight;
  access_control_list = other.access_control_list;
}

bool EndorsementPolicy::operator== (const EndorsementPolicy &other) const {
  return (weight == other.weight) && (access_control_list == other.access_control_list);
}
bool EndorsementPolicy::operator!= (const EndorsementPolicy &other) const {
  return !(*this == other);
}
bool EndorsementPolicy::operator> (const EndorsementPolicy &other) const {
  return (*this >= other) && (*this != other);
}
bool EndorsementPolicy::operator< (const EndorsementPolicy &other) const {
  return (*this <= other) && (*this != other);
}
bool EndorsementPolicy::operator>= (const EndorsementPolicy &other) const {
  // this weight at least other weight
  // and this access control list a superset of other access control list
  return (
    (weight >= other.weight) 
    && (
      std::includes(access_control_list.begin(), access_control_list.end(), 
                    other.access_control_list.begin(), other.access_control_list.end())
    )
  );
}
bool EndorsementPolicy::operator<= (const EndorsementPolicy &other) const {
  // this weight at most other weight
  // and this access control list a subset of other access control list
  return (
    (weight <= other.weight) 
    && (
      std::includes(other.access_control_list.begin(), other.access_control_list.end(), 
                    access_control_list.begin(), access_control_list.end())
    )
  );
}

uint64_t EndorsementPolicy::GetWeight() const {
  return weight;
}
std::set<uint64_t> EndorsementPolicy::GetAccessControlList() const {
  return access_control_list;
}

bool EndorsementPolicy::IsSatisfied(const std::set<uint64_t> &endorsements) const {
  return (
    (endorsements.size() >= weight) 
    && (std::includes(endorsements.begin(), endorsements.end(), 
                      access_control_list.begin(), access_control_list.end()))
  );
}

void EndorsementPolicy::MergePolicy(const EndorsementPolicy &other) {
  if (other.weight > weight) {
    weight = other.weight;
  }
  access_control_list.insert(other.access_control_list.begin(), other.access_control_list.end());
}

EndorsementPolicy EndorsementPolicy::DifferenceToPolicy(const EndorsementPolicy &other) const {
  if (*this >= other) {
    return EndorsementPolicy();
  }

  uint64_t additional_weight = 0;
  if (weight < other.weight) {
    additional_weight = other.weight - weight;
  }

  std::set<uint64_t> additional_access_control;
  std::set_difference(
    other.access_control_list.begin(), other.access_control_list.end(), 
    access_control_list.begin(), access_control_list.end(),
    std::inserter(additional_access_control, additional_access_control.begin())
  );

  return EndorsementPolicy(additional_weight, additional_access_control);
}

void EndorsementPolicy::SerializeToProtoMessage(proto::EndorsementPolicyMessage *msg) const {
  msg->set_weight(weight);
  for (const auto &client_id : access_control_list) {
    msg->add_access_control_list(client_id);
  }  
}

void EndorsementPolicy::Reset() {
  weight = 0;
  access_control_list.clear();
}

} // namespace sintrstore
