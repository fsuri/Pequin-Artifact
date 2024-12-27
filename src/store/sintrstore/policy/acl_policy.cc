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

#include "store/sintrstore/policy/acl_policy.h"
#include "lib/assert.h"

#include <algorithm>

namespace sintrstore {

ACLPolicy::ACLPolicy(const std::set<uint64_t> &access_control_list) : 
  access_control_list(access_control_list) {}
ACLPolicy::ACLPolicy(const proto::EndorsementPolicyMessage &endorsePolicyMsg) {
  for (const auto &client_id : endorsePolicyMsg.access_control_list()) {
    access_control_list.insert(client_id);
  }
}
ACLPolicy::ACLPolicy(const ACLPolicy &other) : access_control_list(other.access_control_list) {}

void ACLPolicy::operator= (const ACLPolicy &other) {
  access_control_list = other.access_control_list;
}

bool ACLPolicy::operator== (const ACLPolicy &other) const {
  return access_control_list == other.access_control_list;
}
bool ACLPolicy::operator!= (const ACLPolicy &other) const {
  return !(*this == other);
}
bool ACLPolicy::operator< (const ACLPolicy &other) const {
  return (*this <= other) && (*this != other);
}
bool ACLPolicy::operator> (const ACLPolicy &other) const {
  return other < *this;
}
bool ACLPolicy::operator<= (const ACLPolicy &other) const {
  // this access control list a subset of other access control list
  return std::includes(
    other.access_control_list.begin(), other.access_control_list.end(), 
    access_control_list.begin(), access_control_list.end()
  );
}
bool ACLPolicy::operator>= (const ACLPolicy &other) const {
  return other <= *this;
}

std::string ACLPolicy::Type() const {
  return type;
}

Policy *ACLPolicy::Clone() const {
  return new ACLPolicy(*this);
}

std::set<uint64_t> ACLPolicy::GetAccessControlList() const {
  return access_control_list;
}

bool ACLPolicy::IsSatisfied(const std::set<uint64_t> &endorsements) const {
  return std::includes(
    endorsements.begin(), endorsements.end(), 
    access_control_list.begin(), access_control_list.end()
  );
}

void ACLPolicy::MergePolicy(const Policy *other) {
  UW_ASSERT(other != nullptr);
  UW_ASSERT(type == other->Type());

  const ACLPolicy *otherACLPolicy = static_cast<const ACLPolicy *>(other);
  std::set<uint64_t> other_access_control_list = otherACLPolicy->GetAccessControlList();
  access_control_list.insert(other_access_control_list.begin(), other_access_control_list.end());
}

std::vector<int> ACLPolicy::DifferenceToPolicy(const Policy *other) const {
  UW_ASSERT(other != nullptr);
  UW_ASSERT(type == other->Type());

  const ACLPolicy *otherACLPolicy = static_cast<const ACLPolicy *>(other);
  std::set<uint64_t> other_access_control_list = otherACLPolicy->GetAccessControlList();
  
  std::vector<int> additional_access_control;
  std::set_difference(
    other_access_control_list.begin(), other_access_control_list.end(), 
    access_control_list.begin(), access_control_list.end(),
    std::inserter(additional_access_control, additional_access_control.begin())
  );

  return additional_access_control;
}

std::vector<int> ACLPolicy::GetMinSatisfyingSet() const {
  std::vector<int> min_satisfying_set;
  for (const auto &client_id : access_control_list) {
    min_satisfying_set.push_back(client_id);
  }
  return min_satisfying_set;
}

void ACLPolicy::SerializeToProtoMessage(proto::EndorsementPolicyMessage *msg) const {
  for (const auto &client_id : access_control_list) {
    msg->add_access_control_list(client_id);
  }  
}

void ACLPolicy::Reset() {
  access_control_list.clear();
}

} // namespace sintrstore
