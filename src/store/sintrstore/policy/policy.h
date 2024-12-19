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

#ifndef _SINTR_POLICY_H_
#define _SINTR_POLICY_H_

#include "store/sintrstore/sintr-proto.pb.h"

#include <set>
#include <vector>

namespace sintrstore {

// this abstract class represents a generic endorsement policy
// underlying assumption - policies of the same type can be merged
// server should have only "pure" policies, i.e. empty mergedPolicies field
// mergedPolicies is for if a txn results in combination of multiple distinct policy types
class Policy {
 public:
  Policy() {};
  virtual ~Policy() {
    for (auto &typePolicy : mergedPolicies) {
      delete typePolicy.second;
    }
  };

  // policy type
  virtual std::string Type() const = 0;
  // clone a new copy on the heap
  virtual Policy *Clone() const = 0;
  // does endorsements satisfy this Policy object?
  virtual bool IsSatisfied(const std::set<uint64_t> &endorsements) const;
  // merge this Policy with other
  // assume that other is owned by caller, not this policy object
  // assume that other does not have any mergedPolicies field
  // this is reasonable since other should always be coming from the server
  virtual void MergePolicy(const Policy *other);
  // how much more does this Policy need to become a superset of other?
  // note that if this policy is already a superset, return is empty policy
  // assume that other is owned by caller, not this policy object
  virtual std::vector<int> DifferenceToPolicy(const Policy *other) const;
  // return the minimum size set of client ids that would satisfy this policy
  // represent generic client ids with -1
  virtual std::vector<int> GetMinSatisfyingSet() const = 0;
  // serialize to proto version
  virtual void SerializeToProtoMessage(proto::EndorsementPolicyMessage *msg) const;
  virtual void Reset();

 protected:
  // mergedPolicies keeps track of all Policy objects that are not the same type merged into this one
  // map from policy name to policy object
  std::map<std::string, Policy *> mergedPolicies;
};

} // namespace sintrstore

#endif /* _SINTR_POLICY_H_ */
