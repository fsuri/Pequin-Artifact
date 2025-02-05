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
class Policy {
 public:
  Policy() {};
  virtual ~Policy() {};

  // policy type
  virtual std::string Type() const = 0;
  // clone a new copy on the heap
  virtual Policy *Clone() const = 0;
  // does endorsements satisfy this Policy object?
  virtual bool IsSatisfied(const std::set<uint64_t> &endorsements) const = 0;
  // merge this Policy with other
  // assume that other is owned by caller, not this policy object
  // other must be of the same type as this policy
  virtual void MergePolicy(const Policy *other) = 0;
  // return the minimum size set of client ids that would satisfy this policy
  // represent generic client ids with -1
  virtual std::vector<int> GetMinSatisfyingSet() const = 0;
  // is other strictly weaker than this policy?
  virtual bool IsOtherWeaker(const Policy *other) const = 0;
  // serialize to proto version
  virtual void SerializeToProtoMessage(proto::PolicyObject *msg) const = 0;
  virtual void Reset() = 0;
};

} // namespace sintrstore

#endif /* _SINTR_POLICY_H_ */
