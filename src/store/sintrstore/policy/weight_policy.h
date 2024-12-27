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

#ifndef _SINTR_ENDORSEMENT_WEIGHT_POLICY_H_
#define _SINTR_ENDORSEMENT_WEIGHT_POLICY_H_

#include "store/sintrstore/policy/policy.h"
#include "store/sintrstore/sintr-proto.pb.h"

#include <set>
#include <map>

namespace sintrstore {

// this class represents an endorsement policy that is weight based
class WeightPolicy : public Policy {
 public:
  WeightPolicy();
  WeightPolicy(uint64_t weight);
  WeightPolicy(const WeightPolicy &other);

  void operator= (const WeightPolicy &other);
  bool operator== (const WeightPolicy &other) const;
  bool operator!= (const WeightPolicy &other) const;
  bool operator< (const WeightPolicy &other) const;
  bool operator> (const WeightPolicy &other) const;
  bool operator<= (const WeightPolicy &other) const;
  bool operator>= (const WeightPolicy &other) const;

  std::string Type() const override;
  Policy *Clone() const override;
  uint64_t GetWeight() const;
  bool IsSatisfied(const std::set<uint64_t> &endorsements) const override;
  void MergePolicy(const Policy *other) override;
  std::vector<int> DifferenceToPolicy(const Policy *other) const override;
  std::vector<int> GetMinSatisfyingSet() const override;
  void SerializeToProtoMessage(proto::EndorsementPolicyMessage *msg) const override;
  void Reset() override;

 private:
  // type
  const std::string type = "weight";
  // weight says number of endorsements needed
  uint64_t weight;
};

} // namespace sintrstore

#endif /* _SINTR_ENDORSEMENT_WEIGHT_POLICY_H_ */
