/***********************************************************************
 *
 * Copyright 2021 Florian Suri-Payer <fsp@cs.cornell.edu>
 *                Matthew Burke <matthelb@cs.cornell.edu>
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
#ifndef KEY_SELECTOR_H
#define KEY_SELECTOR_H

#include <random>
#include <string>
#include <vector>

class KeySelector {
 public:
  KeySelector(const std::vector<std::string> &keys);
  KeySelector(const std::vector<std::string> &keys, const int numKeys);
  virtual ~KeySelector();

  virtual int GetKey(std::mt19937 &rand) = 0;

  inline const std::string &GetKey(int idx) const { return keys[idx]; }
  inline size_t GetNumKeys() const { return numKeys; } //return keys.size(); }

 private:
  const std::vector<std::string> &keys;
  const int numKeys;

};

struct QuerySelector{
  QuerySelector(uint64_t numKeys, KeySelector *tableSelector, KeySelector *baseSelector, KeySelector *rangeSelector) : 
     numKeys(numKeys), tableSelector(tableSelector), baseSelector(baseSelector), rangeSelector(rangeSelector) {}
  ~QuerySelector(){ //TODO: Need to delete them in benchmark.cc
  
  }
  //uint64_t numTables;
  //uint64_t maxRange;
  uint64_t numKeys; //keys per Table

  KeySelector *tableSelector;  //pick which table to read from -- pick random TableIdx (i.e. pick between 0 and numTables-1)

  //pick a random amount of keys to read (i.e. between 1 and maxRange)
  //read the range: base < x < base + offset
  KeySelector *baseSelector;  //pick which row to start scan from -- pick random starting point Idx (i.e. pick between 0 and numKeys-1)
  
  KeySelector *rangeSelector; //pick size of scan  -- pick random offset (i.e. pick between 0 and maxRange)

};

#endif /* KEY_SELECTOR_H */
