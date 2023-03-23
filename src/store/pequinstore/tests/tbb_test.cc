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

#include <iostream>
#include "tbb/concurrent_unordered_map.h"
#include "tbb/concurrent_hash_map.h"
#include "tbb/concurrent_unordered_set.h"

typedef tbb::concurrent_hash_map<std::string, bool> TestMap;

int main(int argc, char *argv[]) {
  
  std::cerr<< "Running TBB tests" << std::endl;;
  TestMap testMap;

  TestMap::accessor t1;
  TestMap::accessor t2;
  TestMap::const_accessor t3;

  std::string dummyString("test");

  if(true){
    testMap.insert(t1, dummyString);
    t1->second = true;
  }
  // t1.release();

  // bool has_dummy = testMap.find(t2, dummyString);
  // std::cerr << "has dummy: " << has_dummy << std::endl;
  // std::cerr << "is t2 empty: " << t2.empty() << std::endl;
  //testMap.erase(t1);


  //Test whether one can lock t2 or not.
  testMap.find(t3, dummyString);
  std::cerr << "Managed to lock t3 " << std::endl;


  t1.release();
  t2.release();
  t3.release();
  
    
  return 0;
}
