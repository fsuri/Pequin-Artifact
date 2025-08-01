/***********************************************************************
 *
 * Copyright 2021 Florian Suri-Payer <fsp@cs.cornell.edu>
 *                Liam Arzola <lma77@cornell.edu>
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

#include <memory>
#include "store/common/query_result/query_result_proto_builder.h"
#include "lib/assert.h"

namespace sql {

  //TODO: Ideally construct it in sorted manner:
  //e.g. have minheap of Rows
  static bool orderRowsbyCol(const Row &l, const Row &r){

    UW_ASSERT(l.fields_size() == r.fields_size());
    for(int i = 0; i < l.fields_size(); ++ i){
      if(l.fields()[i].data() < r.fields()[i].data()) return true;
      else if(l.fields()[i].data() >  r.fields()[i].data()) return false;
      //else continue;
    }
    return false;
  }

auto QueryResultProtoBuilder::add_columns(const std::vector<std::string>& columns) -> void {
  for (auto name : columns) {
    result->add_column_names(name);
  }
}

auto QueryResultProtoBuilder::add_column(const std::string& name) -> void {
  result->add_column_names(name);
}

auto QueryResultProtoBuilder::get_result(bool sort) -> std::unique_ptr<SQLResult> {
  //Peloton may output different result orders at different servers. In order to match the result for equality easily we need them to be sorted.
  //NOTE: If the Query statement has an ORDER BY clause, then don't sort again (that could violate the predicate order). Results will be consistent already.
  if(sort) std::sort(result->mutable_rows()->begin(), result->mutable_rows()->end(), orderRowsbyCol);
  auto old_result = std::move(result);
  result = std::make_unique<SQLResult>();
  return old_result;
}

auto QueryResultProtoBuilder::add_empty_row() -> void {
  result->add_rows();
}

auto QueryResultProtoBuilder::set_rows_affected(const uint32_t n_rows_affected) -> void {
  result->set_rows_affected(n_rows_affected);
}

} //namespace sql

