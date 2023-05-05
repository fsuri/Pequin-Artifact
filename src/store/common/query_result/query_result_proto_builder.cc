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

namespace sql {

auto QueryResultProtoBuilder::add_columns(const std::vector<std::string>& columns) -> void {
  for (auto name : columns) {
    result->add_column_names(name);
  }
}

auto QueryResultProtoBuilder::add_column(const std::string& name) -> void {
  result->add_column_names(name);
}

auto QueryResultProtoBuilder::get_result() -> std::unique_ptr<SQLResult> {
  auto old_result = std::move(result);
  result = std::make_unique<SQLResult>();
  return old_result;
}

auto QueryResultProtoBuilder::add_empty_row() -> void {
  result->add_rows();
}

} //namespace sql

