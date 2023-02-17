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

#ifndef QUERY_RESULT_PROTO_BUILDER_H
#define QUERY_RESULT_PROTO_BUILDER_H

#include <memory>
#include "store/common/query-result-proto.pb.h"

namespace sql {

class QueryResultProtoWrapperBuilder {
 private:
  std::unique_ptr<SQLResult> result;

 public:
  QueryResultProtoWrapperBuilder() {
    result = std::make_unique<SQLResult>();
  }

  auto serialize(int i) -> std::string;
  auto serialize(const std::string& s) -> std::string;

  auto set_column_names(const std::vector<std::string>& columns) -> void;
  auto add_column(const std::string& name) -> void;

  template<class Iterable>
  void add_row(Iterable it, Iterable end)
  {
    Row *row = result->add_rows();
    while (it != end) {
      Field *field = row->add_fields();
      field.set_data(serialize(*it));
      it++;
    }
  }
}

}

#endif /* QUERY_RESULT_PROTO_BUILDER_H */