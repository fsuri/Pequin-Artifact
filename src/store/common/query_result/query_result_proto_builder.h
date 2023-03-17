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
#include "store/common/query_result/query-result-proto.pb.h"
#include "lib/cereal/types/map.hpp"
#include "lib/cereal/types/vector.hpp"
#include "lib/cereal/types/string.hpp"
#include "lib/cereal/types/complex.hpp"
#include "lib/cereal/archives/binary.hpp"

typedef SQLResult SQLResultProto;
typedef Row RowProto;
typedef Field FieldProto;

namespace sql {

class QueryResultProtoBuilder {
 private:
  std::unique_ptr<SQLResultProto> result;

 public:
  QueryResultProtoBuilder() {
    result = std::make_unique<SQLResultProto>();
  }

  // auto serialize(std::int32_t i) -> std::string;
  // auto serialize(const std::string& s) -> std::string;
  // auto serialize(std::uint64_t i) -> std::string;
  // auto serialize(bool i) -> std::string;

  auto add_columns(const std::vector<std::string>& columns) -> void;
  auto add_column(const std::string& name) -> void;

  auto get_result() -> std::unique_ptr<SQLResultProto>;

  template<class Iterable>
  void add_row(Iterable it, Iterable end)
  {
    RowProto *row = result->add_rows();
    while (it != end) {
      FieldProto *field = row->add_fields();
      std::string serialized_field = serialize(*it);
      field->set_data(serialized_field);
      it++;
    }
  }

  template<typename T>
  auto serialize(T t) -> std::string {
    // std::string s(static_cast<char*>(static_cast<void*>(&t)));
    // return s;

    std::stringstream ss(std::ios::out | std::ios::in | std::ios::binary);
    {
      cereal::BinaryOutputArchive oarchive(ss); // Create an output archive
      oarchive(t); // Write the data to the archive

    } // archive goes out of scope, ensuring all contents are flushed
    return ss.str();
  }
};

}

#endif /* QUERY_RESULT_PROTO_BUILDER_H */