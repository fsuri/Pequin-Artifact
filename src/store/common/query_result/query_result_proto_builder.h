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

  auto add_columns(const std::vector<std::string>& columns) -> void;
  auto add_column(const std::string& name) -> void;
  auto add_empty_row() -> void;
  auto set_rows_affected(const uint32_t n_rows_affected) -> void;
  
  auto get_result() -> std::unique_ptr<SQLResultProto>;

  template<class Iterable>
  void add_row(Iterable it, Iterable end)
  //Note: Only use this if entire row is of the same data type.
  {
    RowProto *row = result->add_rows();
    while (it != end) {
      FieldProto *field = row->add_fields();
      std::string serialized_field = serialize(*it);
      field->set_data(serialized_field);
      it++;
    }
  }

  // Overwrites existing field
  template<typename T>
  auto insert_field_to_row(std::size_t row, std::size_t column, T &t) -> void {
    FieldProto *field = result->mutable_rows(row)->mutable_fields(column);
    field->set_data(serialize(t));
  }

  // Overwrites existing field in last row
  template<typename T>
  auto insert_field_to_last_row(std::size_t column, T &t) -> void {
    insert_field_to_row(result->rows_size() - 1, column, t);
  }

  template<typename T>
  auto serialize(T t) -> std::string {
    std::stringstream ss(std::ios::out | std::ios::in | std::ios::binary);
    {
      cereal::BinaryOutputArchive oarchive(ss); // Create an output archive
      oarchive(t); // Write the data to the archive

    } // archive goes out of scope, ensuring all contents are flushed
    return ss.str();
  }

  //Add new empty row
  inline RowProto * new_row()
  {
    return result->add_rows();
  }


  //Append field to given row
  template<typename T>
  void AddToRow(RowProto *row, T &t){
  //Appends field to a row
    FieldProto *field = row->add_fields();
    field->set_data(serialize(t));
  }

    //New interface:

   //Updates specific column in a row
   //Note: Requires fields to be allocated.
  template<typename T>
  auto update_field_in_row(std::size_t row, std::size_t column, T &t) -> void {
    FieldProto *field = result->mutable_rows(row)->mutable_fields(column);
    field->set_data(serialize(t));
  }

  // Overwrites existing field in last row
  template<typename T>
  auto update_field_in_last_row(std::size_t column, T &t) -> void {
    insert_field_to_row(result->rows_size() - 1, column, t);
  }

  // Adds new row with given field value in first column
  template<typename T>
  auto add_field_to_new_row(T &t) -> void {
    FieldProto *field = new_row()->add_fields();
    field->set_data(serialize(t));
  }

  // Adds new field to last row
  template<typename T>
  auto add_field_to_last_row(T &t) -> void {
    FieldProto *field = result->mutable_rows(result->rows_size() - 1)->add_fields();
    field->set_data(serialize(t));
  }

  // Adds new field to last row by column name
  template<typename T>
  auto add_field_to_last_row_by_name(const std::string &column_name, T &t) -> void {
    uint32_t column = 0;
    for(int i = 0; i < result->column_names_size(); i++) {
      if (result->column_names(i) == column_name) {
        break;
      }
      column++;
    }
    RowProto *row = result->mutable_rows(result->rows_size() - 1);
    for(int i = row->fields_size() - 1; i < column; i++) {
      row->add_fields();
    }
    insert_field_to_last_row(column, t);
  }

};

}

#endif /* QUERY_RESULT_PROTO_BUILDER_H */