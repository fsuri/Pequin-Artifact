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

#include <string>
#include <vector>
#include "store/common/query_result/query_result.h"
#include "store/common/query_result/query_result_row.h"
#include "store/common/query_result/query-result-proto.pb.h"
#include "store/common/query_result/query_result_proto_wrapper.h"
#include "store/common/query_result/query_result_proto_wrapper_row.h"

typedef SQLResult SQLResultProto;
typedef Field FieldProto;

namespace sql {

auto QueryResultProtoWrapper::check_has_result_set() const -> void
{
  if(proto_result->column_names_size() == 0) {
    throw std::runtime_error("No result set");
  }
}

auto QueryResultProtoWrapper::name( const std::size_t column ) const -> std::string
{
  return proto_result->column_names(column);
}

auto QueryResultProtoWrapper::column_index_by_name(const std::string &name) const -> std::size_t {
  for (int i = 0; i < proto_result->column_names_size(); i++) {
    if (proto_result->column_names(i) == name) {
      return i;
    }
  }
  throw std::runtime_error("Column name not found");
}

bool QueryResultProtoWrapper::empty() const
{
  return proto_result->rows_size() == 0;
}

auto QueryResultProtoWrapper::size() const -> std::size_t
{
  return proto_result->rows_size();
}

auto QueryResultProtoWrapper::columns() const -> std::size_t
{
  return proto_result->column_names_size();
}

auto QueryResultProtoWrapper::begin() const -> std::unique_ptr<const_iterator>
{
  check_has_result_set();
  return std::unique_ptr<const_iterator>(new const_iterator( Row(this, 0, 0, columns())));
}

auto QueryResultProtoWrapper::end() const -> std::unique_ptr<const_iterator>
{
  return std::unique_ptr<const_iterator>(new const_iterator( Row(this, size(), 0, columns() ) ));
}

auto QueryResultProtoWrapper::is_null( const std::size_t row, const std::size_t column ) const -> bool {
  return !proto_result->rows(row).fields(column).has_data();
}

auto QueryResultProtoWrapper::get( const std::size_t row, const std::size_t column, std::size_t* size ) const -> const char* 
{
  if(!is_null(row, column)) {
    const std::string& r_bytes = proto_result->rows(row).fields(column).data(); // result.at(row).at(column);
    *size = r_bytes.size();
    const char* r = r_bytes.data();
    return r;
  } else {
    return nullptr;
  }
}

auto QueryResultProtoWrapper::operator[]( const std::size_t row ) const -> std::unique_ptr<query_result::Row> {
  return std::unique_ptr<query_result::Row>(new sql::Row(this, row, 0, this->columns()));
}

auto QueryResultProtoWrapper::at( const std::size_t row ) const -> std::unique_ptr<query_result::Row> {
  check_has_result_set();
  return (*this)[row];
}

auto QueryResultProtoWrapper::has_rows_affected() const noexcept -> bool {
  return proto_result->rows_affected() > 0;
}

auto QueryResultProtoWrapper::rows_affected() const -> std::size_t {
  return proto_result->rows_affected();
}

auto QueryResultProtoWrapper::serialize(std::string *output) const -> bool {
  return proto_result->SerializeToString(output);
}

}
