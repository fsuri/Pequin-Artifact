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
#include "store/common/query_result.h"
#include "store/common/query_result_row.h"
#include "store/common/query-result-proto.pb.h"
#include "store/common/query_result_proto_wrapper.h"
#include "store/common/query_result_proto_wrapper_row.h"

typedef SQLResult SQLResultProto;
typedef Field FieldProto;

namespace sql {

auto QueryResultProtoWrapper::create_from_proto(const SQLResultProto* proto_result) -> void
{
  this->proto_result = proto_result;
  n_rows_affected = proto_result->rows_affected();
  column_names = std::vector<std::string>(proto_result->column_names().begin(), proto_result->column_names().end());
  result = std::vector<std::vector<std::string>>();
  for(int i = 0; i < proto_result->rows_size(); i++) {
    RowProto row = proto_result->rows(i);
    auto fields_vector = std::vector<FieldProto>(row.fields().begin(), row.fields().end());
    auto data_vector = std::vector<std::string>();
    for (auto field : fields_vector) {
      data_vector.push_back(field.data());
    }
    result.push_back(data_vector);
  }
}

auto QueryResultProtoWrapper::check_has_result_set() const -> void
{
  if(column_names.size() == 0) {
    throw std::runtime_error("No result set");
  }
}

auto QueryResultProtoWrapper::name( const std::size_t column ) const -> std::string
{
  return column_names[column];
}

bool QueryResultProtoWrapper::empty() const
{
  return result.empty();
}

auto QueryResultProtoWrapper::size() const -> std::size_t
{
  return result.size();
}

auto QueryResultProtoWrapper::columns() const -> std::size_t
{
  return column_names.size();
}

auto QueryResultProtoWrapper::begin() const -> std::unique_ptr<const_iterator>
{
  check_has_result_set();
  return std::unique_ptr<const_iterator>(new const_iterator( Row(this, 0, 0, column_names.size())));
}

auto QueryResultProtoWrapper::end() const -> std::unique_ptr<const_iterator>
{
  return std::unique_ptr<const_iterator>(new const_iterator( Row(this, size(), 0, column_names.size() ) ));
}

auto QueryResultProtoWrapper::is_null( const std::size_t row, const std::size_t column ) const -> bool {
  return result.at(row).at(column) == "";
}

auto QueryResultProtoWrapper::get( const std::size_t row, const std::size_t column, std::size_t* size ) const -> const char* 
{
  if(!is_null(row, column)) {
    const std::string& r_bytes = result.at(row).at(column);
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
  return n_rows_affected > 0;
}

auto QueryResultProtoWrapper::rows_affected() const -> std::size_t {
  return n_rows_affected;
}

auto QueryResultProtoWrapper::serialize(std::string *output) const -> bool {
  return proto_result->SerializeToString(output);
}

}
