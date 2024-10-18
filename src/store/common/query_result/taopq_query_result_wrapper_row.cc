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
#include "store/common/query_result/taopq_query_result_wrapper_row.h"
#include "store/common/query_result/taopq_query_result_wrapper_field.h"

namespace taopq_wrapper {

auto Row::num_columns() const noexcept -> std::size_t 
{
    return row.columns();
}

auto Row::name( const std::size_t column ) const -> std::string
{
  return row.name(column);
}

auto Row::get_bytes( const std::size_t column, std::size_t* size ) const -> const char*
{
  return row.get(column);
}

auto Row::is_null( const std::size_t column ) const -> bool 
{
  return row.is_null(column);
}

auto Row::slice( const std::size_t offset, const std::size_t in_columns ) const -> std::unique_ptr<query_result::Row> 
{
  return std::unique_ptr<query_result::Row>(new taopq_wrapper::Row(row.slice(offset, in_columns)));
}

auto Row::operator[]( const std::size_t column ) const -> std::unique_ptr<query_result::Field> 
{
  return std::unique_ptr<query_result::Field>(new taopq_wrapper::Field(row[column]));
}

void Row::get(const std::size_t column, bool *field) const {
   if(row.is_null(column)){
    *field = false;
    return;
  }
  *field = row.get<bool>(column);
}

void Row::get(const std::size_t column, int32_t *field) const {
   if(row.is_null(column)){
    *field = 0;
    return;
  }
  *field = row.get<int32_t>(column);
}

void Row::get(const std::size_t column, int64_t *field) const {
   if(row.is_null(column)){
    *field = 0;
    return;
  }
  *field = row.get<int64_t>(column);
}

void Row::get(const std::size_t column, uint32_t *field) const {
   if(row.is_null(column)){
    *field = 0;
    return;
  }
  *field = row.get<uint32_t>(column);
}

void Row::get(const std::size_t column, uint64_t *field) const {
  if(row.is_null(column)){
    *field = 0;
    return;
  }
  *field = row.get<uint64_t>(column);
}

void Row::get(const std::size_t column, double *field) const {
  if(row.is_null(column)){
    *field = 0.0;
    return;
  }
  *field = row.get<double>(column);
}

void Row::get(const std::size_t column, std::string *field) const {
  if(row.is_null(column)){
    *field = "";
    return;
  }
  *field = row.get<std::string>(column);
}

}
