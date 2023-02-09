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

#include "store/common/query_result.h"
#include "store/common/query_result_row.h"

namespace taopq_wrapper {

auto Row::columns() const noexcept -> std::size_t {
    return row.columns();
}

auto Row::name( const std::size_t column ) const -> std::string
{
  return row.name();
}

bool Row::empty() const
{
  return row.empty();
}

auto Row::begin() const -> query_result::QueryResult::const_iterator*
{
  return new const_iterator( row.begin() );
}

auto Row::end() const -> query_result::QueryResult::const_iterator*
{
  return new const_iterator( row.end() );
}

auto Row::get( const std::size_t column ) const -> const char* {
  return row.get(column);
}

auto Row::is_null( const std::size_t column ) const -> bool {
  return row.is_null(column);
}

auto Row::slice( const std::size_t offset, const std::size_t in_columns ) const -> query_result::Row {
  return Row(row.slice(offset, in_columns));
}

auto Row::operator[]( const std::size_t column ) const -> query_result::Field {
  return taopq_wrapper::Field(row[column]);
}

}
