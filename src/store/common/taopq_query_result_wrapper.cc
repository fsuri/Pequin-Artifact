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

auto TaoPQQueryResultWrapper::name( const std::size_t column ) const -> std::string
{
  return result.name();
}

bool TaoPQQueryResultWrapper::empty() const
{
  return result.empty();
}

auto TaoPQQueryResultWrapper::size() const -> std::size_t
{
  return result.size();
}

auto TaoPQQueryResultWrapper::begin() const -> query_result::QueryResult::const_iterator*
{
  return new const_iterator( result.begin() );
}

auto TaoPQQueryResultWrapper::end() const -> query_result::QueryResult::const_iterator*
{
  return new const_iterator( result.end() );
}

auto TaoPQQueryResultWrapper::operator[]( const std::size_t row ) const -> query_result::Row {
  return taopq_wrapper::Row(result[row]);
}
auto TaoPQQueryResultWrapper::at( const std::size_t row ) const -> query_result::Row {
  return taopq_wrapper::Row(result[row]);
}

auto TaoPQQueryResultWrapper::has_rows_affected() const noexcept -> bool {
  return result.has_rows_affected();
}

auto TaoPQQueryResultWrapper::rows_affected() const -> std::size_t {
  return result.rows_affected();
}
}
