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
#include "store/common/sql_result.h"

namespace sql {

void SQLResult::check_has_result_set() const 
{
  if(m_columns == 0) {
    throw std::runtime_error("No result set");
  }
}

auto SQLResult::name( const std::size_t column ) const -> std::string
{
  throw std::runtime_error( "Not implemented" );
}

bool SQLResult::empty() const
{
  throw std::runtime_error( "Not implemented" );
}

auto SQLResult::size() const -> std::size_t
{
  throw std::runtime_error( "Not implemented" );
}

auto SQLResult::begin() const -> const_iterator
{
  check_has_result_set();
  return const_iterator( sql::Row( *this, 0, 0, m_columns ) );
}

auto SQLResult::end() const -> const_iterator
{
  return const_iterator( sql::Row( *this, size(), 0, m_columns ) );
}
}
