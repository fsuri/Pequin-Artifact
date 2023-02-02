/***********************************************************************
 *
 * Copyright 2023 Florian Suri-Payer <fsp@cs.cornell.edu>
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
#include "store/common/query_result_row.h"
#include "store/common/query_result_field.h"
#include "store/common/sql_result_field.h"

namespace sql {

class SQLResult;

class Row : query_result::Row {
 private:
  friend class SQLResult;
  const SQLResult* m_result = nullptr;
  std::size_t m_row = 0;
  std::size_t m_offset = 0;
  std::size_t m_columns = 0;

  Row() = default;

  Row( const SQLResult& in_result, const std::size_t in_row, const std::size_t in_offset, const std::size_t in_columns ) noexcept
      : m_result( &in_result ),
        m_row( in_row ),
        m_offset( in_offset ),
        m_columns( in_columns )
  {}

 public:
  class const_iterator : query_result::Row::const_iterator {
    private:
      friend class Row;

      explicit const_iterator( const sql::Field& f ) noexcept
        : sql::Field( f )
      {}
    public:
      const_iterator() = default;

      auto operator++() noexcept -> query_result::Row::const_iterator&
      {
        ++m_column;
        return *this;
      }

      auto operator++( int ) noexcept -> query_result::Row::const_iterator
      {
        const_iterator nrv( *this );
        ++*this;
        return nrv;
      }

      auto operator+=( const std::int32_t n ) noexcept -> query_result::Row::const_iterator&
      {
        m_column += n;
        return *this;
      }

      auto operator--() noexcept -> query_result::Row::const_iterator&
      {
        --m_column;
        return *this;
      }

      auto operator--( int ) noexcept -> query_result::Row::const_iterator
      {
        const_iterator nrv( *this );
        --*this;
        return nrv;
      }

      auto operator-=( const std::int32_t n ) noexcept -> query_result::Row::const_iterator&
      {
        m_column -= n;
        return *this;
      }

      auto operator*() const noexcept -> const Field&
      {
        return *this;
      }

      auto operator->() const noexcept -> const Field*
      {
        return this;
      }

      auto operator[]( const std::int32_t n ) const noexcept -> Field
      {
        return *( *this + n );
      }
  };

  auto operator[]( const std::size_t column ) const -> query_result::Field
  {
    return { *this, m_offset + column };
  }
  
  auto columns() const noexcept -> std::size_t
  {
    return m_columns;
  }

  auto name( const std::size_t column ) const -> std::string;

  // iteration
  auto begin() const -> query_result::Row::const_iterator;

  auto end() const -> query_result::Row::const_iterator;

  auto cbegin() const -> query_result::Row::const_iterator
  {
    return begin();
  }

  auto cend() const -> query_result::Row::const_iterator
  {
    return end();
  }

  auto get( const std::size_t column ) const -> const char*;

  auto is_null( const std::size_t column ) const -> bool;
};

}