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

#ifndef PROTO_WRAPPER_ROW_H
#define PROTO_WRAPPER_ROW_H

#include <string>
#include <vector>
#include <memory>
#include "store/common/query_result_row.h"
#include "store/common/query_result_field.h"
#include "store/common/query_result_proto_wrapper_field.h"

namespace sql {

class QueryResultProtoWrapper;

class Row : public query_result::Row {
 protected:
  friend class sql::Field;
  friend class QueryResultProtoWrapper;
  const QueryResultProtoWrapper* m_result = nullptr;
  std::size_t m_row = 0;
  std::size_t m_offset = 0;
  std::size_t m_columns = 0;

  Row() = default;

  Row( const QueryResultProtoWrapper* in_result, const std::size_t in_row, const std::size_t in_offset, const std::size_t in_columns ) noexcept
      : m_result( in_result ),
        m_row( in_row ),
        m_offset( in_offset ),
        m_columns( in_columns )
  {}

 public:
  class const_iterator : query_result::Row::const_iterator, sql::Field {
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

      auto operator++( int ) noexcept -> std::unique_ptr<query_result::Row::const_iterator>
      {
        sql::Row::const_iterator *nrv = new sql::Row::const_iterator( *this );
        ++*this;
        return std::unique_ptr<query_result::Row::const_iterator>(nrv);
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

      auto operator--( int ) noexcept -> std::unique_ptr<query_result::Row::const_iterator>
      {
        sql::Row::const_iterator *nrv = new sql::Row::const_iterator( *this );
        --*this;
        return std::unique_ptr<query_result::Row::const_iterator>(nrv);
      }

      auto operator-=( const std::int32_t n ) noexcept -> query_result::Row::const_iterator&
      {
        m_column -= n;
        return *this;
      }

      auto operator*() const noexcept -> const query_result::Field&
      {
        return *this;
      }

      auto operator->() const noexcept -> const query_result::Field*
      {
        return this;
      }

      auto operator[]( const std::int32_t n ) const noexcept -> std::unique_ptr<query_result::Field>
      {
        sql::Row::const_iterator *nrv = new sql::Row::const_iterator( *this );
        nrv += n;
        return std::unique_ptr<query_result::Field>(nrv);
      }

      friend auto operator+( const const_iterator& lhs, const std::int32_t rhs ) noexcept -> std::unique_ptr<query_result::Row::const_iterator>
      {
        sql::Row::const_iterator *nrv = new sql::Row::const_iterator( lhs );
        nrv += rhs;
        return std::unique_ptr<query_result::Row::const_iterator>(nrv);
      }
  };

  auto operator[]( const std::size_t column ) const -> std::unique_ptr<query_result::Field>
  {
    sql::Field *p = new sql::Field(*this, m_offset + column);
    return std::unique_ptr<query_result::Field>(p);
  }
  
  auto columns() const noexcept -> std::size_t;

  auto name( const std::size_t column ) const -> std::string;

  // iteration
  auto begin() const -> std::unique_ptr<query_result::Row::const_iterator>;

  auto end() const -> std::unique_ptr<query_result::Row::const_iterator>;

  auto cbegin() const -> std::unique_ptr<query_result::Row::const_iterator>
  {
    return begin();
  }

  auto cend() const -> std::unique_ptr<query_result::Row::const_iterator>
  {
    return end();
  }

  auto get( const std::size_t column ) const -> const char*;

  auto is_null( const std::size_t column ) const -> bool;

  auto slice( const std::size_t offset, const std::size_t in_columns ) const -> std::unique_ptr<query_result::Row>;
};

}

#endif /* PROTO_WRAPPER_ROW_H */