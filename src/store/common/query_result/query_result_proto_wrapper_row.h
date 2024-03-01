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
#include "store/common/query_result/query_result_row.h"
#include "store/common/query_result/query_result_field.h"
#include "store/common/query_result/query_result_proto_wrapper_field.h"
#include "lib/cereal/archives/binary.hpp"
#include "lib/cereal/types/string.hpp"

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

 private:
  template<typename T> 
  void deserialize(const std::size_t column, T& field) const {
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    std::size_t n_bytes = 0;
    const char* r_chars = this->get_bytes(column, &n_bytes);
    std::string r = std::string(r_chars, n_bytes);
    ss << r;
    cereal::BinaryInputArchive iarchive(ss); // Create an input archive
    iarchive(field); // Read the data from the archive
  }

 public:
  class const_iterator : sql::Field {
    private:
      friend class Row;

      explicit const_iterator( const sql::Field& f ) noexcept
        : sql::Field( f )
      {}
    public:
      const_iterator() = default;

      auto operator++() noexcept -> const_iterator&
      {
        ++m_column;
        return *this;
      }

      auto operator++( int ) noexcept -> const_iterator
      {
        const_iterator nrv( *this );
        ++*this;
        return nrv;
      }

      auto operator+=( const std::int32_t n ) noexcept -> const_iterator&
      {
        m_column += n;
        return *this;
      }

      auto operator--() noexcept -> const_iterator&
      {
        --m_column;
        return *this;
      }

      auto operator--( int ) noexcept -> const_iterator
      {
        const_iterator nrv( *this );
        --*this;
        return nrv;
      }

      auto operator-=( const std::int32_t n ) noexcept -> const_iterator&
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

      friend auto operator+( const const_iterator& lhs, const std::int32_t rhs ) noexcept -> const_iterator
      {
        const_iterator nrv( lhs );
        nrv += rhs;
        return nrv;
      }
  };

  auto operator[]( const std::size_t column ) const -> std::unique_ptr<query_result::Field>
  {
    sql::Field *p = new sql::Field(*this, m_offset + column);
    return std::unique_ptr<query_result::Field>(p);
  }

  auto operator[]( const std::string &column_name ) const -> std::unique_ptr<query_result::Field>
  {
    if(m_result) {
      std::size_t column = column_index_by_name(column_name);
      sql::Field *p = new sql::Field(*this, m_offset + column);
      return std::unique_ptr<query_result::Field>(p);
    } else {
      std::cerr << "ProtoRowWrapper: Cannot get column index from null result" << std::endl;
      //throw std::runtime_error("Cannot get column index from null result");
    }
  }
  
  auto num_columns() const noexcept -> std::size_t;

  auto name( const std::size_t column ) const -> std::string;
  auto column_index_by_name(const std::string &name) const -> std::size_t;

  // iteration
  auto begin() const -> std::unique_ptr<const_iterator>;

  auto end() const -> std::unique_ptr<const_iterator>;

  auto cbegin() const -> std::unique_ptr<const_iterator>
  {
    return begin();
  }

  auto cend() const -> std::unique_ptr<const_iterator>
  {
    return end();
  }

  auto get_bytes( const std::size_t column, std::size_t* size ) const -> const char*;
  auto get(const std::size_t column) const -> const std::string;

  auto is_null( const std::size_t column ) const -> bool;

  auto slice( const std::size_t offset, const std::size_t in_columns ) const -> std::unique_ptr<query_result::Row>;

  template<typename T> 
  void get_as_type(const std::size_t column, T& field) const { //FIXME: Why does the explicit get call even need to exist at all? Just make this the top level interface
    std::size_t n_bytes;
    std::istringstream(get_bytes(column, &n_bytes)) >> *field; 
  }

  inline void get(const std::size_t column, bool *field) const {
    get_as_type(column, field);
  }

  inline void get(const std::size_t column, int32_t *field) const {
    get_as_type(column, field);
  }

  inline void get(const std::size_t column, int64_t *field) const {
    get_as_type(column, field);
  }

  inline void get(const std::size_t column, uint32_t *field) const {
    get_as_type(column, field);
  }

  inline void get(const std::size_t column, uint64_t *field) const {
    get_as_type(column, field);
  }

  inline void get(const std::size_t column, double *field) const {
    get_as_type(column, field);
  }

  inline void get(const std::size_t column, std::string *field) const {
    get_as_type(column, field);
  }

  void get_serialized(const std::size_t column, bool *field) const;
  void get_serialized(const std::size_t column, int32_t *field) const;
  void get_serialized(const std::size_t column, int64_t *field) const;
  void get_serialized(const std::size_t column, uint32_t *field) const;
  void get_serialized(const std::size_t column, uint64_t *field) const;
  void get_serialized(const std::size_t column, double *field) const;
  void get_serialized(const std::size_t column, std::string *field) const;
};

}

#endif /* PROTO_WRAPPER_ROW_H */