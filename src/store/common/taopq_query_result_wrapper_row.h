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

#include "store/common/query_result_row.h"
#include "store/common/query_result_field.h"

namespace taopq_wrapper {

class Row : query_result::Row {
 private:
  tao::pq::row row;

 public:
  Row() = default;

  Row( tao::pq::row* in_row ) noexcept
      : row(in_row)
  {}

  class const_iterator : query_result::Row::const_iterator {
    private:
      tao::pq::row::const_iterator taopq_const_iterator;
      taopq_wrapper::Field field;

      explicit const_iterator( const tao::pq::row::const_iterator iterator) noexcept
      {
        taopq_const_iterator = iterator;
      }
    public:
      const_iterator() = default;

      auto operator++() noexcept -> query_result::Row::const_iterator&
      {
        taopq_const_iterator++;
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
        taopq_const_iterator += n;
        return *this;
      }

      auto operator--() noexcept -> query_result::Row::const_iterator&
      {
        --taopq_const_iterator;
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
        taopq_const_iterator -= n;
        return *this;
      }

      auto operator*() const noexcept -> const query_result::Field&
      {
        if(field == null) {
          field = taopq_wrapper::Field(*taopq_const_iterator);
        }
        return *field;
      }

      auto operator->() const noexcept -> const query_result::Field*
      {
        if(field == null) {
          field = taopq_wrapper::Field(*taopq_const_iterator);
        }
        return field;
      }

      auto operator[]( const std::int32_t n ) const noexcept -> query_result::Field
      {
        return taopq_wrapper::Field(taopq_const_iterator + n);
      }
  };

  auto operator[]( const std::size_t column ) const -> query_result::Field;
  
  auto columns() const noexcept -> std::size_t;

  auto name( const std::size_t column ) const -> std::string;

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

  auto slice( const std::size_t offset, const std::size_t in_columns ) const -> query_result::Row;
};

}