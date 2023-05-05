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

#include <memory>
#include <tao/pq.hpp>
#include <tao/pq/row.hpp>
#include "store/common/query_result/query_result_row.h"
#include "store/common/query_result/query_result_field.h"

namespace taopq_wrapper {

class Row : public query_result::Row {
 private:
  tao::pq::row row;

 public:
  Row() = default;

  Row( tao::pq::row in_row ) noexcept
      : row(in_row)
  {}

  auto operator[]( const std::size_t column ) const -> std::unique_ptr<query_result::Field>;
  
  auto num_columns() const noexcept -> std::size_t;

  auto name( const std::size_t column ) const -> std::string;

  auto get( const std::size_t column, std::size_t* size ) const -> const char*;

  auto is_null( const std::size_t column ) const -> bool;

  auto slice( const std::size_t offset, const std::size_t in_columns ) const -> std::unique_ptr<query_result::Row>;
};

}