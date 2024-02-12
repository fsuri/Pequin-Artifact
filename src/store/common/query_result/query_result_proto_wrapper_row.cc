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
#include "store/common/query_result/query_result_proto_wrapper.h"
#include "store/common/query_result/query_result_proto_wrapper_row.h"

namespace sql {

auto Row::num_columns() const noexcept -> std::size_t
{
  return m_result->num_columns();
}

auto Row::name( std::size_t column ) const -> std::string
{
  return m_result->name( m_offset + column );
}

auto Row::column_index_by_name(const std::string &name) const -> std::size_t {
  return m_result->column_index_by_name(name);
}

auto Row::begin() const -> std::unique_ptr<const_iterator>
{
  return std::unique_ptr<const_iterator>(new const_iterator( Field( *this, m_offset ) ));
}

auto Row::end() const -> std::unique_ptr<const_iterator>
{
  return std::unique_ptr<const_iterator>(new const_iterator( Field( *this, m_offset + m_columns ) ));
}

auto Row::get_bytes( const std::size_t column, std::size_t* size ) const -> const char*
{
  //std::cerr << "get bytes for row: " << m_row << " at col position: " << (m_offset + column) << std::endl;
  return m_result->get_bytes( m_row, m_offset + column, size );
}

auto Row::get( const std::size_t column) const -> const std::string
{
  return m_result->get( m_row, m_offset + column);
}

auto Row::is_null( const std::size_t column ) const -> bool 
{
  return m_result->is_null( m_row, m_offset + column );
}

auto Row::slice( const std::size_t offset, const std::size_t in_columns ) const -> std::unique_ptr<query_result::Row>
{
  assert( m_result );
  if( in_columns == 0 ) {
      throw std::invalid_argument( "slice requires at least one column" );
  }
  if( offset + in_columns > m_columns ) {
      throw std::out_of_range("slice out of range");
  }
  return std::unique_ptr<query_result::Row>(new Row(m_result, this->m_row, m_offset + offset, in_columns ));
}

void Row::get_serialized(const std::size_t column, bool *field) const {
  deserialize(column, *field);
}

void Row::get_serialized(const std::size_t column, int32_t *field) const {
  deserialize(column, *field);
}

void Row::get_serialized(const std::size_t column, int64_t *field) const {
  deserialize(column, *field);
}

void Row::get_serialized(const std::size_t column, uint32_t *field) const {
  get_serialized(column, reinterpret_cast<int32_t*>(field));
}

void Row::get_serialized(const std::size_t column, uint64_t *field) const {
  get_serialized(column, reinterpret_cast<int64_t*>(field));
}

void Row::get_serialized(const std::size_t column, double *field) const {
  deserialize(column, *field);
}

void Row::get_serialized(const std::size_t column, std::string *field) const {
  deserialize(column, *field);
}

}