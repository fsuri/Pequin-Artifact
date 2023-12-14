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
#include "store/common/query_result/query_result_proto_wrapper_field.h"
#include "store/common/query_result/query_result_proto_wrapper_row.h"

namespace sql {
  
auto Field::name() const -> std::string
{
  return m_row->name( m_column );
}

auto Field::index() const -> std::size_t
{
  return m_column - m_row->m_offset;
}

auto Field::is_null() const -> bool
{
  return m_row->is_null( m_column );
}

auto Field::get_bytes(std::size_t* size) const -> const char*
{
  return m_row->get_bytes( m_column, size );
}

auto Field::get() const -> const std::string
{
  return m_row->get( m_column);
}

void Field::get(bool *field) const {
  m_row->get(m_column, field);
}

void Field::get(int32_t *field) const {
  m_row->get(m_column, field);
}

void Field::get(int64_t *field) const {
  m_row->get(m_column, field);
}

void Field::get(uint32_t *field) const {
  m_row->get(m_column, field);
}

void Field::get(uint64_t *field) const {
  m_row->get(m_column, field);
}

void Field::get(double *field) const {
  m_row->get(m_column, field);
}

void Field::get(std::string *field) const {
  m_row->get(m_column, field);
}

}
