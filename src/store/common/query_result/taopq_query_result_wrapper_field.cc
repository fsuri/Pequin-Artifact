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

#include "store/common/query_result/taopq_query_result_wrapper_field.h"

namespace taopq_wrapper {

auto Field::name() const -> std::string {
    return _field.name();
}

auto Field::index() const -> std::size_t {
    return _field.index();
}

bool Field::is_null() const {
    return _field.is_null();
}

auto Field::get_bytes(std::size_t* size) const -> const char* {
    return _field.get();
}

auto Field::get(bool *field) const -> void {
    *field = _field.as<bool>();
}

auto Field::get(int32_t *field) const -> void {
    *field = _field.as<int32_t>();
}

auto Field::get(int64_t *field) const -> void {
    *field = _field.as<int64_t>();
}

auto Field::get(uint32_t *field) const -> void {
    *field = _field.as<uint32_t>();
}

auto Field::get(uint64_t *field) const -> void {
    *field = _field.as<uint64_t>();
}

auto Field::get(double *field) const -> void {
    *field = _field.as<double>();
}

auto Field::get(std::string *field) const -> void {
    *field = _field.as<std::string>();
}

}
