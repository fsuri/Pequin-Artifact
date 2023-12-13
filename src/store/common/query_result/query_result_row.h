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
#ifndef ROW_H
#define ROW_H

#include <string>
#include <vector>
#include <optional>
#include <memory>
#include "query_result_field.h"
#include <tao/pq/result_traits.hpp>

namespace query_result
{

	// A row in a table, contains an ordered collection of fields
	class Row
	{
		public:
			virtual ~Row() = default;

			virtual auto slice(const std::size_t offset, const std::size_t in_columns) const -> std::unique_ptr<Row> = 0;

			virtual auto operator[](const std::size_t column) const -> std::unique_ptr<Field> = 0;

			virtual auto num_columns() const noexcept -> std::size_t = 0;

			virtual auto name(const std::size_t column) const -> std::string = 0;

			virtual auto is_null(const std::size_t column) const -> bool = 0;

			// If the data type you need is not supported, you can add it here
			virtual void get(const std::size_t column, bool *field) const = 0;
			virtual void get(const std::size_t column, int32_t *field) const = 0;
			virtual void get(const std::size_t column, int64_t *field) const = 0;
			virtual void get(const std::size_t column, uint32_t *field) const = 0;
			virtual void get(const std::size_t column, uint64_t *field) const = 0;
			virtual void get(const std::size_t column, double *field) const = 0;
			virtual void get(const std::size_t column, std::string *field) const = 0;
	};
}

#endif /* ROW_H */