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
#ifndef QUERY_RESULT_H
#define QUERY_RESULT_H

#include <string>
#include <vector>
#include <memory>
#include "query_result_row.h"

namespace query_result {

// QueryResult contains a collection of rows containing fields of data.
// SQL semantics for field ordering, primary key sorting, etc are not enforced by this interface.
class QueryResult {
	public:
		virtual auto name( const std::size_t column ) const -> std::string = 0;

		// size of the result set
		virtual bool empty() const = 0;
		virtual auto size() const -> std::size_t = 0;
		virtual auto columns() const -> std::size_t = 0;

		virtual auto is_null( const std::size_t row, const std::size_t column ) const -> bool = 0;
		virtual auto get( const std::size_t row, const std::size_t column, std::size_t* size ) const -> const char* = 0;
		
		// access rows
    virtual auto operator[]( const std::size_t row ) const -> std::unique_ptr<Row> = 0;
    virtual auto at( const std::size_t row ) const -> std::unique_ptr<Row> = 0;

		// update/insert result
		virtual auto has_rows_affected() const noexcept -> bool = 0;
		virtual auto rows_affected() const -> std::size_t = 0;
};

}

#endif /* QUERY_RESULT_H */