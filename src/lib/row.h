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
#include "row.h"

namespace query_result {

// QueryResult contains a collection of rows containing one or more fields of data.
class QueryResult {
	public:
		auto name( const std::size_t column ) const -> std::string;

		// size of the result set
		bool empty() const;
		auto size() const -> std::size_t;

		// iteration
    auto begin() const -> std::vector<Row>::const_iterator;
    auto end() const -> std::vector<Row>::const_iterator;

		auto cbegin() const -> std::vector<Row>::const_iterator;
    auto cend() const -> std::vector<Row>::const_iterator;
		
		// access rows
    auto operator[]( const std::size_t row ) const -> Row;
    auto at( const std::size_t row ) const -> Row;
};

}

#endif /* ROW_H */