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

namespace query_result {

// A row in a table, contains an ordered collection of fields
class Row {
	public:
		class const_iterator {
			public:
				virtual auto operator++() noexcept -> const_iterator& = 0;

        virtual auto operator++( int ) -> std::unique_ptr<const_iterator> = 0;

        virtual auto operator+=( const std::int32_t n ) noexcept -> const_iterator& = 0;

        virtual auto operator--() noexcept -> const_iterator& = 0;

        virtual auto operator--( int ) noexcept -> std::unique_ptr<const_iterator> = 0;

        virtual auto operator-=( const std::int32_t n ) noexcept -> const_iterator& = 0;

        virtual auto operator*() const noexcept -> const Field& = 0;

        virtual auto operator->() const noexcept -> const Field* = 0;

        virtual auto operator[]( const std::int32_t n ) const noexcept -> std::unique_ptr<Field> = 0;
		};

		virtual auto slice( const std::size_t offset, const std::size_t in_columns ) const -> std::unique_ptr<Row> = 0;

	  virtual auto operator[]( const std::size_t column ) const -> std::unique_ptr<Field> = 0;
		
		virtual auto columns() const noexcept -> std::size_t = 0;

		virtual auto name( const std::size_t column ) const -> std::string = 0;

		// iteration
    virtual auto begin() const -> std::unique_ptr<const_iterator> = 0;

    virtual auto end() const -> std::unique_ptr<const_iterator> = 0;

    virtual auto cbegin() const -> std::unique_ptr<const_iterator> = 0;

    virtual auto cend() const -> std::unique_ptr<const_iterator> = 0;

		virtual auto get( const std::size_t column, std::size_t* size ) const -> const char* = 0;

		virtual auto is_null( const std::size_t column ) const -> bool = 0;
};

}

#endif /* ROW_H */