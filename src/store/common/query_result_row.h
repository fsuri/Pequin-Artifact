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
#include "query_result_field.h"
#include <tao/pq/result_traits.hpp>

namespace query_result {

// A row in a table, contains an ordered collection of fields
class Row {
	public:
		class const_iterator : protected Field {
			public:
				virtual auto operator++() noexcept -> const_iterator& = 0;

        virtual auto operator++( int ) noexcept -> const_iterator = 0;

        virtual auto operator+=( const std::int32_t n ) noexcept -> const_iterator& = 0;

        virtual auto operator--() noexcept -> const_iterator& = 0;

        virtual auto operator--( int ) noexcept -> const_iterator = 0;

        virtual auto operator-=( const std::int32_t n ) noexcept -> const_iterator& = 0;

        virtual auto operator*() const noexcept -> const Field& = 0;

        virtual auto operator->() const noexcept -> const Field* = 0;

        virtual auto operator[]( const std::int32_t n ) const noexcept -> Field = 0;
		};

		virtual auto slice( const std::size_t offset, const std::size_t in_columns ) const -> Row = 0;

	  virtual auto operator[]( const std::size_t column ) const -> Field = 0;
		
		virtual auto columns() const noexcept -> std::size_t = 0;

		virtual auto name( const std::size_t column ) const -> std::string = 0;

		// iteration
    virtual auto begin() const -> const_iterator = 0;

    virtual auto end() const -> const_iterator = 0;

    virtual auto cbegin() const -> const_iterator = 0;

    virtual auto cend() const -> const_iterator = 0;

		virtual auto get( const std::size_t column ) const -> const char* = 0;

		virtual auto is_null( const std::size_t column ) const -> bool = 0;

		template< typename T >
    auto get( const std::size_t column ) const -> T {
			if constexpr( result_traits_size< T > == 0 ) {
				static_assert( false, "tao::pq::result_traits<T>::size yields zero" );
				__builtin_unreachable();
			}
			else if constexpr( result_traits_size< T > == 1 ) {
				if constexpr( result_traits_has_null< T > ) {
						if( is_null( column ) ) {
							return result_traits< T >::null();
						}
				}
				return result_traits< T >::from( get( column ) );
			}
			else {
				return result_traits< T >::from( slice( column, result_traits_size< T > ) );
			}
		}

    template< typename T >
    auto optional( const std::size_t column ) const
    {
       return get< std::optional< T > >( column );
    }
};

}

#endif /* ROW_H */