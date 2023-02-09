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

#include <string>
#include <vector>
#include <tao/pq.hpp>
#include "store/common/query_result.h"
#include "store/common/query_result_row.h"
#include "store/common/taopq_query_result_wrapper_row.h"

namespace taopq_wrapper {

class TaoPQQueryResultWrapper : query_result::QueryResult {
  private:
    tao::pq::result* result;

	public:
    TaoPQQueryResultWrapper(tao::pq:result* taopq_result) {
      result = taopq_result
    }

    ~TaoPQQueryResultWrapper() {
    }

		class const_iterator : query_result::QueryResult::const_iterator {
      private:
        tao::pq::result::const_iterator taopq_const_iterator;
        taopq_wrapper::Row row;

        explicit const_iterator( const tao::pq::result::const_iterator iterator) noexcept
        {
          taopq_const_iterator = iterator;
        }
			public:
        const_iterator() = default;

				auto operator++() noexcept -> query_result::QueryResult::const_iterator& 
        {
          taopq_const_iterator++;
          return *this;
        }

        auto operator++( int ) noexcept -> query_result::QueryResult::const_iterator
        {
          const_iterator nrv( *this );
          ++*this;
          return nrv;
        }

        auto operator+=( const std::int32_t n ) noexcept -> query_result::QueryResult::const_iterator&
        {
          taopq_const_iterator += n;
          return *this;
        }

        auto operator--() noexcept -> query_result::QueryResult::const_iterator&
        {
          --taopq_const_iterator;
          return *this;
        }

        auto operator--( int ) noexcept -> query_result::QueryResult::const_iterator
        {
          const_iterator nrv( *this );
          --*this;
          return nrv;
        }

        auto operator-=( const std::int32_t n ) noexcept -> query_result::QueryResult::const_iterator&
        {
          taopq_const_iterator -= n;
          return *this;
        }

        auto operator*() const noexcept -> const query_result::Row& 
        {
          if(row == null) {
            row = taopq_wrapper::Row(*taopq_const_iterator);
          }
          return *row;
        }

        auto operator->() const noexcept -> const query_result::Row*
        {
          if(row == null) {
            row = taopq_wrapper::Row(*taopq_const_iterator);
          }
          return row;
        }

        auto operator[]( const std::int32_t n ) const noexcept -> query_result::Row
        {
          return taopq_wrapper::Row(taopq_const_iterator + n);
        }
		};

		auto name( const std::size_t column ) const -> std::string;

		// size of the result set
		bool empty() const;
		auto size() const -> std::size_t;

		// iteration
    auto begin() const -> query_result::QueryResult::const_iterator*;
    auto end() const -> query_result::QueryResult::const_iterator*;

		auto cbegin() const -> query_result::QueryResult::const_iterator* {
      return begin();
    }

    auto cend() const -> query_result::QueryResult::const_iterator* {
      return end();
    }
		
		// access rows
    auto operator[]( const std::size_t row ) const -> query_result::Row;
    auto at( const std::size_t row ) const -> query_result::Row;

		// update/insert result
		auto has_rows_affected() const noexcept -> bool;
		auto rows_affected() const -> std::size_t;
};

}
