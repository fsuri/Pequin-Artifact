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
#include <memory>
#include <tao/pq.hpp>
#include "store/common/query_result.h"
#include "store/common/query_result_row.h"
#include "store/common/taopq_query_result_wrapper_row.h"

namespace taopq_wrapper {

class TaoPQQueryResultWrapper : public query_result::QueryResult {
  private:
    tao::pq::result* result;

	public:
    TaoPQQueryResultWrapper(tao::pq::result* taopq_result) {
      result = taopq_result;
    }

    ~TaoPQQueryResultWrapper() {
    }

		auto name( const std::size_t column ) const -> std::string;

		// size of the result set
		bool empty() const;
		auto size() const -> std::size_t;
    auto columns() const -> std::size_t;

    auto is_null( const std::size_t row, const std::size_t column ) const -> bool;
    auto get( const std::size_t row, const std::size_t column, std::size_t* size ) const -> const char*;
		
		// access rows
    auto operator[]( const std::size_t row ) const -> std::unique_ptr<query_result::Row>;
    auto at( const std::size_t row ) const -> std::unique_ptr<query_result::Row>;

		// update/insert result
		auto has_rows_affected() const noexcept -> bool;
		auto rows_affected() const -> std::size_t;
};

}
