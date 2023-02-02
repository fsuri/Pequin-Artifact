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
#include "store/common/query_result.h"
#include "store/common/query_result_row.h"
#include "store/common/query-result-proto.pb.h"
#include "store/common/sql_result_row.h"

typedef SQLResult SQLResultProto;
typedef Row RowProto;
typedef Field FieldProto;

namespace sql {

class SQLResult : query_result::QueryResult {
  private:
    int rows_affected;
    std::vector<std::string> column_names;
    std::vector<std::vector<std::string>> result;

	public:
    SQLResult(SQLResultProto* proto_result) {
      rows_affected = proto_result->rows_affected();
      column_names = std::vector<std::string>(proto_result->column_names().begin(), proto_result->column_names().end());
      result = std::vector<std::vector<std::string>>();
      for(int i = 0; i < proto_result->rows_size(); i++) {
        RowProto row = proto_result->rows(i);
        auto fields_vector = std::vector<FieldProto>(row.fields().begin(), row.fields().end());
        auto data_vector = std::vector<std::string>();
        for (auto field : fields_vector) {
          data_vector.push_back(field.data());
        }
        result.push_back(data_vector);
      }
    }

    SQLResult(const std::string& data) {
      SQLResultProto result;
      if(!result.ParseFromString(data)) {
        throw std::invalid_argument("Failed to parse SQLResult from data");
      }
    }

    ~SQLResult() {
    }

		class const_iterator : query_result::QueryResult::const_iterator {
      private:
        friend class SQLResult;

        explicit const_iterator( const sql::Row& r ) noexcept
            : sql::Row( r )
         {}
			public:
        const_iterator() = default;

				auto operator++() noexcept -> query_result::QueryResult::const_iterator& 
        {
            ++m_row;
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
          m_row += n;
          return *this;
        }

        auto operator--() noexcept -> query_result::QueryResult::const_iterator&
        {
          --m_row;
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
          m_row -= n;
          return *this;
        }

        auto operator*() const noexcept -> const query_result::Row& 
        {
          return *this;
        }

        auto operator->() const noexcept -> const query_result::Row*
        {
          return this;
        }

        auto operator[]( const std::int32_t n ) const noexcept -> query_result::Row
        {
          return *( *this + n );
        }
		};

		auto name( const std::size_t column ) const -> std::string;

		// size of the result set
		bool empty() const;
		auto size() const -> std::size_t;

		// iteration
    auto begin() const -> query_result::QueryResult::const_iterator;
    auto end() const -> query_result::QueryResult::const_iterator;

		auto cbegin() const -> query_result::QueryResult::const_iterator;
    auto cend() const -> query_result::QueryResult::const_iterator;
		
		// access rows
    auto operator[]( const std::size_t row ) const -> query_result::Row;
    auto at( const std::size_t row ) const -> query_result::Row;

		// update/insert result
		auto has_rows_affected() const noexcept -> bool;
		auto rows_affected() const -> std::size_t;
};

}
