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

#ifndef PROTO_WRAPPER_RESULT_H
#define PROTO_WRAPPER_RESULT_H

#include <string>
#include <vector>
#include <memory>

#include "store/common/query_result/query_result.h"
#include "store/common/query_result/query_result_row.h"
#include "store/common/query_result/query-result-proto.pb.h"
#include "store/common/query_result/query_result_proto_wrapper_row.h"

typedef SQLResult SQLResultProto;
typedef Row RowProto;

namespace sql {

class QueryResultProtoWrapper : public query_result::QueryResult {
  private:
    std::unique_ptr<SQLResultProto> proto_result;
    auto check_has_result_set() const -> void;

	public:
    QueryResultProtoWrapper(): proto_result(std::make_unique<SQLResultProto>()) {} 
    // QueryResultProtoWrapper(SQLResultProto* proto_result): proto_result(proto_result) {
    //   //create_from_proto(proto_result);
    // }
    QueryResultProtoWrapper(std::unique_ptr<SQLResultProto> proto_result) 
      : proto_result(std::move(proto_result)) {}

    QueryResultProtoWrapper(const std::string& data){
      //SQLResultProto result;
      proto_result = std::make_unique<SQLResultProto>();
      if(data.empty()){ //default case
        //create_from_proto(&result);
        //Just return empty result.
      }
      else if(proto_result->ParseFromString(data)) {
        //create_from_proto(&result);
      } else {
        throw std::invalid_argument("Failed to parse QueryResultProtoWrapper from data");
      }
    }
    
    // Delete copy constructor and assignment operator to enforce ownership of proto
    QueryResultProtoWrapper(const QueryResultProtoWrapper&) = delete;
    QueryResultProtoWrapper& operator=(const QueryResultProtoWrapper&) = delete;

    // QueryResultProtoWrapper& operator=(const QueryResultProtoWrapper& old){
    //   this->proto_result.reset( new SQLResultProto(*old.proto_result));
    //   return *this;
    // }

    ~QueryResultProtoWrapper() {}

    inline void SetResult(SQLResultProto &result){
      proto_result = std::make_unique<SQLResultProto>(std::move(result));
    }

		class const_iterator : sql::Row {
      private:
        friend class QueryResultProtoWrapper;

        explicit const_iterator( const sql::Row& r ) noexcept
            : sql::Row( r )
         {}
			public:
        const_iterator() = default;

				auto operator++() noexcept -> const_iterator& 
        {
            ++m_row;
            return *this;
        }

        auto operator++( int ) noexcept -> const_iterator
        {
          const_iterator nrv( *this );
          ++*this;
          return nrv;
        }

        auto operator+=( const std::int32_t n ) noexcept -> const_iterator&
        {
          m_row += n;
          return *this;
        }

        auto operator--() noexcept -> const_iterator&
        {
          --m_row;
          return *this;
        }

        auto operator--( int ) noexcept -> const_iterator
        {
          const_iterator nrv( *this );
          --*this;
          return nrv;
        }

        auto operator-=( const std::int32_t n ) noexcept -> const_iterator&
        {
          m_row -= n;
          return *this;
        }

        auto operator*() const noexcept -> const sql::Row& 
        {
          return *this;
        }

        auto operator->() const noexcept -> const sql::Row*
        {
          return this;
        }

        auto operator[]( const std::int32_t n ) const noexcept -> sql::Row
        {
          return *( *this + n );
        }

        friend auto operator+( const const_iterator& lhs, const std::int32_t rhs ) noexcept -> const_iterator
        {
          const_iterator nrv( lhs );
          nrv += rhs;
          return nrv;
        }
		};

		auto name( const std::size_t column ) const -> std::string;
    auto column_index_by_name(const std::string &name) const -> std::size_t;

		// size of the result set
		bool empty() const;
		auto size() const -> std::size_t;
    auto num_columns() const -> std::size_t;

		// iteration
    auto begin() const -> std::unique_ptr<const_iterator>;
    auto end() const -> std::unique_ptr<const_iterator>;

		auto cbegin() const -> std::unique_ptr<const_iterator> {
      return begin();
    }

    auto cend() const -> std::unique_ptr<const_iterator> {
      return end();
    }
		
    auto is_null( const std::size_t row, const std::size_t column ) const -> bool;
		auto get_bytes( const std::size_t row, const std::size_t column, std::size_t* size ) const -> const char*;
		auto get_bytes( const std::size_t row, const std::string& column, std::size_t* size ) const -> const char*;

		// access rows
    auto operator[]( const std::size_t row ) const -> std::unique_ptr<query_result::Row>;
    auto at( const std::size_t row ) const -> std::unique_ptr<query_result::Row>;

		// update/insert result
    inline void set_rows_affected(const uint32_t n_rows_affected) override { proto_result->set_rows_affected(n_rows_affected); } //Use to set n_rows for writes manually

		auto has_rows_affected() const noexcept -> bool;
		auto rows_affected() const -> std::size_t;

    auto serialize(std::string *output) const -> bool;
};

}

#endif /* PROTO_WRAPPER_RESULT_H */