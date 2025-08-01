/*! \file unordered_set.hpp
    \brief Support for types found in \<unordered_set\>
    \ingroup STLSupport */
/*
  Copyright (c) 2014, Randolph Voorhies, Shane Grant
  All rights reserved.

  Redistribution and use in source and binary forms, with or without
  modification, are permitted provided that the following conditions are met:
      * Redistributions of source code must retain the above copyright
        notice, this list of conditions and the following disclaimer.
      * Redistributions in binary form must reproduce the above copyright
        notice, this list of conditions and the following disclaimer in the
        documentation and/or other materials provided with the distribution.
      * Neither the name of the copyright holder nor the
        names of its contributors may be used to endorse or promote products
        derived from this software without specific prior written permission.

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
  ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
  DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY
  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
  LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
  ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/
#ifndef CEREAL_TYPES_UNORDERED_SET_HPP_
#define CEREAL_TYPES_UNORDERED_SET_HPP_

#include "lib/cereal/cereal.hpp"
#include <unordered_set>

namespace cereal
{
  namespace unordered_set_detail
  {
    //! @internal
    template <class Archive, class SetT> inline
    void save( Archive & ar, SetT const & set )
    {
      ar( make_size_tag( static_cast<size_type>(set.size()) ) );

      for( const auto & i : set )
        ar( i );
    }

    //! @internal
    template <class Archive, class SetT> inline
    void load( Archive & ar, SetT & set )
    {
      size_type size;
      ar( make_size_tag( size ) );

      set.clear();
      set.reserve( static_cast<std::size_t>( size ) );

      for( size_type i = 0; i < size; ++i )
      {
        typename SetT::key_type key;

        ar( key );
        set.emplace( std::move( key ) );
      }
    }
  }

  //! Saving for std::unordered_set
  template <class Archive, class K, class H, class KE, class A> inline
  void CEREAL_SAVE_FUNCTION_NAME( Archive & ar, std::unordered_set<K, H, KE, A> const & unordered_set )
  {
    unordered_set_detail::save( ar, unordered_set );
  }

  //! Loading for std::unordered_set
  template <class Archive, class K, class H, class KE, class A> inline
  void CEREAL_LOAD_FUNCTION_NAME( Archive & ar, std::unordered_set<K, H, KE, A> & unordered_set )
  {
    unordered_set_detail::load( ar, unordered_set );
  }

  //! Saving for std::unordered_multiset
  template <class Archive, class K, class H, class KE, class A> inline
  void CEREAL_SAVE_FUNCTION_NAME( Archive & ar, std::unordered_multiset<K, H, KE, A> const & unordered_multiset )
  {
    unordered_set_detail::save( ar, unordered_multiset );
  }

  //! Loading for std::unordered_multiset
  template <class Archive, class K, class H, class KE, class A> inline
  void CEREAL_LOAD_FUNCTION_NAME( Archive & ar, std::unordered_multiset<K, H, KE, A> & unordered_multiset )
  {
    unordered_set_detail::load( ar, unordered_multiset );
  }
} // namespace cereal

#endif // CEREAL_TYPES_UNORDERED_SET_HPP_
