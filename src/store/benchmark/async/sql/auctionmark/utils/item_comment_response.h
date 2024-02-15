#ifndef AUCTIONMARK_ITEM_COMMENT_RESPONSE_H
#define AUCTIONMARK_ITEM_COMMENT_RESPONSE_H

#include <string>
#include <optional>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>

namespace auctionmark
{

  class ItemCommentResponse
  {
  private:
    long comment_id;
    std::string item_id;
    std::string seller_id;

    friend class boost::serialization::access;
    // When the class Archive corresponds to an output archive, the
    // & operator is defined similar to <<.  Likewise, when the class Archive
    // is a type of input archive the & operator is defined similar to >>.
    template <class Archive>
    void serialize(Archive &ar, const unsigned int version)
    {
      ar & comment_id;
      ar & item_id;
      ar & seller_id;
    }

  public:
    inline ItemCommentResponse(long comment_id, std::string item_id, std::string seller_id)
        : comment_id(comment_id), item_id(item_id), seller_id(seller_id) {}
    ItemCommentResponse() = default;
    ~ItemCommentResponse() = default;

    inline long get_comment_id() const
    {
      return this->comment_id;
    }

    inline std::string get_item_id() const
    {
      return this->item_id;
    }

    inline std::string get_seller_id() const
    {
      return this->seller_id;
    }

    inline bool operator==(const ItemCommentResponse &other) const
    {
      return comment_id == other.comment_id && item_id == other.item_id && seller_id == other.seller_id;
    }
  };

} // namespace auctionmark

#endif // AUCTIONMARK_ITEM_COMMENT_RESPONSE_H
