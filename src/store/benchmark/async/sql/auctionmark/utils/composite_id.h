#ifndef AUCTIONMARK_COMPOSITE_ID_H
#define AUCTIONMARK_COMPOSITE_ID_H

#include <string>
#include <vector>
#include <sstream>
#include <iomanip>

namespace auctionmark {

class CompositeId {
protected:
    static const std::string PAD_STRING;
    static const int INT_MAX_DIGITS = 10;
    static const int LONG_MAX_DIGITS = 19;

    std::string encode(const std::vector<int>& offset_bits) const;
    std::vector<std::string> decode(const std::string& composite_id, const std::vector<int>& offset_bits);

public:
    virtual std::string encode() const = 0;
    virtual void decode(const std::string& composite_id) = 0;
    virtual std::vector<std::string> to_vec() const = 0;
};

} // namespace auctionmark

#endif // AUCTIONMARK_COMPOSITE_ID_H
