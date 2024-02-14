#include "store/benchmark/async/sql/auctionmark/utils/composite_id.h"

namespace auctionmark {

const std::string CompositeId::PAD_STRING = "0";

std::string CompositeId::encode(const std::vector<int>& offset_bits) const {
    std::stringstream compositeBuilder;

    std::vector<std::string> decodedValues = this->to_vec();
    for (int i = 0; i < decodedValues.size(); i++) {
        std::string value = decodedValues[i];
        int valueLength = offset_bits[i];
        compositeBuilder << std::setw(valueLength) << std::setfill(PAD_STRING[0]) << value;
    }

    return compositeBuilder.str();
}

std::vector<std::string> CompositeId::decode(const std::string& composite_id, const std::vector<int>& offset_bits) {
    std::vector<std::string> decodedValues(offset_bits.size());

    int start = 0;
    for (int i = 0; i < decodedValues.size(); i++) {
        int valueLength = offset_bits[i];
        int end = start + valueLength;
        decodedValues[i] = composite_id.substr(start, valueLength);
        start = end;
    }
    return decodedValues;
}

} // namespace auctionmark
