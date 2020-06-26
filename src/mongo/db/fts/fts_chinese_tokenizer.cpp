#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kQuery

#include "mongo/platform/basic.h"

#include "mongo/db/fts/fts_chinese_tokenizer.h"

#include "mongo/db/fts/fts_query_impl.h"
#include "mongo/db/fts/fts_spec.h"
#include "mongo/db/fts/stemmer.h"
#include "mongo/db/fts/stop_words.h"
#include "mongo/db/fts/tokenizer.h"
#include "mongo/db/server_options.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/stringutils.h"

namespace mongo {
namespace fts {

ChineseFTSTokenizer::ChineseFTSTokenizer(const FTSLanguage* language,
                                         const cppjieba::Jieba* segmenter)
    : _language(language), _segmenter(segmenter) {}

void ChineseFTSTokenizer::reset(StringData document, Options options) {
    (void)options;
    _document = document.toString();
    const auto& tmp = split(_document);
    for (const auto& v : tmp) {
        _words.emplace_back(v);
    }
    _stem = "";
}

std::list<std::string> ChineseFTSTokenizer::split(const StringData& doc) {
    std::vector<std::string> tmp;
    std::list<std::string> result;
    LOG(2) << "split string " << doc;
    _segmenter->CutForSearch(doc.toString(), tmp);
    LOG(2) << "split string size " << tmp.size();
    for (const auto& v : tmp) {
        LOG(2) << "ChineseFTSTokenizer " << v;
        if (v == "\n" || v == "\t" || v == "\r" || v == "\f" || v == "\v" || v == " ") {
            continue;
        }
        result.push_back(v);
    }
    return result;
}

bool ChineseFTSTokenizer::moveNext() {
    if (_words.size() == 0) {
        _stem = "";
        return false;
    }
    _stem = *(_words.begin());
    _words.pop_front();
    return true;
}

StringData ChineseFTSTokenizer::get() const {
    return _stem;
}

}  // namespace fts
}  // namespace mongo
