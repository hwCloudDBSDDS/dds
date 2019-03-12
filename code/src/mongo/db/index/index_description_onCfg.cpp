// index_descriptor.cpp

/**
*    Copyright (C) 2014 10gen Inc.
*
*    This program is free software: you can redistribute it and/or  modify
*    it under the terms of the GNU Affero General Public License, version 3,
*    as published by the Free Software Foundation.
*
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU Affero General Public License for more details.
*
*    You should have received a copy of the GNU Affero General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*
*    As a special exception, the copyright holders give permission to link the
*    code of portions of this program with the OpenSSL library under certain
*    conditions as described in each individual source file and distribute
*    linked combinations including the program with the OpenSSL library. You
*    must comply with the GNU Affero General Public License in all respects for
*    all of the code used other than as permitted herein. If you modify file(s)
*    with this exception, you may extend this exception to your version of the
*    file(s), but you are not obligated to do so. If you do not wish to do so,
*    delete this exception statement from your version. If you delete this
*    exception statement from all source files in the program, then also delete
*    it in the license file.
*/

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kIndex

#include "mongo/platform/basic.h"

#include "mongo/db/index/index_description_onCfg.h"

#include <algorithm>

#include "mongo/bson/simple_bsonelement_comparator.h"
#include "mongo/util/log.h"

namespace mongo {

using IndexVersion = IndexDescriptionOnCfg::IndexVersion;

namespace {
void populateOptionsMap(std::map<StringData, BSONElement>& theMap, const BSONObj& spec) {
    BSONObjIterator it(spec);
    while (it.more()) {
        const BSONElement e = it.next();

        StringData fieldName = e.fieldNameStringData();
        if (fieldName == IndexDescriptionOnCfg::kKeyPatternFieldName1 ||
            fieldName == IndexDescriptionOnCfg::kNamespaceFieldName1 ||
            fieldName == IndexDescriptionOnCfg::kIndexNameFieldName1 ||
            fieldName ==
                IndexDescriptionOnCfg::kIndexVersionFieldName1 ||  // not considered for equivalence
            fieldName == IndexDescriptionOnCfg::kTextVersionFieldName1 ||  // same as index version
            fieldName ==
                IndexDescriptionOnCfg::k2dsphereVersionFieldName1 ||      // same as index version
            fieldName == IndexDescriptionOnCfg::kBackgroundFieldName1 ||  // this is a creation time
                                                                          // option only
            fieldName == IndexDescriptionOnCfg::kDropDuplicatesFieldName1 ||  // this is now ignored
            fieldName == IndexDescriptionOnCfg::kSparseFieldName1 ||          // checked specially
            fieldName == IndexDescriptionOnCfg::kUniqueFieldName1 ||          // check specially
            fieldName == IndexDescriptionOnCfg::kPrefix1) {
            continue;
        }
        theMap[fieldName] = e;
    }
}
}  // namespace

constexpr StringData IndexDescriptionOnCfg::k2dIndexBitsFieldName1;
constexpr StringData IndexDescriptionOnCfg::k2dIndexMaxFieldName1;
constexpr StringData IndexDescriptionOnCfg::k2dIndexMinFieldName1;
constexpr StringData IndexDescriptionOnCfg::k2dsphereCoarsestIndexedLevel1;
constexpr StringData IndexDescriptionOnCfg::k2dsphereFinestIndexedLevel1;
constexpr StringData IndexDescriptionOnCfg::k2dsphereVersionFieldName1;
constexpr StringData IndexDescriptionOnCfg::kBackgroundFieldName1;
constexpr StringData IndexDescriptionOnCfg::kCollationFieldName1;
constexpr StringData IndexDescriptionOnCfg::kDefaultLanguageFieldName1;
constexpr StringData IndexDescriptionOnCfg::kDropDuplicatesFieldName1;
constexpr StringData IndexDescriptionOnCfg::kExpireAfterSecondsFieldName1;
constexpr StringData IndexDescriptionOnCfg::kGeoHaystackBucketSize1;
constexpr StringData IndexDescriptionOnCfg::kIndexNameFieldName1;
constexpr StringData IndexDescriptionOnCfg::kIndexVersionFieldName1;
constexpr StringData IndexDescriptionOnCfg::kKeyPatternFieldName1;
constexpr StringData IndexDescriptionOnCfg::kLanguageOverrideFieldName1;
constexpr StringData IndexDescriptionOnCfg::kNamespaceFieldName1;
constexpr StringData IndexDescriptionOnCfg::kPartialFilterExprFieldName1;
constexpr StringData IndexDescriptionOnCfg::kSparseFieldName1;
constexpr StringData IndexDescriptionOnCfg::kStorageEngineFieldName1;
constexpr StringData IndexDescriptionOnCfg::kTextVersionFieldName1;
constexpr StringData IndexDescriptionOnCfg::kUniqueFieldName1;
constexpr StringData IndexDescriptionOnCfg::kWeightsFieldName1;
constexpr StringData IndexDescriptionOnCfg::kPrefix1;

bool IndexDescriptionOnCfg::isIndexVersionSupported(IndexVersion indexVersion) {
    switch (indexVersion) {
        case IndexVersion::kV0:
        case IndexVersion::kV1:
        case IndexVersion::kV2:
            return true;
    }
    return false;
}

std::set<IndexVersion> IndexDescriptionOnCfg::getSupportedIndexVersions() {
    return {IndexVersion::kV0, IndexVersion::kV1, IndexVersion::kV2};
}

Status IndexDescriptionOnCfg::isIndexVersionAllowedForCreation(
    IndexVersion indexVersion,
    const ServerGlobalParams::FeatureCompatibility& featureCompatibility,
    const BSONObj& indexSpec) {
    switch (indexVersion) {
        case IndexVersion::kV0:
            break;
        case IndexVersion::kV1:
            return Status::OK();
        case IndexVersion::kV2: {
            if (ServerGlobalParams::FeatureCompatibility::Version::k32 ==
                    featureCompatibility.version.load() &&
                featureCompatibility.validateFeaturesAsMaster.load()) {
                return {ErrorCodes::CannotCreateIndex,
                        str::stream() << "Invalid index specification " << indexSpec
                                      << "; cannot create an index with v="
                                      << static_cast<int>(IndexVersion::kV2)
                                      << " when the featureCompatibilityVersion is 3.2. See "
                                         "http://dochub.mongodb.org/core/"
                                         "3.4-feature-compatibility."};
            }
            return Status::OK();
        }
    }
    return {ErrorCodes::CannotCreateIndex,
            str::stream() << "Invalid index specification " << indexSpec
                          << "; cannot create an index with v="
                          << static_cast<int>(indexVersion)};
}

IndexVersion IndexDescriptionOnCfg::getDefaultIndexVersion(
    ServerGlobalParams::FeatureCompatibility::Version featureCompatibilityVersion) {
    ServerGlobalParams::FeatureCompatibility featureCompatibility;
    featureCompatibility.version.store(featureCompatibilityVersion);
    // We always pass validateFeaturesAsMaster=true since this flag should not affect our
    // determination of the default index version.
    featureCompatibility.validateFeaturesAsMaster.store(true);
    // We pass in an empty object for the index specification because it is only used within the
    // error reason.
    if (!IndexDescriptionOnCfg::isIndexVersionAllowedForCreation(
             IndexVersion::kV2, featureCompatibility, BSONObj())
             .isOK()) {
        // When the featureCompatibilityVersion is 3.2, we use index version v=1 as the default
        // index version.
        return IndexVersion::kV1;
    }

    // When the featureCompatibilityVersion is 3.4, we use index version v=2 as the default index
    // version.
    return IndexVersion::kV2;
}

bool IndexDescriptionOnCfg::areIndexOptionsEquivalent(const IndexDescriptionOnCfg* other) const {
    if (isSparse() != other->isSparse()) {
        return false;
    }

    if (!isIdIndex() && unique() != other->unique()) {
        // Note: { _id: 1 } or { _id: -1 } implies unique: true.
        return false;
    }

    // Then compare the rest of the options.

    std::map<StringData, BSONElement> existingOptionsMap;
    populateOptionsMap(existingOptionsMap, infoObj());

    std::map<StringData, BSONElement> newOptionsMap;
    populateOptionsMap(newOptionsMap, other->infoObj());

    return existingOptionsMap.size() == newOptionsMap.size() &&
        std::equal(existingOptionsMap.begin(),
                   existingOptionsMap.end(),
                   newOptionsMap.begin(),
                   [](const std::pair<StringData, BSONElement>& lhs,
                      const std::pair<StringData, BSONElement>& rhs) {
                       return lhs.first == rhs.first &&
                           SimpleBSONElementComparator::kInstance.evaluate(lhs.second ==
                                                                           rhs.second);
                   });
}
}
