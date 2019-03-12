
#pragma once

#include <mongo/bson/bsonelement.h>
#include <mongo/bson/bsonobj.h>
#include <mongo/bson/json.h>
#include <mongo/bson/simple_bsonobj_comparator.h>

#include <string>


#define BSON_PARSER_GET_VALUE_TEMPLATE(retType, typeId, func)            \
    static bool GetValue(const mongo::BSONElement& elem, retType& out) { \
        if (elem.type() != mongo::BSONType::typeId)                      \
            return false;                                                \
        out = elem.func();                                               \
        return true;                                                     \
    }


//
//  Helper class for BSon parsing
//
class BsonParser {
public:
    static bool AreEqual(const mongo::BSONObj& obj1, const mongo::BSONObj& obj2) {
        return mongo::SimpleBSONObjComparator::kInstance.evaluate(obj1 == obj2);
    }

    static bool AreEqual(const mongo::BSONElement& element1, const mongo::BSONElement& element2) {
        return element1.woCompare(element2) == 0;
    }

    BSON_PARSER_GET_VALUE_TEMPLATE(int, NumberInt, numberInt);
    BSON_PARSER_GET_VALUE_TEMPLATE(mongo::BSONObj, Object, Obj);
    BSON_PARSER_GET_VALUE_TEMPLATE(std::vector<mongo::BSONElement>, Array, Array);
    BSON_PARSER_GET_VALUE_TEMPLATE(std::string, String, str);
    BSON_PARSER_GET_VALUE_TEMPLATE(bool, Bool, boolean);

    static bool GetValue(const mongo::BSONElement& elem, uint64_t& out) {
        if (elem.type() == (uint64_t)mongo::BSONType::NumberInt) {
            out = elem.numberInt();
            return true;
        }

        if (elem.type() == mongo::BSONType::NumberLong) {
            out = elem.numberLong();
            return true;
        }

        return false;
    }
};

/*
#define BSON_PARSER_GENERATE_FIELD_DEFINITION(fieldName, fieldType, bsonName)   \
    fieldType fieldName;

#define BSON_PARSER_GENERATE_FIELD_GETTER(fieldName, fieldType, bsonName)       \
    const fieldType& Get_##fieldName() const { return fieldName; }

#define BSON_PARSER_GENERATE_FIELD_PARSING(fieldName, fieldType, bsonName)       \
    if(!BsonParser::GetValue(fields[fieldId++], fieldName))                       \
        RES_RET(rocksdb::Status::Corruption());

#define BSON_PARSER_GENERATE_FIELD_STRING(fieldName, fieldType, bsonName)   , #bsonName


#define BSON_PARSER_GENERATE_PARSER_FUNC(FIELDS_MACROS, parserFuncName, anyField)         \
    rocksdb::Status parserFuncName(const mongo::BSONObj& bson)
    {
        std::array<mongo::StringData,8> fieldNames{anyField
FIELDS_MACROS(BSON_PARSER_GENERATE_FIELD_STRING)};
        std::array<mongo::BSONElement, fieldNames.size()> fields;
        bson.getFields(fieldNames, &fields);

        int fieldId = 0;
        FIELDS_MACROS(BSON_PARSER_GENERATE_FIELD_PARSING);
        if(meta->version != 1)
            RES_RET(rocksdb::Status::NotSupported());

        CHUNK_METADATA_PARSE_ELEMENT(fields[1], meta->collectionId);

        CHUNK_METADATA_PARSE_ELEMENT(fields[2], meta->chunkId);

        CHUNK_METADATA_PARSE_ELEMENT(fields[3], meta->keyRange.keyLow);

        CHUNK_METADATA_PARSE_ELEMENT(fields[4], meta->keyRange.keyHigh);

        CHUNK_METADATA_PARSE_ELEMENT(fields[5], meta->infoObj);

        int storageId = 0;
        CHUNK_METADATA_PARSE_ELEMENT(fields[6], storageId);
        meta->indexedRecordStorageId = (IndexedRecordStorageId)storageId;

        std::vector<mongo::BSONElement> indexes;
        CHUNK_METADATA_PARSE_ELEMENT(fields[7], indexes);

        meta->indexes.reserve(indexes.size());
        for(const mongo::BSONElement& indexElem : indexes)
        {
            mongo::BSONObj obj;
            CHUNK_METADATA_PARSE_ELEMENT(indexElem, obj);

            IndexDefinition indexDef;
            CHUNK_METADATA_PARSE_ELEMENT(obj.getField("indexId"), indexDef.indexId);
            CHUNK_METADATA_PARSE_ELEMENT(obj.getField("infoObj"), indexDef.infoObj);

            meta->indexes.push_back(indexDef);
        }

        metadata = std::move(meta);
        return rocksdb::Status::OK();
    }
*/
