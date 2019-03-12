#include <atomic>
#include <cstring>
#include <limits>
#include <sstream>

namespace mongo {

///////////////////////////////////////////////////////////////////////////////////////////////////
//
//  Class which replace uint64_t as RecordId storage type.
//  Class now can store arbitrary byte array.
//
//  Class instance occupies 16 bytes (original RecordId needed only 8).
//  If data size is not more than 14 bytes it is stored in instace (TODO: optimise to 15).
//  Otherwise, instance allocate additional block of data, which it references.
//  Block supports reference count tracking, so it's easy to share it.
//
//  Note: for OutPlaceData some of the operations below are not thread safe!!!
//  Can end up in crash or memory leak, if sombody call it from different threads.
//  E.g. if somebody compares RecordIds, while other thread overwrites it. Or when
//  one thread assign RecordId to another value, while different thread overwrites it.
//  Most probably, we shouldn't hit into these scenarios, because it produces undetermined
//  behaviour even for original uint64_t. However, for uint64_t it doesn't crash and
//  doesn't leak for sure. So, need to re-think how we can make it more safe or check
//  that nobody actually overwrites RecordId from different threads. Potentially, we
//  need to disallow assignment operators.
//
class RecordIdVariant {
protected:
    enum ObjectType : uint8_t {
        //  Flags
        DataFlag = 1 << 6,
        Int64ConvertableFlag = 1 << 7,

        Null = Int64ConvertableFlag,
        Minimum = Int64ConvertableFlag | 1,
        Maximum = Int64ConvertableFlag | 2,
        Int64 = Int64ConvertableFlag | 3,

        InPlaceData = DataFlag | 4,
        OutPlaceData = DataFlag | 5
    };

    struct Header {
        std::atomic<uint32_t> refCount;
        char data[4];

        static constexpr int HeaderSize() {
            return sizeof(refCount);
        }
    };

protected:  //  Fields
    //
    //  GCC is pretty dump when it comes to setting alignments for fields/struct
    //  After spending some time playing with union/struct and __attribute__
    //  ((packed))/__attribute__ ((aligned (4)))
    //  i didn't find any good way to align data the way i want. So i give up on using unions
    //  and just put fields sequentially. It takes 16 bytes overall, however inPlaceData can
    //  actually store 14 bytes.
    //
    union {
        struct {
            ObjectType type;
            uint8_t inPlaceSize;
            char inPlaceData[2];
            uint32_t outPlaceSize;
        };

        int64_t metaNum;
    };

    union {
        Header* header;
        int64_t intNum;
    };

    static const int MaxInPlaceSize = 14;

protected:
    bool HasHeader() const {
        return type == ObjectType::OutPlaceData;
    }

    void Release() {
        if (!HasHeader())
            return;

        if (header->refCount.fetch_sub(1) == 1) {
            delete[](char*) header;
            Clean();
        }
    }

    void Clean() {
        header = nullptr;
        type = ObjectType::Null;
    }

    void AddRef() {
        if (!HasHeader())
            return;

        header->refCount.fetch_add(1);
    }

    void AllocateData(int size) {
        invariant(size > 0);

        if (size < MaxInPlaceSize)  //  In place update
        {
            type = ObjectType::InPlaceData;
            inPlaceSize = (uint8_t)size;
            return;
        }

        type = ObjectType::OutPlaceData;
        header = (Header*)new char[Header::HeaderSize() + size];
        header->refCount = 1;
        outPlaceSize = (uint32_t)size;
    }

    void Set(const void* buffer, int size) {
        AllocateData(size);
        memcpy((void*)Data(), buffer, size);
    }

    RecordIdVariant(ObjectType inType, int64_t num) : type(inType), intNum(num) {}

    inline void CopyFieldsFrom(const RecordIdVariant& other) {
        metaNum = other.metaNum;
        intNum = other.intNum;
    }

public:
    RecordIdVariant() : RecordIdVariant(ObjectType::Null, 0) {}

    RecordIdVariant(int64_t value) : RecordIdVariant(ObjectType::Int64, value) {
        type = ObjectType::Int64;
        intNum = value;
    }

    RecordIdVariant(const void* buffer, size_t size) {
        Set(buffer, size);
    }

    RecordIdVariant(const std::string& str) {
        Set(str.c_str(), str.size());
    }

    ~RecordIdVariant() {
        Release();
    }

    RecordIdVariant(const RecordIdVariant& other) {
        CopyFieldsFrom(other);
        AddRef();
    }

    RecordIdVariant& operator=(const RecordIdVariant& other) {
        if (HasHeader() && header == other.header)
            return *this;

        Release();
        CopyFieldsFrom(other);
        AddRef();

        return *this;
    }

    RecordIdVariant(RecordIdVariant&& other) {
        CopyFieldsFrom(other);
        other.Clean();
    }

    RecordIdVariant& operator=(RecordIdVariant&& other) {
        if (HasHeader() && header == other.header)
            return *this;

        Release();
        CopyFieldsFrom(other);
        other.Clean();

        return *this;
    }

    inline const char* Data() const {
        switch (type) {
            case ObjectType::Int64:
                return (char*)&intNum;

            case ObjectType::InPlaceData:
                return inPlaceData;

            case ObjectType::OutPlaceData:
                return header->data;

            default:
                return nullptr;
        }
    }

    inline int Size() const {
        switch (type) {
            case ObjectType::InPlaceData:
                return inPlaceSize;

            case ObjectType::OutPlaceData:
                return outPlaceSize;

            case ObjectType::Int64:
                return sizeof(int32_t);

            default:
                return 0;
        }
    }

    inline bool IsNull() const {
        return type == ObjectType::Null || (type == ObjectType::Int64 && intNum == 0);
    }

    inline bool IsMin() const {
        return IsConvertableToInt64() && intNum == std::numeric_limits<int64_t>::min();
    }

    inline bool IsMax() const {
        return IsConvertableToInt64() && intNum == std::numeric_limits<int64_t>::max();
    }

    inline bool IsSpecial() const {
        return IsNull() || IsMax() || IsMin();
        ;
    }

    inline const bool IsConvertableToInt64() const {
        return (type & ObjectType::Int64ConvertableFlag);
    }

    inline const bool IsData() const {
        return (type & ObjectType::DataFlag);
    }

    static const RecordIdVariant Min() {
        return RecordIdVariant(ObjectType::Minimum, std::numeric_limits<int64_t>::min());
    }

    static const RecordIdVariant Max() {
        return RecordIdVariant(ObjectType::Maximum, std::numeric_limits<int64_t>::max());
    }

    //
    //  Todo: need to optimize
    //
    static int Compare(const RecordIdVariant& l, const RecordIdVariant& r) {
        //
        //  Check Int64
        //
        if (l.IsConvertableToInt64() && r.IsConvertableToInt64()) {
            return l.intNum == r.intNum ? 0 : (l.intNum < r.intNum) ? -1 : 1;
        }

        //
        //  Check for NULL. Not: both cannot be NULL, since NULL is convertable to Int64
        //  and in this case the previous condition would return 0;
        //
        if (l.IsNull())
            return -1;

        if (r.IsNull())
            return 1;

        //
        //  Firstly check Min/Max cases
        //
        if (l.IsMin())
            return r.IsMin() ? 0 : -1;

        if (r.IsMin())
            return 1;

        if (l.IsMax())
            return r.IsMax() ? 0 : 1;

        if (r.IsMax())
            return -1;

        invariant(l.IsData() && r.IsData());

        if (l.Data() == r.Data())
            return 0;

        auto sizeToCompare = std::min(l.Size(), r.Size());

        int result = memcmp(l.Data(), r.Data(), sizeToCompare);

        if (result != 0)
            return result;

        if (l.Size() == r.Size())
            return 0;

        return l.Size() > r.Size() ? 1 : -1;
    }

    std::string ToString() const {
        if (type == ObjectType::Int64)
            return std::to_string(intNum);

        if (IsNull())
            return "Null";

        if (IsMin())
            return "Min";

        if (IsMax())
            return "Max";

        std::stringstream stream;
        for (auto i = 0; i < Size(); i++) {
            char c = ((char*)Data())[i];
            if (isprint(c))
                stream << c;
            else
                stream << "{" << std::hex << (int)c << "}";
        }

        return stream.str();
    }

    inline int64_t ToInt64() const {
        invariant(IsConvertableToInt64());
        return intNum;
    }

    //  For backward compatibility with old MongoDB RecordId
    operator int64_t() const {
        return ToInt64();
    }

    int GetMemoryUsage() const {
        if (type == ObjectType::OutPlaceData)
            return sizeof(RecordIdVariant) + Header::HeaderSize() + outPlaceSize;

        return sizeof(RecordIdVariant);
    }

    friend class RecordId;
    friend class TestRecordIdVariant;
};


inline bool operator==(const RecordIdVariant& l, const RecordIdVariant& r) {
    return RecordIdVariant::Compare(l, r) == 0;
}

inline bool operator!=(const RecordIdVariant& l, const RecordIdVariant& r) {
    return !(l == r);
}

inline bool operator<(const RecordIdVariant& l, const RecordIdVariant& r) {
    return RecordIdVariant::Compare(l, r) < 0;
}

inline bool operator<=(const RecordIdVariant& l, const RecordIdVariant& r) {
    int result = RecordIdVariant::Compare(l, r);
    return result == 0 || result < 0;
}

inline bool operator>(const RecordIdVariant& l, const RecordIdVariant& r) {
    return RecordIdVariant::Compare(l, r) > 0;
}

inline bool operator>=(const RecordIdVariant& l, const RecordIdVariant& r) {
    int result = RecordIdVariant::Compare(l, r);
    return result == 0 || result > 0;
}

inline bool operator==(const RecordIdVariant& l, int64_t r) {
    return RecordIdVariant::Compare(l, RecordIdVariant(r)) == 0;
}

inline bool operator==(int64_t l, const RecordIdVariant& r) {
    return RecordIdVariant::Compare(RecordIdVariant(l), r) == 0;
}

};  //  mongo
