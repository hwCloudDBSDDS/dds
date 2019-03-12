#pragma once
#include "common/common.h"
#include <vector>
#include <string>


const int RESOURCE_ID_LENGTH = 48;

class ResourceId
{  
public:

    ResourceId(){
        CommonMemZero(id, RESOURCE_ID_LENGTH);
    }

    ResourceId(const std::string& str) {
         CommonMemZero(id, RESOURCE_ID_LENGTH);
         CommonMemCopy(id, RESOURCE_ID_LENGTH, str.c_str(), str.size());
    }

    std::string toString() const {
        return std::string(id);
    }
 
    bool operator == (const ResourceId& other) const { 
        return std::string(id) == std::string(other.id);
    }

    bool operator != (const ResourceId& other) const {
        return std::string(id) != std::string(other.id);
    }

    bool operator < (const ResourceId& other) const {
        return std::string(id) < std::string(other.id);
    }

    ResourceId& operator = (const std::string& other) {
        CommonMemZero(id, RESOURCE_ID_LENGTH);
        CommonMemCopy(id, RESOURCE_ID_LENGTH, other.c_str(), other.size());
        return *this;
    }

    char id[RESOURCE_ID_LENGTH];
};

typedef ResourceId SharedResourceId;

inline std::ostream& operator<<(std::ostream& stream, const SharedResourceId& id)
{
   stream << std::string(id.id);
   return stream;
}

//typedef int64_t SharedResourceId;

// transaction log: in split, ENV register shared resource(like : plog Id) to transcation log

struct SharedResourceRemoveDescription{
    // it's setted by caller
    SharedResourceId id;
    // it's setted logRecordprovider; false, it means plog is not shared, space gc remove the plog self;
    // true, it means plog is shared, global gc will remove the plog
    bool shared_flag;
    // only valid for shared plog; return OK, it means logRecordprovider has written transcation log success,
    // caller can remove this plog from map; return fail, it means caller cannot remove this plog from map
    // and must retry to remove this plog
    int  remove_result;
};

class SharedResource {
  public:
   SharedResource() {}
   virtual ~SharedResource() {}

   virtual int RegisterSharedResource(std::vector<SharedResourceId>& resource_id_list) = 0;

   // if return fail, it means logRecordprovider is splitting, space gc of rocks db must retry to remove
   // the plogs in resource_id_list
   virtual int RemoveSharedResource(std::vector<SharedResourceRemoveDescription>& list) = 0;

   // if return true, it means this file is shared, the path is the file Physical path 
   virtual bool CheckSharedResource(const SharedResourceId &id, std::string &path) = 0;

};


struct DbInstanceOption{
    // DB instance's path
    std::string instance_path;
    // split-feature: support split DB
    // resource_checker is used to check file is shared or not
    std::shared_ptr<SharedResource>  shared_checker;
    //Used to print debug log not used
    std::string     info_log;
    //Used to distinguish between different DB instance
    std::string     chunk_id; 
};

