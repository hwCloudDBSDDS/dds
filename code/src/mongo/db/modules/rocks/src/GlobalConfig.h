
#pragma once

#include "Chunk/BSONParser.h"
#include <string>


//
//  Please, define your global settings here in a form:
//      FIELD(FieldName, FieldType, FieldDefaultValue)
//
#define GLOBAL_CONFIG_FIELDS(FIELD)                     \
    FIELD(ConfigSettingsVersion, int, 1)                \
    FIELD(UseStreamEnv, bool, false)                    \
    FIELD(UsePLogEnv, bool, false)                      \
    FIELD(UseMultiRocksDBInstanceEngine, bool, true)    \
    FIELD(GenerateRootPlogOnSS, bool, true)             \
    FIELD(SSRegisterToCS, bool, true)                   \
    FIELD(SSHeartBeat, bool, true)                      \
    FIELD(StatusRenewal, bool, false)                   \
    FIELD(DefaultStorageTypeId, int, 1)                 \
    FIELD(enableGlobalGC, bool, false)                  \
    FIELD(enableAutoChunkSplit, bool, false)            \
    FIELD(UsePrefixBloomFilter, bool, false)            \
    FIELD(IsCoreTest, bool, false)                      \
    FIELD(hdfsUri, std::string, "")                     \

namespace mongo
{

#define GLOBAL_CONFIG_DEFINE_FIELD(name, type, defaultValue)    type __##name = defaultValue;

#define GLOBAL_CONFIG_DEFINE_GETTER(name, type, defaultValue)   static const type& Get_##name();

//  Refrain from using for anything except UT
#define GLOBAL_CONFIG_DEFINE_UNSAFE_LOCAL_SETTER(name, type, defaultValue)   static void UnsafeLocalSet_##name(type&&);

#define GLOBAL_CONFIG_GET(name) mongo::GlobalConfig::Get_##name()
#define GLOBAL_CONFIG_UNSAFE_LOCAL_SET(name, value) mongo::GlobalConfig::UnsafeLocalSet_##name(value)


class GlobalConfig
{
    friend class GlobalConfigTest;

    static constexpr const char* const defaultConfigLocation = "/etc/maas.cfg";

protected:
    //
    //  TODO: Support for config consistency through multi-versioning
    //
    class GlobalConfigVersion
    {
        public:
        int verions = 0;
        GLOBAL_CONFIG_FIELDS(GLOBAL_CONFIG_DEFINE_FIELD);
    };
    
    GlobalConfigVersion* currentVersion;
    
    void LoadText(const std::string& config);

    static void LoadFile(const char* filePath);
    static void Load(const std::string& config);

public:
    GlobalConfig();

    GLOBAL_CONFIG_FIELDS(GLOBAL_CONFIG_DEFINE_GETTER);
    GLOBAL_CONFIG_FIELDS(GLOBAL_CONFIG_DEFINE_UNSAFE_LOCAL_SETTER);

    static std::string GetConfigurationJson();
};

}
