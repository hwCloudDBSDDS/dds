
#include "GlobalConfig.h"
#include <iostream>
#include <fstream>
#include <mongo/bson/json.h>
#include <map>
#include <sstream>

namespace mongo
{

GlobalConfig globalConfig;

GlobalConfig::GlobalConfig()
{
    currentVersion = new GlobalConfigVersion();
    LoadFile(defaultConfigLocation);
}
#define GLOBAL_CONFIG_DEFINE_GETTER_CODE(name, type, defaultValue)   \
const type& GlobalConfig::Get_##name()                        \
{                                                                    \
    return globalConfig.currentVersion->__##name;                    \
}

#define GLOBAL_CONFIG_DEFINE_UNSAFE_LOCAL_SETTER_CODE(name, type, defaultValue)     \
void GlobalConfig::UnsafeLocalSet_##name(type&& value)                       \
{                                                                                   \
    globalConfig.currentVersion->__##name = value;                                  \
}

GLOBAL_CONFIG_FIELDS(GLOBAL_CONFIG_DEFINE_GETTER_CODE)
GLOBAL_CONFIG_FIELDS(GLOBAL_CONFIG_DEFINE_UNSAFE_LOCAL_SETTER_CODE)



#define GLOBAL_CONFIG_DEFINE_LOAD_FIELDS(name, type, defaultValue)      \
    {                                                                   \
        auto it = elements.find(#name);                                 \
        if(it != elements.end())                                        \
        {                                                               \
            BsonParser::GetValue(it->second, currentVersion->__##name); \
        }                                                               \
    }                                                                   \

void GlobalConfig::LoadFile(const char* filePath)
{
    std::ifstream file(filePath);   //  , ios::in|ios::binary|ios::ate);
    if (!file.is_open())
        return;      

    std::string config((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
    globalConfig.LoadText(config);
}

void GlobalConfig::LoadText(const std::string& config)
{
    mongo::BSONObj obj = fromjson(config);

    std::map<std::string, mongo::BSONElement> elements;
    for (BSONElement elem : obj)
    {
        elements[elem.fieldName()] = elem;
    }

    GLOBAL_CONFIG_FIELDS(GLOBAL_CONFIG_DEFINE_LOAD_FIELDS);
}

void GlobalConfig::Load(const std::string& config)
{
    globalConfig.LoadText(config);
}


#define GLOBAL_CONFIG_DEFINE_DUMP_FIELDS(name, type, defaultValue)      \
    {                                                                   \
        ss << #name << ":" << globalConfig.currentVersion->__##name << ";";          \
    }                                                                                \


std::string GlobalConfig::GetConfigurationJson()
{
    std::stringstream ss;
    GLOBAL_CONFIG_FIELDS(GLOBAL_CONFIG_DEFINE_DUMP_FIELDS);
    return ss.str();
}

}
