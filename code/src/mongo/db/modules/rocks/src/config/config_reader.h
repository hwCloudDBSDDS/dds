#ifndef CONFIG_READER_WRITE_H
#define CONFIG_READER_WRITE_H
#include <fstream>
#include <sstream>
#include <map>
#include <cstdlib>
#include <string.h>
#include <mutex>

using namespace std;


typedef struct tag_ConfigKey
{
    string sectionName;
    string key;
    tag_ConfigKey(const string & inSectionName, const string & inKeyName)
        :sectionName(inSectionName),
        key(inKeyName)
    {}

    bool operator < (const tag_ConfigKey & other) const
    {
        if (sectionName != other.sectionName)
        {
            return sectionName < other.sectionName;
        }
        return key < other.key;
    }
}ConfigKey;


class ConfigReader
{
public:
    static ConfigReader* getInstance();
    string getString(const string& sectionName, const string& key);
    int getInt(const string& sectionName, const string& key);
    ~ConfigReader(){}

private:
    ConfigReader();
    void init();
    int read_ini();
    void putStringConfigInfo(const string& sectionName, const string& key, const string& value);
private:
    static ConfigReader* m_pInstance;
    map<ConfigKey, string> m_configMap;
    static std::mutex   lock_;

    //GC is not necessary
    class GC
    {
    public:
        ~GC()
        {
            if(NULL != m_pInstance)
            {
                delete m_pInstance;
                m_pInstance = NULL;
            }
        }
    };
    static GC gc;
};

#endif
