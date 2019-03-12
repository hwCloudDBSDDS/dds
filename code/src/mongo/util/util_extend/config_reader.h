#ifndef CONFIG_READER_WRITE_H
#define CONFIG_READER_WRITE_H
#include <cstdlib>
#include <fstream>
#include <map>
#include <mutex>
#include <sstream>
#include <string.h>

using namespace std;


typedef struct tag_ConfigKey {
    string sectionName;
    string key;
    tag_ConfigKey(const string& inSectionName, const string& inKeyName)
        : sectionName(inSectionName), key(inKeyName) {}

    bool operator<(const tag_ConfigKey& other) const {
        if (sectionName != other.sectionName) {
            return sectionName < other.sectionName;
        }
        return key < other.key;
    }
} ConfigKey;


class ConfigReader {
public:
    static ConfigReader* getInstance();
    string getString(const string& sectionName, const string& key);
    template <class T>
    T getDecimalNumber(const string& sectionName, const string& key) {
        ConfigKey ckey(sectionName, key);
        map<ConfigKey, string>::iterator it = m_configMap.find(ckey);
        T i = T();
        if (it != m_configMap.end()) {
            string value = it->second;
            stringstream ss;
            ss << value;
            ss >> i;
        }
        return i;
    }

    const std::map<ConfigKey, string>& getConfigMap() const{
        return m_configMap;
    }

    ~ConfigReader() {}

private:
    ConfigReader();
    void init();
    int read_ini();
    void putStringConfigInfo(const string& sectionName, const string& key, const string& value);

private:
    static ConfigReader* m_pInstance;
    map<ConfigKey, string> m_configMap;
    static std::mutex lock_;

    // GC is not necessary
    class GC {
    public:
        ~GC() {
            if (NULL != m_pInstance) {
                delete m_pInstance;
                m_pInstance = NULL;
            }
        }
    };
    static GC gc;
};

#endif
