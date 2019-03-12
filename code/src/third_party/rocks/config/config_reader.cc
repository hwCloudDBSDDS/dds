#include "config_reader.h"
#include <sstream>

namespace rocksdb{

//remove all blank space
string &TrimString(string &str)
{
    string::size_type pos = 0;
    while(str.npos != (pos = str.find(" ")))
        str = str.replace(pos, pos+1, "");
    return str;
}

std::mutex ConfigReader::lock_;
ConfigReader* ConfigReader::m_pInstance = NULL;
ConfigReader::GC ConfigReader::gc;

ConfigReader::ConfigReader()
{
    m_configMap.clear();
    init();
}

ConfigReader* ConfigReader::getInstance()
{
    if(m_pInstance == NULL)
    {
        std::unique_lock<std::mutex> lock(lock_);
        m_pInstance = new ConfigReader;
        if(m_pInstance == NULL) {return NULL;}
    }

    return m_pInstance;
}

string ConfigReader::getString(const string& sectionName, const string& key)
{
    if(m_configMap.empty())
    {
        init();
    }
    ConfigKey ckey(sectionName, key);
    map<ConfigKey, string>::iterator it = m_configMap.find(ckey);
    if(it != m_configMap.end())
    {
        return it->second;
    }
    return "";
}

int ConfigReader::getInt(const string& sectionName, const string& key)
{
    if(m_configMap.empty())
    {
        init();
    }
    ConfigKey ckey(sectionName, key);
    map<ConfigKey, string>::iterator it = m_configMap.find(ckey);
    if(it != m_configMap.end())
    {
        string value = it->second;
        stringstream ss;
        ss<<value;
        int i;
        ss>>i;
        return i;
    }
    return 0;
}

void ConfigReader::init()
{
    int ret = read_ini();
    if(-1 == ret)
    {
        //TODO: print error log
    }
}

int ConfigReader::read_ini()
{
    //if we use the MACRO as the parameter to ifstream, UT binary can not recognize the path, I do not know why, here use the path
    ifstream in_file("/opt/stream/config/rocks_conf.ini");
    if(!in_file)
    {
        return -1;
    }

    string line = "";
    string root = "";
    while (getline(in_file, line))
    {
        string::size_type left = 0;
        string::size_type right = 0;
        string::size_type div = 0;
        string key = "";
        string value = "";
        if((line.npos != (left = line.find("["))) && (line.npos != (right = line.find("]"))))
        {
            root = line.substr(left+1, right-1);
        }

        if(line.npos != (div = line.find("=")))
        {
            key = line.substr(0, div);
            value = line.substr(div+1, line.size()-1);
            key = TrimString(key);
            value = TrimString(value);
        }

        if (!root.empty() && !key.empty() && !value.empty())
        {
            ConfigKey ck(root, key);
            m_configMap[ck] = value;
        }
    }

    in_file.close();
    in_file.clear();

    return 0;
}

void ConfigReader::putStringConfigInfo(const string& sectionName, const string& key, const string& value)
{
    ConfigKey ckey(sectionName, key);
    m_configMap[ckey] = value;
}
}
