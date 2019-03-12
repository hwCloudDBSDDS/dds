#include "config_reader.h"
#include <sstream>

const string INIPATH = "/var/mongodb/rocksenginecfg.ini";

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
    putStringConfigInfo("NameNode", "ip", "127.0.0.1");
    putStringConfigInfo("NameNode", "port", "8080");
    putStringConfigInfo("NameNode", "nameservice", "nameservice");
    int ret = read_ini();
    if(-1 == ret)
    {
        //TODO: print error log
    }
}

bool isFirstCharPound(string str)
{
    string tmp = TrimString(str);

    if (tmp[0] == '#')
    {
        return true;
    }

    return false;
}

int ConfigReader::read_ini()
{
    ifstream in_file(INIPATH.c_str());
    if(!in_file)
    {
        return -1;
    }

    string line = "";
    string root = "";
    while (getline(in_file, line))
    {
        if (isFirstCharPound(line))
        {
            continue;
        }

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
