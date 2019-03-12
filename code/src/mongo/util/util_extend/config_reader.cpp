#include "config_reader.h"
#include <sstream>

// remove all blank space
string& TrimString(string& str) {
    string::size_type pos = 0;
    while (str.npos != (pos = str.find(" ")))
        str = str.replace(pos, pos + 1, "");
    return str;
}

std::mutex ConfigReader::lock_;
ConfigReader* ConfigReader::m_pInstance = NULL;
ConfigReader::GC ConfigReader::gc;

ConfigReader::ConfigReader() {
    m_configMap.clear();
    init();
}

ConfigReader* ConfigReader::getInstance() {
    if (m_pInstance == NULL) {
        std::unique_lock<std::mutex> lock(lock_);
        m_pInstance = new ConfigReader;
    }

    return m_pInstance;
}

string ConfigReader::getString(const string& sectionName, const string& key) {
    ConfigKey ckey(sectionName, key);
    map<ConfigKey, string>::iterator it = m_configMap.find(ckey);
    string s = "";
    if (it != m_configMap.end()) {
        s = it->second;
    }
    return s;
}


void ConfigReader::init() {
    putStringConfigInfo("DBOptions", "max_background_compactions", "8");
    putStringConfigInfo("DBOptions", "base_background_compactions", "4");
    putStringConfigInfo("DBOptions", "max_background_flushes", "4");

    putStringConfigInfo("CFOptions", "row_cache_size", "104857600");
    putStringConfigInfo("CFOptions", "enable_row_cache", "1");
    putStringConfigInfo("CFOptions", "index_write_buffer_size", "8388608");
    putStringConfigInfo("CFOptions", "index_max_write_buffer_number", "4");
    putStringConfigInfo("CFOptions", "data_write_buffer_size", "16777216");
    putStringConfigInfo("CFOptions", "data_max_write_buffer_number", "4");

    putStringConfigInfo("TableOptions", "block_cache", "838860800");
    putStringConfigInfo("TableOptions", "db_write_buffer_size", "314572800");
    putStringConfigInfo("TableOptions", "enable_persistent_cache", "0");
    putStringConfigInfo("TableOptions", "persistent_cache_path", "/mnt/cache");
    putStringConfigInfo("TableOptions", "persistent_cache_size", "1073741824");

    putStringConfigInfo("PublicOptions", "rocksdb_log_level", "WARN_LEVEL");
    putStringConfigInfo("PublicOptions", "max_chunk_count_in_one_shard", "50");
    putStringConfigInfo("PublicOptions", "max_init_cks_num_one_coll_per_ss", "50");
    putStringConfigInfo("PublicOptions", "move_chunk_interval", "10");
    putStringConfigInfo("PublicOptions", "flush_threads_num", "1");
    putStringConfigInfo("PublicOptions", "flush_memtable_interval", "60");
    putStringConfigInfo("PublicOptions", "open_ftds", "0");
    putStringConfigInfo("PublicOptions", "max_concurrent_find_num", "1");
    putStringConfigInfo("PublicOptions", "max_chunk_size_lt4", "200");
    putStringConfigInfo("PublicOptions", "max_chunk_size_lt8", "500");
    putStringConfigInfo("PublicOptions", "max_chunk_size_lt16", "2048");
    putStringConfigInfo("PublicOptions", "max_chunk_size_gt16", "10240");

    int ret = read_ini();
    if (-1 == ret) {
        // TODO: print error log
    }
}

bool isFirstCharPound(string str) {
    string tmp = TrimString(str);

    if (tmp[0] == '#') {
        return true;
    }

    return false;
}

int ConfigReader::read_ini() {
    ifstream in_file("/opt/stream/config/rocksenginecfg.ini");
    if (!in_file) {
        return -1;
    }

    string line = "";
    string root = "";
    while (getline(in_file, line)) {
        if (isFirstCharPound(line)) {
            continue;
        }

        string::size_type left = 0;
        string::size_type right = 0;
        string::size_type div = 0;
        string key = "";
        string value = "";
        if ((line.npos != (left = line.find("["))) && (line.npos != (right = line.find("]")))) {
            root = line.substr(left + 1, right - 1);
        }

        if (line.npos != (div = line.find("="))) {
            key = line.substr(0, div);
            value = line.substr(div + 1, line.size() - 1);
            key = TrimString(key);
            value = TrimString(value);
        }

        if (!root.empty() && !key.empty() && !value.empty()) {
            ConfigKey ck(root, key);
            m_configMap[ck] = value;
        }
    }

    in_file.close();
    in_file.clear();

    return 0;
}

void ConfigReader::putStringConfigInfo(const string& sectionName,
                                       const string& key,
                                       const string& value) {
    ConfigKey ckey(sectionName, key);
    m_configMap[ckey] = value;
}
