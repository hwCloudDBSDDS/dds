
#include "gc_manager.h"
#include <mongo/bson/json.h>
#include "mongo/unittest/unittest.h"

namespace mongo {

    TEST(GCManagerTest, StartAndStop) {
        GCManager::Get().Start();
        GCManager::Get().Stop();
    }
}
