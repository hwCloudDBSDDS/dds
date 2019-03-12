
#include "mongo/unittest/unittest.h"
#include "gc_manager.h"
#include <mongo/bson/json.h>


namespace mongo
{


TEST(GCManagerTest, StartAndStop)
{
    GCManager::Get().Start();
    GCManager::Get().Stop();

}

}
