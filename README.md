# Document Database Service
Document Database Service(aka dds) is an open source project born from HuaweiCloud's MongoDB service.
The codebase is based on MongoDB-4.0.3, the latest 4.0 series which still follows AGPL license.
There are [several enhancements]() which are not supported in MongoDB community version.

# Enhancements 
## AuditLog
As MongoDB Company does not release Auditing functionality in community version, almost all cloud providers have their own solutions for auditing.
dds also provides s set of configurations for auditing. Generally, users can add lines below into mongodb's yaml configuration to enable auditing.
```
auditLog:
  authSuccess: true|false
  format: JSON
  opFilter: off|all|...
  path: /path/to/your/audit_log
```
For the details of configuring auditing parameters, please see [Audit Configuration](https://github.com/hwCloudDBSDDS/dds/wiki/Audit-Configuration)

## QueryStage Memory Limit
Although MongoDB limited the memory used for wiredTiger/rocksDB engine. It sometimes may still OOM, that's because MongoDB's QueryStage does not
have the global memory limit. Let's take an example, one user can use at most [32MB](https://docs.mongodb.com/manual/reference/limits/#Sort-Operations) RAM.
However, if 100 queries run the sort in parallel, that should be 3.2GB. Until Mongo4.0, there is no global limit for this.
We added a suite of parameters to limit query stage memory usage in total. please see [Query Memory limit Params]() for more details.

## InternalPort/ExternalPort
### TODO(cuixin)

## Default admin/rwuser/readonly user and the privilleges
### TODO(gaoqiang)

## Mongos Auto Flush Router
### TODO(danglifei)
