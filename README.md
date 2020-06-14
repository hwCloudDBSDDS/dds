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
For the details of configuring auditing parameters, please see [Audit Configuration]()

## QueryStage Memory Limit
Although MongoDB limited the memory used for wiredTiger/rocksDB engine. It sometimes may still OOM, that's because MongoDB's QueryStage does not
have the global memory limit. Let's take an example, one user can use at most [32MB](https://docs.mongodb.com/manual/reference/limits/#Sort-Operations) RAM.
However, if 100 queries run the sort in parallel, that should be 3.2GB. Until Mongo4.0, there is no global limit for this.
We added a suite of parameters to limit query stage memory usage in total. please see [Query Memory limit Params]() for more details.

## InternalPort/ExternalPort
### TODO(cuixin)

## Default admin/rwuser/readonly user and the privilleges

There are two kinds of users defined in DDS that are internal users and external users. The rwuser user and the users created by the rwuser user are external users, and other users are internal users. 

### admin

As the administrator with the highest rights, user "admin" is an internal user and is used for internal system management. 

#### What admin can do

* Users can use any commands supported by the system. 

#### What admin can not do

* User admin can not access the database using IP addresses that are not in the admin_white_list.

### rwuser

The "rwuser" user is the highest-level user for customers. It is an external user. All users created by the "rwuser" user are external users too. The "rwuser" user must be added and granted with proper rights as the admin user when the instance is created. 

#### What rwuser can not do

* Some high-risk commands are not open to the rwuser user to prevent risks caused by misoperations. For details, see the command list defined by the global variable globalDisableCommands.

* Similarly, the external user created by "rwuser" cannot use the preceding commands.

#### What rwuser can do

* User rwuser executes all CRUD commands and system administration commands defined when rwuser is created except high-risk commands.

* User rwuser can access the database using IP addresses that are not in the admin_white_list.

### readonlyuser

User "readonlyuser" is not an actual user. It is a kind of user with the "readonlyuser" role, it is both an internal user and an external user. The role of this type of user needs to be added by user "admin" during instance creation and granted with appropriate read permission. By creating this type of user on shard and config nodes, customers can have direct access to shard and config. From this perspective, these users are external users. However, the permissions of these roles cannot be adjusted by external users. Therefore, they are a kind of internal users. 

#### What readonlyuser can not do

* Role readonlyuser can not be managed by external users using the commands like createRole, grantRolesToRole,grantPrivilegesToRole,revokeRolesFromRole,revokePrivilegesFromRole,dropRole.

* Users of readonlyuser  can not be managed by external users  using the commands like createUser,dropUser,updateUser,grantRolesToUser,revokeRolesFromUser.

#### What readonlyuser can do

* Role and users of readonlyuser  can be managed by admin.

* Readonly user can query all databases on which they have permission.  

* User readonlyuser can access the database using IP addresses that are not in the admin_white_list.

## Mongos Auto Flush Router
### TODO(danglifei)
