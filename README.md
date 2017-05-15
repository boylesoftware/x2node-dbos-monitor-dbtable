# X2 Framework for Node.js | DB Table Record Collections Monitor

Record collections monitor implementation for the [x2node-dbos](https://www.npmjs.com/package/x2node-dbos) module that uses a database table to keep track of the record collection updates.

See module's [API Reference Documentation](https://boylesoftware.github.io/x2node-api-reference/module-x2node-dbos-monitor-dbtable.html).

## Usage

To add the monitor to a DBO factory, the following code can be used:

```javascript
const mysql = require('mysql');
const dbos = require('x2node-dbos');
const rcMonitor = require('x2node-dbos-monitor-dbtable');
...

// record types library
const recordTypes = ...

// create the DBO factory
const dboFactory = dbos.createDBOFactory(recordTypes, 'mysql');

// get the standardized data source for the MySQL connection pool
const pool = mysql.createPool(...);
const ds = dboFactory.adaptDataSource(pool);

// add record collections monitor to the DBO factory
rcMonitor.assignTo(dboFactory, ds);
```

The `assignTo()` function exposed by the module takes two arguments: the DBO factory, to which to assign the monitor, and a data source object, which it needs in order to initialize the monitor and make sure that the record collections version info table exists.

The monitor uses the same debug logging section as the DBOs module, so to see what SQL it runs against the database add "X2_DBO" to the `NODE_DEBUG` environment variable (see [Node.js API docs](https://nodejs.org/docs/latest-v4.x/api/util.html#util_util_debuglog_section) for details).

## The Table

When the monitor is initialized, it will try to automatically create the record collections version info table if it does not exists. That often requires certain priviledges for the user used by the application to connect to the database. It may make sense to pre-create the table as a part of the database initialization instead. The table name is `x2rcinfo` and by default it has the following definition for _MySQL_:

```sql
CREATE TABLE x2rcinfo (
    name VARCHAR(64) PRIMARY KEY,
    version INTEGER UNSIGNED NOT NULL,
    modified_on TIMESTAMP(3) DEFAULT 0
)
```

and the following definition for _PostgreSQL_:

```sql
CREATE TABLE x2rcinfo (
    name VARCHAR(64) PRIMARY KEY,
    version INTEGER NOT NULL,
    modified_on TIMESTAMP NOT NULL
)
```

The monitor will automatically insert a row for each record type (with the `name` column used for the record type name) as it encounters them. Once inserted, the row is updated whenever the corresponding records collection gets updated.
