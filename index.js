/**
 * Implementation of the record collections monitor for the DBOs module that uses
 * a database table to lock collection and store collections version information.
 *
 * @module x2node-dbos-monitor-dbtable
 * @requires module:x2node-common
 */
'use strict';

const common = require('x2node-common');


/**
 * Debug logger.
 *
 * @private
 * @constant {function}
 */
const log = common.getDebugLogger('X2_DBO');

/**
 * Monitor table name.
 *
 * @private
 * @constant {string}
 */
const TABLE = 'x2rcinfo';

/**
 * Record collections monitor table descriptor.
 *
 * @private
 * @constant {Array.<Object>}
 */
const TABLE_DESCS = [{
	tableName: TABLE,
	tableAlias: TABLE
}];

/**
 * Symbol used to store the set of exclusively locked record type names on the
 * transaction.
 *
 * @private
 * @constant {Symbol}
 */
const LOCKED_EXCLUSIVE = Symbol('LOCKED_EXCLUSIVE');

/**
 * Symbol used to store the set of record type names locked for share on the
 * transaction.
 *
 * @private
 * @constant {Symbol}
 */
const LOCKED_SHARED = Symbol('LOCKED_SHARED');


/**
 * The DB table record collections monitor implementation.
 *
 * @private
 * @inner
 * @implements {module:x2node-dbos.RecordCollectionsMonitor}
 */
class DBTableRecordCollectionsMonitor {

	/**
	 * Create new unitnialized monitor instance.
	 *
	 * @param {Promise} initCompletePromise Initialization completion promise.
	 */
	constructor(initCompletePromise) {

		this._init = initCompletePromise;
	}


	/**
	 * Get promise that resolves upon monitor initialization completion.
	 *
	 * @returns {Promise} Initialization completion promise. If there was an
	 * error and the monitor could not be initialized, the promise is rejected
	 * with the error.
	 */
	waitForInitComplete() {

		return this._init;
	}

	// lock record collections and get version info
	lockCollections(tx, locks, includeInVersion) {

		// create aggregate version information descriptor
		const versionInfo = {
			modifiedOn: new Date(0),
			version: 0
		};

		// versioned names set
		const versionedNames = new Set(includeInVersion);
		if (versionedNames.size === 0)
			versionedNames.has = function() { return true; };

		// function to use to process and aggregate a version row
		function processVersionRow(row) {
			let name, modifiedOn, version;
			if (Array.isArray(row)) {
				name = row[0];
				modifiedOn = row[1];
				version = Number(row[2]);
			} else {
				name = row.name;
				modifiedOn = row.modified_on;
				version = Number(row.version);
			}
			if (versionedNames.has(name)) {
				if (modifiedOn.getTime() > versionInfo.modifiedOn.getTime())
					versionInfo.modifiedOn = modifiedOn;
				versionInfo.version += version;
			}
		}

		// start queries promise chain with monitor initialization
		let chain = this._init;

		// add exclusive locks
		const exclusiveNames = new Set(locks.exclusive);
		if (exclusiveNames.size > 0)
			chain = chain.then(() => new Promise((resolve, reject) => {
				try {
					const sql = tx.dbDriver.makeSelectWithLocks(
						'SELECT name, modified_on, version' +
							` FROM ${TABLE} WHERE ` +
							this._createFilterExpr(
								tx, Array.from(exclusiveNames)),
						TABLE_DESCS, null
					);
					log(`(tx #${tx.id}) executing SQL: ${sql}`);
					tx.dbDriver.executeQuery(tx.connection, sql, {
						onRow(row) {
							processVersionRow(row);
						},
						onSuccess() {
							if (tx[LOCKED_EXCLUSIVE])
								for (let name of tx[LOCKED_EXCLUSIVE])
									exclusiveNames.add(name);
							tx[LOCKED_EXCLUSIVE] = exclusiveNames;
							resolve();
						},
						onError(err) {
							common.error(`error executing SQL [${sql}]`, err);
							reject(err);
						}
					});
				} catch (err) {
					common.error(
						'error querying record collections monitor table', err);
					reject(err);
				}
			}));

		// add shared locks
		const sharedNames = new Set(locks.shared);
		if (sharedNames.size > 0)
			chain = chain.then(() => new Promise((resolve, reject) => {
				try {
					const sql = tx.dbDriver.makeSelectWithLocks(
						'SELECT name, modified_on, version' +
							` FROM ${TABLE} WHERE ` +
							this._createFilterExpr(
								tx, Array.from(sharedNames)),
						null, TABLE_DESCS
					);
					log(`(tx #${tx.id}) executing SQL: ${sql}`);
					tx.dbDriver.executeQuery(tx.connection, sql, {
						onRow(row) {
							processVersionRow(row);
						},
						onSuccess() {
							if (tx[LOCKED_SHARED])
								for (let name of tx[LOCKED_SHARED])
									sharedNames.add(name);
							tx[LOCKED_SHARED] = sharedNames;
							resolve();
						},
						onError(err) {
							common.error(`error executing SQL [${sql}]`, err);
							reject(err);
						}
					});
				} catch (err) {
					common.error(
						'error querying record collections monitor table', err);
					reject(err);
				}
			}));

		// return version info promise
		return chain.then(() => versionInfo);
	}

	// add shared lock for record type
	lockCollectionForShare(tx, recordTypeName) {

		// check if already locked
		const sharedNames = tx[LOCKED_SHARED];
		const exclusiveNames = tx[LOCKED_EXCLUSIVE];
		if ((sharedNames && sharedNames.has(recordTypeName)) ||
			(exclusiveNames && exclusiveNames.has(recordTypeName)))
			return Promise.resolve();

		// perform the lock
		return this._init.then(() => new Promise((resolve, reject) => {
			try {
				const sql = tx.dbDriver.makeSelectWithLocks(
					`SELECT name FROM ${TABLE} WHERE ` +
						this._createFilterExpr(tx, [ recordTypeName ]),
					null, TABLE_DESCS
				);
				log(`(tx #${tx.id}) executing SQL: ${sql}`);
				tx.dbDriver.executeQuery(tx.connection, sql, {
					onSuccess() {
						if (tx[LOCKED_SHARED])
							tx[LOCKED_SHARED].add(recordTypeName);
						else
							tx[LOCKED_SHARED] = new Set([ recordTypeName ]);
						resolve();
					},
					onError(err) {
						common.error(`error executing SQL [${sql}]`, err);
						reject(err);
					}
				});
			} catch (err) {
				common.error(
					'error querying record collections monitor table', err);
				reject(err);
			}
		}));
	}

	// process record collections update
	collectionsUpdated(ctx, recordTypeNames) {

		// check if anything was updated
		const namesList = Array.from(recordTypeNames || []);
		if (namesList.length === 0)
			return;

		// make sure the collection is exclusively locked
		const lockedNames = ctx.transaction[LOCKED_EXCLUSIVE];
		if (!lockedNames || namesList.some(name => !lockedNames.has(name)))
			throw new common.X2UsageError(
				'Some of the updated record collections were not exclusively' +
					' locked for the transaction.');

		// update the table
		return this._init.then(() => new Promise((resolve, reject) => {
			try {
				let lastSql;
				ctx.dbDriver.updateVersionTable(
					ctx.connection, TABLE, namesList,
					ctx.executedOn.toISOString(), {
						trace(sql) {
							lastSql = sql;
							ctx.log(
								`(tx #${ctx.transaction.id}) executing SQL: ` +
									sql);
						},
						onSuccess() {
							resolve();
						},
						onError(err) {
							common.error(
								`error executing SQL [${lastSql}]`, err);
							reject(err);
						}
					}
				);
			} catch (err) {
				common.error(
					'error updating record collections monitor table', err);
				reject(err);
			}
		}));
	}

	/*
	// query record collections version
	getCollectionsVersion(tx, recordTypeNames, lockType) {

		// query the table
		return this._init.then(() => new Promise((resolve, reject) => {
			try {
				const filterExpr = this._createFilterExpr(tx, recordTypeNames);
				let sql;
				if (lockType && !tx.dbDriver.supportsRowLocksWithAggregates()) {
					sql = `SELECT modified_on, version FROM ${TABLE} WHERE ` +
						filterExpr;
					switch (lockType) {
					case 'shared':
						sql = tx.dbDriver.makeSelectWithLocks(
							sql, null, TABLE_DESCS);
						break;
					case 'exclusive':
						sql = tx.dbDriver.makeSelectWithLocks(
							sql, TABLE_DESCS, null);
					}
					sql = 'SELECT' +
						' MAX(t.modified_on) AS modifiedOn,' +
						' SUM(t.version) AS version' +
						' FROM (' + sql + ') AS t';
				} else {
					sql = 'SELECT' +
						' MAX(modified_on) AS modifiedOn,' +
						` SUM(version) AS version FROM ${TABLE}` +
						' WHERE ' + filterExpr;
					switch (lockType) {
					case 'shared':
						sql = tx.dbDriver.makeSelectWithLocks(
							sql, null, TABLE_DESCS);
						break;
					case 'exclusive':
						sql = tx.dbDriver.makeSelectWithLocks(
							sql, TABLE_DESCS, null);
					}
				}
				log(`(tx #${tx.id}) executing SQL: ${sql}`);
				let res;
				tx.dbDriver.executeQuery(tx.connection, sql, {
					onRow(row) {
						if (Array.isArray(row))
							res = {
								modifiedOn: row[0],
								version: Number(row[1])
							};
						else
							res = {
								modifiedOn: row.modifiedOn,
								version: Number(row.version)
							};
					},
					onSuccess() {
						if (!res)
							res = new Object();
						if (!(res.modifiedOn instanceof Date)) {
							res.modifiedOn = new Date();
							res.modifiedOn.setTime(0);
						}
						if (!Number.isFinite(res.version))
							res.version = 0;
						resolve(res);
					},
					onError(err) {
						common.error(`error executing SQL [${sql}]`, err);
						reject(err);
					}
				});
			} catch (err) {
				common.error(
					'error querying record collection version info table', err);
				reject(err);
			}
		}));
	}

	// lock record collections
	lockCollections(tx, recordTypeNames, lockType) {

		// place the lock on the table
		return this._init.then(() => new Promise((resolve, reject) => {
			try {
				let sql = `SELECT name FROM ${TABLE} WHERE ` +
					this._createFilterExpr(tx, recordTypeNames);
				switch (lockType) {
				case 'shared':
					sql = tx.dbDriver.makeSelectWithLocks(
						sql, null, TABLE_DESCS);
					break;
				case 'exclusive':
					sql = tx.dbDriver.makeSelectWithLocks(
						sql, TABLE_DESCS, null);
				}
				log(`(tx #${tx.id}) executing SQL: ${sql}`);
				tx.dbDriver.executeQuery(tx.connection, sql, {
					onSuccess() {
						resolve();
					},
					onError(err) {
						common.error(`error executing SQL [${sql}]`, err);
						reject(err);
					}
				});
			} catch (err) {
				common.error(
					'error querying record collection version info table', err);
				reject(err);
			}
		}));
	}
	*/

	/**
	 * Create collection name SQL filter expression for the specified record
	 * types.
	 *
	 * @private
	 * @param {module:x2node-dbos~Transaction} tx The transaction handler.
	 * @param {Array.<string>} namesList Record type names. Must not be empty.
	 * @returns {string} SQL expression for the <code>WHERE</code> clause.
	 */
	_createFilterExpr(tx, namesList) {

		if (namesList.length === 0)
			throw new common.X2UsageError(
				'Record type names must be a non-empty iterable.');

		if (namesList.length === 1)
			return 'name = ' + tx.dbDriver.stringLiteral(namesList[0]);

		return 'name IN (' + namesList.map(
			v => tx.dbDriver.stringLiteral(v)).join(', ') + ')';
	}
}


/**
 * Assign the monitor to the specified DBO factory and initialize it.
 *
 * @param {module:x2node-dbos~DBOFactory} dboFactory The DBO factory, to which to
 * to assign the monitor.
 * @param {module:x2node-dbos.DataSource} ds Database connection data source used
 * by the monitor to initialize the record collections version information table.
 * @returns {module:x2node-dbos.RecordCollectionsMonitor} The monitor assigned to
 * the DBO factory.
 */
exports.assignTo = function(dboFactory, ds) {

	// make sure we have the table
	const initCompletePromise = ds.getConnection(
	).then(
		con => new Promise((resolve, reject) => {
			try {
				let lastSql;
				dboFactory.dbDriver.createVersionTableIfNotExists(
					con, TABLE, dboFactory.recordTypes.definedRecordTypeNames,
					{
						trace(sql) {
							lastSql = sql;
							log(`executing SQL: ${sql}`);
						},
						onSuccess() {
							ds.releaseConnection(con);
							resolve();
						},
						onError(err) {
							common.error(
								`error executing SQL [${lastSql}]`, err);
							ds.releaseConnection(con, err);
							reject(err);
						}
					}
				);
			} catch (err) {
				common.error(
					'error creating record collections version info table', err);
				ds.releaseConnection(con);
				reject(err);
			}
		}),
		err => {
			common.error('error acquiring DB connection', err);
			return Promise.reject(err);
		}
	);

	// create the monitor
	const monitor = new DBTableRecordCollectionsMonitor(initCompletePromise);

	// add monitor to the DBO factory
	dboFactory.setRecordCollectionsMonitor(monitor);

	// return it
	return monitor;
};
