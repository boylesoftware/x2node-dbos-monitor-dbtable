/**
 * Implementation of the record collections monitor for the DBOs module that uses
 * a database table to store current record collections version information.
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
 * Version info table name.
 *
 * @private
 * @constant {string}
 */
const TABLE = 'x2rcinfo';

/**
 * Version info table descriptor.
 *
 * @private
 * @constant {Array.<Object>}
 */
const TABLE_DESCS = [{
	tableName: TABLE,
	tableAlias: TABLE
}];


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

	// process record collections update
	collectionsUpdated(ctx, recordTypeNames) {

		// check if anything was updated
		if (!recordTypeNames ||
			(Array.isArray(recordTypeNames) && (recordTypeNames.length === 0)) ||
			(recordTypeNames.size === 0))
			return;

		// update the table
		return this._init.then(() => new Promise((resolve, reject) => {
			try {
				let lastSql;
				ctx.dbDriver.updateVersionTable(
					ctx.connection, TABLE, Array.from(recordTypeNames),
					ctx.executedOn.toISOString(), {
						trace(sql) {
							lastSql = sql;
							ctx.log('executing SQL: ' + sql);
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
					'error updating record collection version info table', err);
				reject(err);
			}
		}));
	}

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

	/**
	 * Create collection name SQL filter expression for the specified record
	 * types.
	 *
	 * @private
	 * @param {module:x2node-dbos~Transaction} tx The transaction handler.
	 * @param {(string|Array.<string>|Iterable.<string>)} recordTypeNames Record
	 * type names.
	 * @returns {string} SQL expression for the <code>WHERE</code> clause.
	 */
	_createFilterExpr(tx, recordTypeNames) {

		let res;
		if (Array.isArray(recordTypeNames))
			res = recordTypeNames;
		else if ((typeof recordTypeNames) === 'string')
			res = [ recordTypeNames ];
		else if (recordTypeNames &&
			((typeof recordTypeNames[Symbol.iterator]) === 'function'))
			res = Array.from(recordTypeNames);

		if (!res || (res.length === 0))
			throw new common.X2UsageError(
				'Record type names must be a non-empty iterable' +
					' or a single string.');

		if (res.length === 1)
			return 'name = ' + tx.dbDriver.stringLiteral(res[0]);

		return 'name IN (' + res.map(
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
					con, TABLE, {
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
				ds.releaseConnection(con, err);
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
