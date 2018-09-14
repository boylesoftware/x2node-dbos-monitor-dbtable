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
 * Record collections monitor table name.
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
 * Symbol used to store a set of record collection names locked in share mode on
 * a transaction.
 *
 * @private
 * @constant {Symbol}
 */
const LOCKED_SHARED = Symbol('LOCKED_SHARED');

/**
 * Symbol used to store a set of record collection names locked in exclusive mode
 * on a transaction.
 *
 * @private
 * @constant {Symbol}
 */
const LOCKED_EXCLUSIVE = Symbol('LOCKED_EXCLUSIVE');


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
					ctx.executedOn, {
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

	// query record collections version
	getCollectionsVersion(tx, recordTypeNames, lockType) {

		// create aggregate version information descriptor
		const versionInfo = {
			modifiedOn: new Date(0),
			version: 0
		};

		// function to use to process and aggregate a version row
		function processVersionRow(row) {
			let modifiedOn, version;
			if (Array.isArray(row)) {
				modifiedOn = row[1];
				version = Number(row[2]);
			} else {
				modifiedOn = row.modified_on;
				version = Number(row.version);
			}
			if (modifiedOn.getTime() > versionInfo.modifiedOn.getTime())
				versionInfo.modifiedOn = modifiedOn;
			versionInfo.version += version;
		}

		// query the table
		return this._init.then(() => new Promise((resolve, reject) => {
			try {

				const filterExpr = this._createFilterExpr(tx, recordTypeNames);

				let sql = 'SELECT name, modified_on, version' +
					` FROM ${TABLE} WHERE ` + filterExpr;

				switch (lockType) {
				case 'shared':
					sql = tx.dbDriver.makeSelectWithLocks(
						sql, null, TABLE_DESCS);
					break;
				case 'exclusive':
					sql = tx.dbDriver.makeSelectWithLocks(
						sql, TABLE_DESCS, null);
				}

				const lockedCollections = this._getTxLockedCollections(
					tx, lockType);

				log(`(tx #${tx.id}) executing SQL: ${sql}`);
				tx.dbDriver.executeQuery(tx.connection, sql, {
					onRow(row) {
						processVersionRow(row);
					},
					onSuccess() {
						if (lockedCollections)
							for (let n of recordTypeNames)
								lockedCollections.add(n);
						resolve(versionInfo);
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

	// lock record collections
	lockCollections(tx, recordTypeNames, lockType) {

		// place the lock on the table
		return this._init.then(() => new Promise((resolve, reject) => {
			try {

				const lockedCollectionsShared = this._getTxLockedCollections(
					tx, 'shared');
				const lockedCollectionsExclusive = this._getTxLockedCollections(
					tx, 'exclusive');
				let lockedCollections, allLockedCollections;
				switch (lockType) {
				case 'shared':
					lockedCollections = lockedCollectionsShared;
					allLockedCollections = [
						lockedCollectionsShared, lockedCollectionsExclusive
					];
					break;
				case 'exclusive':
					lockedCollections = lockedCollectionsExclusive;
					allLockedCollections = [
						lockedCollectionsExclusive
					];
				}

				const namesToLock = new Set(recordTypeNames);
				for (let name of recordTypeNames)
					if (allLockedCollections.some(names => names.has(name)))
						namesToLock.delete(name);
				if (namesToLock.size === 0)
					return resolve();

				let sql = `SELECT name FROM ${TABLE} WHERE ` +
					this._createFilterExpr(tx, namesToLock);
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
						for (let n of namesToLock)
							lockedCollections.add(n);
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

	/**
	 * Get set of locked collections for the transaction and the lock type.
	 *
	 * @private
	 * @param {module:x2node-dbos~Transaction} tx The transaction handler.
	 * @param {string} lockType Lock type, "shared" or "exclusive".
	 * @returns {Set.<string>} The locked collection names. Returns
	 * <code>undefined</code> if the <code>lockType</code> argument anything else
	 * than a string "shared" or "exclusive".
	 */
	_getTxLockedCollections(tx, lockType) {

		let lockedCollectionsSymbol;
		switch (lockType) {
		case 'shared':
			lockedCollectionsSymbol = LOCKED_SHARED;
			break;
		case 'exclusive':
			lockedCollectionsSymbol = LOCKED_EXCLUSIVE;
		}

		let lockedCollections;
		if (lockedCollectionsSymbol) {
			lockedCollections = tx[lockedCollectionsSymbol];
			if (!lockedCollections)
				lockedCollections = tx[lockedCollectionsSymbol] = new Set();
		}

		return lockedCollections;
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
					'error initializing record collections monitor table', err);
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
