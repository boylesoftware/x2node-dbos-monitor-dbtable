/**
 * Implementation of the record collections monitor for the DBOs module that uses
 * a database table to store current record collections version information.
 *
 * @module x2node-dbos-monitor-dbtable
 * @requires module:x2node-common
 */
'use strict';

const EventEmitter = require('events');
const common = require('x2node-common');


/**
 * Debug logger.
 *
 * @private
 * @constant {function}
 */
const log = common.getDebugLogger('X2_DBO');


/**
 * Monitor initialization completion event.
 *
 * @private
 * @event module:x2node-dbos-monitor-dbtable~DBTableRecordCollectionsMonitor#ready
 * @type {string}
 */

/**
 * The DB table record collections monitor implementation.
 *
 * @private
 * @inner
 * @extends external:EventEmitter
 * @implements {module:x2node-dbos.RecordCollectionsMonitor}
 * @fires module:x2node-dbos-monitor-dbtable~DBTableRecordCollectionsMonitor#ready
 */
class DBTableRecordCollectionsMonitor extends EventEmitter {

	/**
	 * Create new unitnialized monitor instance.
	 */
	constructor() {
		super();

		this._ready = false;
		this._initError = null;
	}

	/**
	 * Called when the monitor initialization is complete (the DB table is in the
	 * database and is ready to be used).
	 *
	 * @param {external:Error} [err] Error, if could not be initialized.
	 */
	initComplete(err) {

		if (err)
			this._initError = err;
		else
			this._ready = true;

		this.emit('ready');
	}

	// process record collections update
	collectionsUpdated(ctx, recordTypeNames) {

		// check if initialization error
		if (this._initError)
			return Promise.reject(this._initError);

		// check if anything was updated
		if (!recordTypeNames || (recordTypeNames.size === 0))
			return;

		// check if not initialized yet
		if (!this._ready) {
			const monitor = this;
			return new Promise(resolve => {
				monitor.on('ready', () => {
					resolve(monitor.collectionsUpdated(ctx, recordTypeNames));
				});
			});
		}

		// update the table
		return new Promise((resolve, reject) => {
			try {
				let lastSql;
				ctx.dbDriver.updateVersionTable(
					ctx.connection, 'x2rcinfo', Array.from(recordTypeNames),
					ctx.executedOn.toISOString(), {
						trace(sql) {
							lastSql = sql;
							ctx.log(`executing SQL: ${sql}`);
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
		});
	}

	// query record collections version
	getCollectionsVersion(tx, recordTypeNames) {

		// check if initialization error
		if (this._initError)
			return Promise.reject(this._initError);

		// check if not initialized yet
		if (!this._ready) {
			const monitor = this;
			return new Promise(resolve => {
				monitor.on('ready', () => {
					resolve(monitor.getCollectionsVersion(tx, recordTypeNames));
				});
			});
		}

		// query the table
		return new Promise((resolve, reject) => {
			try {
				const sql =
					'SELECT MAX(modified_on) AS modifiedOn,' +
					' SUM(version) AS version FROM x2rcinfo' +
					' WHERE name' + (
						recordTypeNames.size === 1 ?
							' = ' + tx.dbDriver.stringLiteral(
								recordTypeNames.values().next().value) :
							' IN (' + Array.from(recordTypeNames).map(
								v => tx.dbDriver.stringLiteral(v)).join(', ') +
							')'
					);
				log(`executing SQL: ${sql}`);
				let res;
				tx.dbDriver.executeQuery(tx.connection, sql, {
					onRow(row) {
						if (Array.isArray(row))
							res = {
								modifiedOn: row[0],
								version: row[1]
							};
						else
							res = {
								modifiedOn: row.modifiedOn,
								version: row.version
							};
					},
					onSuccess() {
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
		});
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
exports.addTo = function(dboFactory, ds) {

	// create the monitor
	const monitor = new DBTableRecordCollectionsMonitor();

	// make sure we have the table
	ds.getConnection().then(
		connection => {
			try {
				let lastSql;
				dboFactory.dbDriver.createVersionTableIfNotExists(
					connection, 'x2rcinfo', {
						trace(sql) {
							lastSql = sql;
							log(`executing SQL: ${sql}`);
						},
						onSuccess() {
							ds.releaseConnection(connection);
							monitor.initComplete();
						},
						onError(err) {
							ds.releaseConnection(connection);
							common.error(
								`error executing SQL [${lastSql}]`, err);
							monitor.initComplete(err);
						}
					});
			} catch (err) {
				ds.releaseConnection(connection);
				common.error(
					'error creating record collections version info table', err);
				monitor.initComplete(err);
			}
		},
		err => {
			common.error('error acquiring DB connection', err);
			monitor.initComplete(err);
		}
	);

	// add monitor to the DBO factory
	dboFactory.setRecordCollectionsMonitor(monitor);

	// return it
	return monitor;
};
