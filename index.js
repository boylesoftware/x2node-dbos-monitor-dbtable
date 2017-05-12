/**
 * Implementation of the record collections version tracker for the DBOs module
 * that uses a database table to store current record collections version
 * information.
 *
 * @module x2node-dbos-rctrack-dbtable
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
 * Tracker initialization completion event.
 *
 * @private
 * @event module:x2node-dbos-rctrack-dbtable~DBTableRecordCollectionsTracker#ready
 * @type {string}
 */

/**
 * The DB table record collections version tracker implementation.
 *
 * @private
 * @inner
 * @extends external:EventEmitter
 * @fires module:x2node-dbos-rctrack-dbtable~DBTableRecordCollectionsTracker#ready
 */
class DBTableRecordCollectionsTracker extends EventEmitter {

	/**
	 * Create new unitnialized tracker instance.
	 */
	constructor() {
		super();

		this._ready = false;
		this._initError = null;
	}

	/**
	 * Called when the tracker initialization is complete (the DB table is in the
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
	recordsUpdated(ctx, recordTypeNames) {

		// check if initialization error
		if (this._initError)
			return Promise.reject(this._initError);

		// check if anything was updated
		if (!recordTypeNames || (recordTypeNames.size === 0))
			return;

		// check if not initialized yet
		if (!this._ready) {
			const tracker = this;
			return new Promise(resolve => {
				tracker.on('ready', () => {
					resolve(tracker.recordsUpdated(ctx, recordTypeNames));
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
}


/**
 * Add the tracker to the specified DBO factory and initialize it.
 *
 * @param {module:x2node-dbos~DBOFactory} dboFactory The DBO factory, to which to
 * to add the tracker.
 * @param {module:x2node-dbos.DataSource} ds Database connection data source used
 * by the tracker to initialize the record collections version information table.
 * @returns {module:x2node-dbos.RecordCollectionsTracker} The tracker added to
 * the DBO factory.
 */
exports.addTo = function(dboFactory, ds) {

	// create the tracker
	const tracker = new DBTableRecordCollectionsTracker();

	// make sure we have the table
	ds.getConnection().then(
		connection => {
			try {
				dboFactory.dbDriver.createVersionTableIfNotExists(
					connection, 'x2rcinfo', {
						trace(sql) {
							log(`executing SQL: ${sql}`);
						},
						onSuccess() {
							ds.releaseConnection(connection);
							tracker.initComplete();
						},
						onError(err) {
							ds.releaseConnection(connection);
							common.error(
								'error creating record collections version' +
									' info table', err);
							tracker.initComplete(err);
						}
					});
			} catch (err) {
				ds.releaseConnection(connection);
				common.error(
					'error creating record collections version info table', err);
				tracker.initComplete(err);
			}
		},
		err => {
			common.error('error acquiring DB connection', err);
			tracker.initComplete(err);
		}
	);

	// add tracker to the DBO factory
	dboFactory.addRecordCollectionsTracker(tracker);

	// return it
	return tracker;
};
