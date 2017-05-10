/**
 * Implementation of the record collections version tracker for the DBOs module
 * that uses a database table to store current record collections version
 * information.
 *
 * @module x2node-dbos-rctrack-dbtable
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
 * The tracker.
 *
 * @private
 * @inner
 */
class DBTableRecordCollectionsTracker {

	constructor() {

		this._ready = false;
		this._initError = null;
	}

	initComplete(err) {

		//...
	}

	recordsUpdated(ctx, recordTypeNames) {

		//...
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
