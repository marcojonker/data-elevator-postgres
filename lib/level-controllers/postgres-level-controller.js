/**
 * PostgresLevelController
 * Store and retrieve current level from postgres
**/
const async = require('async');
const pg = require('pg');
const Errors = require('data-elevator/lib/errors/elevator-errors');
const BaseLevelController = require('data-elevator/lib/level-controllers/base-level-controller.js');
const Level = require('data-elevator/lib/level-controllers/level.js');

/**
 * Constructor
 * @param config
 */
class PostgresLevelController extends BaseLevelController {
  constructor(config) {
    super(config)
    this.database = null;

    if (!config.levelControllerConfig.connectionOptions || typeof config.levelControllerConfig.connectionOptions !== 'object') {
      throw Errors.invalidConfig('Postgres connection options missing in configuration file');
    }

    if (!config.levelControllerConfig.tableName || typeof config.levelControllerConfig.tableName !== 'string' && config.levelControllerConfig.tableName.length === 0) {
      throw Errors.invalidConfig('Postgres tableName missing in configuration file');
    }
  }

  /**
     * Get database connection and check table existence
     * @param callback(error, connection)
     */
  getConnection(callback) {
    const tableName = this.config.levelControllerConfig.tableName;
    const connection = new pg.Client(this.config.levelControllerConfig.connectionOptions);

    connection.connect((error) => {
      if (error) {
        return callback(Errors.generalError('Postgres connection error', error));
      } else {
        //Create table if it is not available
        const query = "CREATE TABLE IF NOT EXISTS " + tableName + " (" +
                    "identifier integer, " +
                    "timestamp bigint);";

        connection.query(query, (error, _result) => {
          if (error) {
            connection.end();
            return callback(Errors.generalError("Postgres failed to create table '" + tableName + "'", error));
          } else {
            return callback(error, connection);
          }
        });
      }
    });
  }

  /**
     * Save level
     * @param level
     * @param callback(error)
     */
  saveCurrentLevel(level, callback) {
    const tableName = this.config.levelControllerConfig.tableName;

    async.waterfall([
      //Get database connection
      (callback) => {
        this.getConnection((error, connection) => {
          return callback(error, connection);
        });
      },
      //Check if there are already migrations
      (connection, callback) => {
        const query = "SELECT count(*) AS rowcount FROM " + tableName;
        connection.query(query, function (error, result) {
          if (error) {
            connection.end();
            return callback(Errors.generalError("Postgres failed to query table '" + tableName + "'", error));
          } else {
            return callback(null, connection, (parseInt(result.rows[0].rowcount) !== 0));
          }
        });
      },
      //Insert or update current migration
      (connection, rowsFound, callback) => {
        let query = null;

        if (rowsFound === false) {
          query = "INSERT INTO " + tableName + "(identifier, timestamp) VALUES (" + level.identifier + ", " + level.timestamp + ")";
        } else {
          query = "UPDATE " + tableName + " SET identifier=" + level.identifier + ", timestamp=" + level.timestamp;
        }

        connection.query(query, (error, _result) => {
          connection.end();

          if (error) {
            return callback(Errors.generalError("Postgres failed to update table '" + tableName + "'", error));
          } else {
            return callback(null);
          }
        });
      }],
    (error) => {
      return callback(error);
    }
    );
  }

  /**
     * Retrieve the current level
     * @param callback(error, level)
     */
  retrieveCurrentLevel(callback) {
    const tableName = this.config.levelControllerConfig.tableName;

    async.waterfall([
      //Get postgres connection
      (callback) => {
        this.getConnection((error, connection) => {
          return callback(error, connection);
        });
      },
      //Retrieve level
      (connection, callback) => {
        const query = "SELECT * FROM " + tableName + " LIMIT 1";
        connection.query(query, (error, result) => {
          connection.end();

          if (error) {
            return callback(Errors.generalError("Postgres failed to query table '" + tableName + "'", error));
          } else {
            let level = null;

            if (result.rowCount > 0) {
              level = new Level();
              level.identifier = result.rows[0].identifier;
              level.timestamp = result.rows[0].timestamp;
            }
            return callback(null, level);
          }
        });
      }], callback);
  }
}

module.exports = PostgresLevelController;