/**
 * PostgresLevelController
 * Store and retrieve current level from postgres
**/
var async = require('async');
var pg = require('pg');
var Errors = require('data-elevator/lib/errors/elevator-errors');
var BaseLevelController = require('data-elevator/lib/level-controllers/base-level-controller.js');
var Level = require('data-elevator/lib/level-controllers/level.js');

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
    var tableName = this.config.levelControllerConfig.tableName;
    var connection = new pg.Client(this.config.levelControllerConfig.connectionOptions);

    connection.connect((error) => {
      if (error) {
        return callback(Errors.generalError('Postgres connection error', error));
      } else {
        //Create table if it is not available
        var query = "CREATE TABLE IF NOT EXISTS " + tableName + " (" +
                    "identifier integer, " +
                    "timestamp bigint);";

        connection.query(query, (error, result) => {
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
    var tableName = this.config.levelControllerConfig.tableName;

    async.waterfall([
      //Get database connection
      (callback) => {
        this.getConnection((error, connection) => {
          return callback(error, connection);
        });
      },
      //Check if there are already migrations
      (connection, callback) => {
        var query = "SELECT count(*) AS rowcount FROM " + tableName;
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
        var query = null;

        if (rowsFound === false) {
          query = "INSERT INTO " + tableName + "(identifier, timestamp) VALUES (" + level.identifier + ", " + level.timestamp + ")";
        } else {
          query = "UPDATE " + tableName + " SET identifier=" + level.identifier + ", timestamp=" + level.timestamp;
        }

        connection.query(query, (error, result) => {
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
    var tableName = this.config.levelControllerConfig.tableName;

    async.waterfall([
      //Get postgres connection
      (callback) => {
        this.getConnection((error, connection) => {
          return callback(error, connection);
        });
      },
      //Retrieve level
      (connection, callback) => {
        var query = "SELECT * FROM " + tableName + " LIMIT 1";
        connection.query(query, (error, result) => {
          connection.end();

          if (error) {
            return callback(Errors.generalError("Postgres failed to query table '" + tableName + "'", error));
          } else {
            var level = null;

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