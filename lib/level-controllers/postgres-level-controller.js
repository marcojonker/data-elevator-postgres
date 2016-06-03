/**
 * PostgresLevelController
 * Store and retrieve current level from mongodb
**/

'use strict'

var util = require('util');
var async = require('async');
var pg = require('pg');
var Errors = require('data-elevator/lib/errors/elevator-errors');
var BaseLevelController = require('data-elevator/lib/level-controllers/base-level-controller.js');
var Level = require('data-elevator/lib/level-controllers/level.js');

/**
 * Constructor
 * @param config
 */
var PostgresLevelController = function(config) {
    this.database = null;
    
    PostgresLevelController.super_.apply(this, arguments);
    
    if(!config.levelControllerConfig.connectionUrl || typeof config.levelControllerConfig.connectionUrl !== 'string' && config.levelControllerConfig.connectionUrl.length === 0) {
        throw Errors.invalidConfig('Postgres connectionUrl missing in configuration file');
    }

    if(!config.levelControllerConfig.tableName || typeof config.levelControllerConfig.tableName !== 'string' && config.levelControllerConfig.tableName.length === 0) {
        throw Errors.invalidConfig('Postgres tableName missing in configuration file');
    }
};

util.inherits(PostgresLevelController, BaseLevelController);

/**
 * Get database connection and check table existence
 * @param callback(error, connection, done (methods must be called to release connection))
 */
PostgresLevelController.prototype.getConnection = function(callback) {
    var self = this;
    pg.connect(this.config.levelControllerConfig.connectionUrl, function(error, client, done) {
        if(error) {
            return callback(Errors.generalError('Postgres connection error', error)); 
        } else {
           var query = "CREATE TABLE IF NOT EXISTS " + self.config.levelControllerConfig.tableName + " (" + 
                            "identifier integer, " + 
                            "direction varchar(10), " + 
                            "timestamp bigint);";
              
           client.query(query, function(error, result) {
                if(error) {
                    done();
                    return callback(Errors.generalError("Postgres failed to create table '" + self.config.levelControllerConfig.tableName + "'", error));
                } else {
                    return callback(error, client, done);
                }
           });
        }
    });
};

/**
 * Save level
 * @param level
 * @param callback(error)
 */
PostgresLevelController.prototype.saveCurrentLevel = function(level, callback) {
    var self = this;
    var tableName = this.config.levelControllerConfig.tableName;
    async.waterfall([
            function(callback) {
                self.getConnection(function(error, client, done) {
                    return callback(error, client, done);
                });            
            }, 
            function(client, done, callback) {
                var query = "SELECT count(*) AS rowcount FROM " + tableName;
                client.query(query, function(error, result) {
                    if(error) {
                        return callback(Errors.generalError("Postgres failed to query table '" + tableName + "'", error), done);
                    } else {
                        console.log(result.rows[0]);
                        return callback(null, client, done, (parseInt(result.rows[0].rowcount) !== 0));
                    }
                });
            
            },
            function(client, done, rowsFound, callback) {
                var query = null;
                console.log(rowsFound);
                if(rowsFound === false) {
                    query = "INSERT INTO " + tableName + "(identifier, direction, timestamp) VALUES (" + level.identifier + ", '" + level.direction +  "', " + level.timestamp + ")";
                } else {
                    query = "UPDATE " + tableName + " SET identifier=" + level.identifier + ", direction='" + level.direction + "', timestamp=" + level.timestamp;
                }

                client.query(query, function(error, result) {
                    if(error) {
                        return callback(Errors.generalError("Postgres failed to update table '" + tableName + "'", error), done);
                    } else {
                        return callback(null, done);
                    }
                });
            }  
        ], 
        function(error, done) {
            if(done) {
                done();
            }
            return callback(error);
        }
    );
};

/**
 * Retrieve the current level
 * @param callback(error, level)
 */
PostgresLevelController.prototype.retrieveCurrentLevel = function(callback) {
    var self = this;
    var tableName = this.config.levelControllerConfig.tableName;
    async.waterfall([
            function(callback) {
                self.getConnection(function(error, client, done) {
                    return callback(error, client, done);
                });            
            }, 
            function(client, done, callback) {
                var query = "SELECT * FROM " + tableName + " LIMIT 1";
                client.query(query, function(error, result) {
                    if(error) {
                        return callback(Errors.generalError("Postgres failed to query table '" + tableName + "'", error), done);
                    } else {
                        var level = null;
                        
                        if(result.rowCount > 0) {
                            level = new Level();
                            level.identifier = result.rows[0].identifier;
                            level.direction = result.rows[0].direction;
                            level.timestamp = result.rows[0].timestamp;
                        } 
                        console.log(level);                        
                        return callback(null, done, level);
                    }
                });
            },
        ], 
        function(error, done, level) {
            if(done) {
                done();
            }
            return callback(error, level);
        }
    );
};

module.exports = PostgresLevelController;