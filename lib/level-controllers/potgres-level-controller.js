/**
 * PostgresLevelController
 * Store and retrieve current level from mongodb
**/

'use strict'

var util = require('util');
var async = require('async');
var Errors = require('data-elevator/lib/errors/elevator-errors');
var BaseLevelController = require('data-elevator/lib/level-controllers/base-level-controller.js');

/**
 * Constructor
 * @param config
 */
var PostgresLevelController = function(config) {
    this.database = null;
    
    PostgresLevelController.super_.apply(this, arguments);
};

util.inherits(PostgresLevelController, BaseLevelController);

/**
 * Save level
 * @param level
 * @param callback(error)
 */
PostgresLevelController.prototype.saveCurrentLevel = function(level, callback) {
    return callback(null);
};

/**
 * Retrieve the current level
 * @param callback(error, level)
 */
PostgresLevelController.prototype.retrieveCurrentLevel = function(callback) {
    return callback(null);
};

module.exports = PostgresLevelController;