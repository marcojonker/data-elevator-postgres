/**
 * Test function for data elevator
**/

'use strict'

var TestBase = require('../node_modules/data-elevator/test/test-base.js');
var path = require('path');
var PostgresLevelController = require('../lib/level-controllers/postgres-level-controller.js');

var test = new TestBase(path.normalize(path.join(__dirname, '../')), PostgresLevelController);
test.runDefaultCommandTests();
