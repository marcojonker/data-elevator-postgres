/**
 * Elevator
 * Data elevator
**/
const ElevatorBase = require('data-elevator/lib/elevator-engine/elevator-base');
const ConsoleLogger = require('data-elevator/lib/logger/console-logger');
const PostgresLevelController = require('data-elevator-postgres/lib/level-controllers/postgres-level-controller');

/**
 * Constructor
 * @param logger
 * @param LevelController
 */
class Elevator extends ElevatorBase {
    constructor(logger, LevelController) {
        super(logger, LevelController)
    }
}

const elevator = new Elevator(new ConsoleLogger(false), PostgresLevelController);

//Run the elevator
elevator.run(function(_error) { });