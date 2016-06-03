var environment = process.env.NODE_ENV ? process.env.NODE_ENV : "developent";

var config = {
    levelControllerConfig : {
       tableName: "_data_elevator",
       connectionUrl: "postgres://postgres:postgres@192.168.99.100:5432/test"
    }
}

switch(environment) {
    case "development":
        break;
}

module.exports = config;