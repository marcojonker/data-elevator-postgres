var config = {
    levelControllerConfig: {
       tableName: "_data_elevator",
       connectionOptions: {
            host: "localhost",
            port: 5432,
            database: "test",
            user: "admin",
            password: "admin"
       }
    }
}

module.exports = config;