const config = {
    levelControllerConfig: {
       tableName: "_data_elevator",
       connectionOptions: {
            host: "postgres",
            port: 5432,
            database: "postgres",
            user: "postgres",
            password: "postgres"
       }
    }
}

module.exports = config;
