 
const environment = process.env.NODE_ENV ? process.env.NODE_ENV : "developent";
 

const config = {
  levelControllerConfig : {
    tableName: "_data_elevator",
    connectionOptions: {
      host: null,   
      port: null,
      database: null,
      user: null,
      password: null
    }
  }
}

switch(environment) {
case "development":
  break;
}

module.exports = config;