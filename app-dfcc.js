/**
 * Created by Sukitha on 11/16/2016.
 */

var ExternalUser = require("dvp-mongomodels/model/ExternalUser");
var logger = require("dvp-common/LogHandler/CommonLogHandler.js").logger;

var config = require("config");
const commandLineArgs = require("command-line-args");
var csv = require("fast-csv");
var fs = require("fs");

var util = require("util");
var mongoip = config.Mongo.ip;
var mongoport = config.Mongo.port;
var mongodb = config.Mongo.dbname;
var mongouser = config.Mongo.user;
var mongopass = config.Mongo.password;
var mongoreplicaset = config.Mongo.replicaset;

var mongoose = require("mongoose");
var connectionstring = "";
if (util.isArray(mongoip)) {
  mongoip.forEach(function(item) {
    connectionstring += util.format("%s:%d,", item, mongoport);
  });

  connectionstring = connectionstring.substring(0, connectionstring.length - 1);
  connectionstring = util.format(
    "mongodb://%s:%s@%s/%s",
    mongouser,
    mongopass,
    connectionstring,
    mongodb
  );

  if (mongoreplicaset) {
    connectionstring = util.format(
      "%s?replicaSet=%s",
      connectionstring,
      mongoreplicaset
    );
  }
} else {
  connectionstring = util.format(
    "mongodb://%s:%s@%s:%d/%s",
    mongouser,
    mongopass,
    mongoip,
    mongoport,
    mongodb
  );
}

var async = require("async");

mongoose.connect(connectionstring, { server: { auto_reconnect: true } });

mongoose.connection.on("error", function(err) {
  console.error(new Error(err));
  mongoose.disconnect();
});

mongoose.connection.on("opening", function() {
  console.log("reconnecting... %d", mongoose.connection.readyState);
});

mongoose.connection.on("disconnected", function() {
  console.error(new Error("Could not connect to database"));
  mongoose.connect(connectionstring, { server: { auto_reconnect: true } });
});

mongoose.connection.once("open", function() {
  console.log("Connected to db");
});

mongoose.connection.on("reconnected", function() {
  console.log("MongoDB reconnected!");
});

process.on("SIGINT", function() {
  mongoose.connection.close(function() {
    console.log(
      "Mongoose default connection disconnected through app termination"
    );
    process.exit(0);
  });
});

const optionDefinitions = [
  { name: "verbose", alias: "v", type: Boolean },
  { name: "batch", alias: "b", type: Number, defaultValue: 10000 },
  { name: "company", alias: "c", type: Number, defaultValue: -1 },
  { name: "tenant", alias: "t", type: Number, defaultValue: -1 },
  { name: "file", alias: "f", type: String, defaultValue: "my.csv" }
];

const options = commandLineArgs(optionDefinitions);

console.log(options);

var stream = fs.createReadStream(options.file);

/*
var csvStream = csv
    .parse()
    .on("data", function(data){
        console.log(data);
    })
    .on("end", function(){
        console.log("done");
    });

stream.pipe(csvStream);
*/

var parse = require("csv-parse");

var csvData = [];
var asyncTasks = [];

ExternalUser.findOne({}, function(err, data) {
  if (err) {
  }
});

stream
  .pipe(
    parse({
      delimiter: "|",
      quote: "",
      escape: "",
      relax_column_count: true,
      columns: ["FIRSTNAME", "SSN", "PHONE01"]
    })
  )
  .on("data", function(csvrow) {
    var contacts = [];

    var extUser = ExternalUser({
      name: csvrow.FIRSTNAME,
      firstname: csvrow.FIRSTNAME,
      lastname: csvrow.LASTNAME,
      contacts: contacts,
      ssn: csvrow.SSN,
      phone: csvrow.PHONE01,
      company: options.company,
      tenant: options.tenant,
      created_at: Date.now(),
      updated_at: Date.now()
    });

    console.log(csvrow.CUSTOMERCODE);
    csvData.push(extUser);
  })
  .on("end", function() {
    var chunk = chunks(csvData, options.batch);
    csvData = [];

    chunk.forEach(function(item) {
      asyncTasks.push(function(callback) {
        ExternalUser.insertMany(item)
          .then(function(mongooseDocuments) {
            /* ... */
            console.log("data inserted");
            callback();
          })
          .catch(function(err) {
            /* Error handling */
            console.log("data insert failed", err);
            callback();
          });
      });
    });

    async.waterfall(asyncTasks, function() {
      // All tasks are done now
      //doSomethingOnceAllAreDone();
      console.log("data insertion completed");
    });
  });

function onInsert(err, docs) {
  if (err) {
    console.error(err);
  } else {
    console.info("%d External users were successfully stored.", docs.length);
  }
}

var chunks = function(array, size) {
  var results = [];
  while (array.length) {
    results.push(array.splice(0, size));
  }
  return results;
};

var insert = function(csvData) {
  ExternalUser.insertMany(csvData)
    .then(function(mongooseDocuments) {
      /* ... */
      console.log("data inserted");
    })
    .catch(function(err) {
      /* Error handling */
      console.log("data insert failed", err);
    });
};
