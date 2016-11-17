/**
 * Created by Sukitha on 11/16/2016.
 */


var ExternalUser = require('dvp-mongomodels/model/ExternalUser');
var logger = require('dvp-common/LogHandler/CommonLogHandler.js').logger;
var util = require('util');
var config = require('config');
const commandLineArgs = require('command-line-args')
var csv = require('fast-csv');
var fs = require('fs');
var mongoip=config.Mongo.ip;
var mongoport=config.Mongo.port;
var mongodb=config.Mongo.dbname;
var mongouser=config.Mongo.user;
var mongopass = config.Mongo.password;



var mongoose = require('mongoose');
var connectionstring = util.format('mongodb://%s:%s@%s:%d/%s',mongouser,mongopass,mongoip,mongoport,mongodb)


var async = require("async");

mongoose.connection.on('error', function (err) {
    console.error( new Error(err));
});

mongoose.connection.on('disconnected', function() {
    console.error( new Error('Could not connect to database'));
});

mongoose.connection.once('open', function() {
    console.log("Connected to db");
});


mongoose.connect(connectionstring);

const optionDefinitions = [
    { name: 'verbose', alias: 'v', type: Boolean },
    { name: 'batch', alias: 'b', type: Number }
];

const options = commandLineArgs(optionDefinitions)




var stream = fs.createReadStream("my.csv");

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

var parse = require('csv-parse');

var csvData = [];
var asyncTasks = [];

ExternalUser.findOne({}, function(err,data){

    if(err){

    }
});

stream.pipe(parse({delimiter: ',', quote:'', escape:'', relax_column_count:true, columns:['CLIENTNO','STAKEHOLDERNO','FIRSTNAME','SECONDNAME','LASTNAME','GLOBAL_ID_TYPE','CUSTOMERCODE','PHONE01','PHONE02','PHONE03','PHONE04','TITLE','EMAIL','CITY','ADDRESSLINE1','ADDRESSLINE2','ADDRESSLINE3']}))
    .on('data', function(csvrow) {
        //console.log(csvrow);
        //do something with csvrow


        //CLIENTNO: '0000IY000543',
        //    STAKEHOLDERNO: '',
        //    FIRSTNAME: '#',
        //    SECONDNAME: '#',
        //    LASTNAME: 'P YAYANETTI',
        //    GLOBAL_ID_TYPE: '',
        //    CUSTOMERCODE: '0000IY000543',
        //    PHONE01: '#',
        //    PHONE02: '',
        //    PHONE03: '',
        //    PHONE04: '',
        //    TITLE: 'MR',
        //    EMAIL: '',
        //    CITY: '#',
        //    ADDRESSLINE1: 'NO 250 LEFT BANK MAHAWILACHCHIYA',
        //    ADDRESSLINE2: '#',
        //    ADDRESSLINE3: '#'

        //{contacts : {contact:req.params.contact, type:req.body.type, verified: false}}


        //generate contacts

        var contacts = [];

        if(csvrow.PHONE02){

            var nums = csvrow.PHONE02.split(/[ ,]+/)
            nums.forEach(function(item){

                item.replace('-', '');
                var contact = {contacts : {contact:item, type:'call', verified: false}}

                contacts.push(contact);
            });

        }

        if(csvrow.PHONE03){

            var nums = csvrow.PHONE03.split(/[ ,]+/)
            nums.forEach(function(item){

                item.replace('-', '');
                var contact = {contacts : {contact:item, type:'call', verified: false}}

                contacts.push(contact);
            });
        }

        if(csvrow.PHONE04){

            var nums = csvrow.PHONE04.split(/[ ,]+/)
            nums.forEach(function(item){

                item.replace('-', '');
                var contact = {contacts : {contact:item, type:'call', verified: false}}

                contacts.push(contact);
            });
        }




        var extUser = ExternalUser({
            title: csvrow.TITLE,
            name: csvrow.FIRSTNAME,
            //avatar: req.body.avatar,
            //birthday: req.body.birthday,
            //gender: req.body.gender,
            firstname: csvrow.FIRSTNAME,
            lastname: csvrow.LASTNAME,
            //locale: req.body.locale,
            //ssn: csvrow.CUSTOMERCODE,
            contacts: contacts,
            address: {
                zipcode: csvrow.ADDRESSLINE1,
                number: csvrow.ADDRESSLINE2,
                street: csvrow.ADDRESSLINE3
                //city: req.body.address.city,
                //province: req.body.address.province,
                //country: req.body.address.country,

            },


            //phone: csvrow.PHONE01,
            //email: csvrow.EMAIL,
            company: 2,
            tenant: 2,
            created_at: Date.now(),
            updated_at: Date.now()
        });


        if(!csvrow.FIRSTNAME || csvrow.FIRSTNAME == '#'){
            extUser.firstname = csvrow.LASTNAME;
            extUser.name = csvrow.LASTNAME;
        }


        if(csvrow.PHONE01 && csvrow.PHONE01 != '#'){
            extUser.phone = csvrow.PHONE01;
        }


        if(csvrow.EMAIL && csvrow.EMAIL != '#'){
            extUser.email = csvrow.EMAIL;
        }

        if(csvrow.CUSTOMERCODE && csvrow.CUSTOMERCODE != '#'){
            extUser.ssn = csvrow.CUSTOMERCODE;
        }



        csvData.push(extUser);


    })
    .on('end',function() {

        var chunk = chunks(csvData, 100);
        csvData = [];

        chunk.forEach(function(item){

            asyncTasks.push(function(callback){



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



        async.waterfall(asyncTasks, function(){
            // All tasks are done now
            //doSomethingOnceAllAreDone();
        });


    });

function onInsert(err, docs) {
    if (err) {
        console.error(err);
    } else {
        console.info('%d External users were successfully stored.', docs.length);
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
        .then(function (mongooseDocuments) {
            /* ... */
            console.log("data inserted");
        })
        .catch(function (err) {
            /* Error handling */
            console.log("data insert failed", err);
        });

};






