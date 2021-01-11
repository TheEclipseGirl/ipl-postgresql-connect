const fastcsv = require("fast-csv");
const fs = require("fs");
const path = require('path');

exports.seed = function(knex) {
  // Deletes ALL existing entries
  return knex('deliveries').del()
    .then(function () {
      // Inserts seed entries

      let stream = fs.createReadStream(path.join(__dirname + '/../../data/deliveries.csv'));
      let csvData = [];
      let csvStream = fastcsv
      .parse({ 
        headers: true,
       })
      .on("data", function(data) {
          csvData.push(data);
      })
      .on("end", function() {
          csvData.shift();   
          return knex('deliveries').insert(csvData);
      });
      stream.pipe(csvStream);
      
    });
};
