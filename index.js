const fs = require("fs");
const { Pool } = require('pg');
const fastcsv = require("fast-csv");
const format = require('pg-format');
const knex = require('./db/knex');
const { select, count } = require("./db/knex");
const { matches } = require("lodash");

const configDb = {
    host: "localhost",
    user: "shalini",
    database: "ipl",
    password: "admin",
    port: 5432
}


const insertCsvIntoTable = (filePath, tableName) => {
    return new Promise((resolve, reject) => {
        let stream = fs.createReadStream(filePath);
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
            const rows = csvData;
            const chunkSize = 30;
            knex.batchInsert(tableName, rows, chunkSize)
            .then(function(ids) { 
                console.log('Inserted');
                resolve();
            })
            .catch(function(error) { 
                console.log(error);
            });
        });
        stream.pipe(csvStream);
    });
}


const matchesPLayedPerYear = () => {
    return new Promise((resolve, reject) => {
        knex.select('season as year').count('* as total_matches').from('matches').groupBy('season').havingRaw('count(*) > 1').orderBy('year')
        .then((output) => {
            console.log(output);
            resolve();
        })
        .catch((error) => {
            console.log('Error in matches played per year:', error);
            reject();
        })
    });
}


const matchesWonPerYearPerTeam = () => {
    return new Promise((resolve, reject) => {
// select season,winner,count(winner) as matches_won from matches where winner !=  ''  group by winner,season order by season;
        knex.select('season','winner').count('winner as matches_won').from ('matches').whereNot('winner' , '').groupBy('winner','season').orderBy('season')
        .then((output)=>{
            console.log(output);
            resolve();
        })
        .catch((error)=>{
            console.log('Error in matches played per year:', error);
            reject();
        })
    });
}

const extraRunsConceededInAYear = (year) => {
    return new Promise((resolve, reject) => {
        const subquery = knex.select('id as match_id').from ('matches').whereRaw(`season = ${year}`)
        knex.distinct('match_id','bowling_team').sum('extra_runs as total_extras').from('deliveries').whereIn('match_id',subquery).groupBy('match_id','bowling_team').orderBy('match_id')
        .then((output) => {
            console.log(output);
            resolve();
        })
        .catch((error) => {
            console.log('Error in matches played per year:', error);
            reject();
        })
    });
}

const topEconomicalBowlersInAYear = (limit, year) => {
    return new Promise((resolve, reject) => {
        // select row_number() over (order by economy) as rank ,bowler, economy from  (select bowler,((1.0 * sum(total_runs))/count(over)) as economy from matches inner join deliveries on id = match_id where season = ${year} group by bowler order by economy limit ${limit}) as bowler_economy;
        const subquery = knex.raw(`select bowler,((1.0 * sum(total_runs))/count(over)) as economy from matches inner join deliveries on id = match_id where season = ${year} group by bowler order by economy limit ${limit}`);
        knex.raw(`select row_number() over (order by economy) as rank, bowler , economy from (${subquery}) as bowler_economy`)
        .then((output)=>{
            console.log(output);
            resolve();
        })
        .catch((error)=>{
            console.log('Error in matches played per year:', error);
            reject();
        })
    });
}

(async function(){
    // Insert queries using Knex :-
    // await insertCsvIntoTable('./data/deliveries.csv', 'deliveries');
    // await insertCsvIntoTable('./data/matches.csv', 'matches');

    // Common Query Functions -
    // await matchesPLayedPerYear();
    // await matchesWonPerYearPerTeam();
    // await extraRunsConceededInAYear(2016);
    await topEconomicalBowlersInAYear(10, 2015);
    
})();