const fs = require("fs");
const { Pool } = require('pg');
const fastcsv = require("fast-csv");
const format = require('pg-format');

const configDb = {
    host: "localhost",
    user: "shalini",
    database: "ipl",
    password: "admin",
    port: 5432
}

const createTable = (tableName, columns) => {
    return new Promise((resolve, reject) =>{
        const pool = new Pool(configDb);
        pool.connect((err, client, done) => {
            if(err) throw err;
            console.log('Connected to database');
            columns = columns.join(',');
            pool.query(`CREATE TABLE ${tableName} ( ${columns} )`, (err, res) => {
                if (err) throw err
                console.log('New Table created');
                done();
                resolve();
            });
        });
    });
}

const insertCsvIntoTable = (filePath, fileName,columns) => {
    let stream = fs.createReadStream(filePath);
    let csvData = [];
    let csvStream = fastcsv
    .parse()
    .on("data", function(data) {
        csvData.push(data);
    })
    .on("end", function() {

        // remove the first line: header
        csvData.shift();
        const pool = new Pool(configDb);

        columns = columns.join(',');                            
        const query = format(`INSERT INTO ${fileName} ( ${columns} ) VALUES %L`, csvData);

        pool.connect((err, client, done) => {
            if (err) throw err;
            console.log('connected');
            console.log('Total rows to be inserted:-', csvData.length - 1)
            console.log('Starting insertion.....');
            pool.query(query, (err, res) => {
                if(err){
                    console.log('Error while inserting a row',err);
                    throw err;
                }
                console.log('Rows have been inserted');
            });
            done();
        });
    });
    stream.pipe(csvStream);
}

(async function(){
    // deliveries
    let deliveriesColumnsWithDatatypes = ['match_id INTEGER','inning INTEGER','batting_team TEXT','bowling_team TEXT','over INTEGER','ball INTEGER','batsman TEXT','non_striker TEXT','bowler TEXT','is_super_over INTEGER','wide_runs INTEGER','bye_runs INTEGER','legbye_runs INTEGER','noball_runs INTEGER','penalty_runs INTEGER','batsman_runs INTEGER','extra_runs INTEGER','total_runs INTEGER','player_dismissed TEXT','dismissal_kind TEXT','fielder TEXT'];
    let deliveriesColumns = ['match_id','inning','batting_team','bowling_team','over','ball','batsman','non_striker','bowler','is_super_over','wide_runs','bye_runs','legbye_runs','noball_runs','penalty_runs','batsman_runs','extra_runs','total_runs','player_dismissed','dismissal_kind','fielder'];
    await createTable('deliveries', deliveriesColumnsWithDatatypes);
    insertCsvIntoTable("./data/deliveries.csv", 'deliveries', deliveriesColumns);

    // matches
    // let matchesColumnsWithDatatypes = ['id INTEGER PRIMARY KEY','season INTEGER','city TEXT','date TEXT','team1 TEXT','team2 TEXT','toss_winner TEXT','toss_decision TEXT','result TEXT','dl_applied INTEGER','winner TEXT','win_by_runs INTEGER','win_by_wickets INTEGER','player_of_match TEXT','venue TEXT','umpire1 TEXT','umpire2 TEXT','umpire3 TEXT'];
    // let matchesColumns = ['id','season','city','date','team1','team2','toss_winner','toss_decision','result','dl_applied','winner','win_by_runs','win_by_wickets','player_of_match','venue','umpire1','umpire2','umpire3']
    // await createTable('matches', matchesColumnsWithDatatypes);
    // insertCsvIntoTable("./data/matches.csv", 'matches', matchesColumns);
})();