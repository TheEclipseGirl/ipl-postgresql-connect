// Update with your config settings.

const configDb = {
    host: "localhost",
    user: "shalini",
    database: "ipl",
    password: "admin",
    port: 5432
}

module.exports = {
    development: {
        client: 'pg',
        connection: configDb,
        migrations: {
            directory: __dirname + '/db/migrations'
        },
        seeds: {
            directory: __dirname + '/db/seeds'
        }
    },
    production: {
        client: 'pg',
        connection: process.env.DATABASE_URL,
        migrations: {
            directory: __dirname + '/db/migrations'
        },
        seeds: {
            directory: __dirname + '/db/seeds'
        }
    }
}