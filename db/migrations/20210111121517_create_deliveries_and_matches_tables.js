exports.up = function(knex) {
  return knex.schema.createTable('deliveries', (table) => {
    table.integer('match_id');
    table.integer('inning');
    table.text('batting_team');
    table.text('bowling_team');
    table.integer('over');
    table.integer('ball');
    table.text('batsman');
    table.text('non_striker');
    table.text('bowler');
    table.integer('is_super_over');
    table.integer('wide_runs');
    table.integer('bye_runs');
    table.integer('legbye_runs');
    table.integer('noball_runs');
    table.integer('penalty_runs');
    table.integer('batsman_runs');
    table.integer('extra_runs');
    table.integer('total_runs');
    table.text('player_dismissed');
    table.text('dismissal_kind');
    table.text('fielder');
  })
  .createTable('matches', (table) => {
    table.integer('id');
    table.integer('season');
    table.text('city');
    table.text('date');
    table.text('team1');
    table.text('team2');
    table.text('toss_winner');
    table.text('toss_decision');
    table.text('result');
    table.integer('dl_applied');
    table.text('winner');
    table.integer('win_by_runs');
    table.integer('win_by_wickets');
    table.text('player_of_match');
    table.text('venue');
    table.text('umpire1');
    table.text('umpire2');
    table.text('umpire3');
  })
};

exports.down = function(knex) {
  return knex.schema.dropTable('deliveries').dropTable('matches');
};
