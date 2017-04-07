const csvFilePath = './csv/test_players.csv';
const csv         = require('csvtojson');
const MongoClient = require('mongodb').MongoClient;
const url         = 'mongodb://localhost:27017/ehm';
const argv        = require('minimist')(process.argv.slice(2));
// const Promise = require("bluebird");

var isValidDate = (date) => {
  let dateArray = date.split("-"),
      year = dateArray[0],
      month = parseInt(dateArray[1], 10),
      day = parseInt(dateArray[2], 10);

  if ( (year.length === 4) && (month >= 1) && (month <= 12) && (day >= 1) && (day <= 31) ) {
    return true;
  } else {
    return false;
  }
};

// TODO track dates used in a collection
if (isValidDate(argv.date)) {
  var date = argv.date;
} else {
  throw("Date Arg is Not Provided or is Invalid! [eg: --date 12-31-1976]");
}

var initPlayer = (doc) => {
  var player = {};

  player.general = {};
  player.stats = {};
  player.stats.technical = {};
  player.stats.mental = {};
  player.stats.physical = {};
  player.stats.positional = {};
  player.stats.hidden = {};

  // general non-stat player properties, TODO: why are height and weight not exported?
  player._id = doc._id;
  player.ehm_id = doc.Id;
  player.general.name = doc.Name;
  player.general.nation = doc.Nation;
  player.general.second_nation = doc['Second Nation'];
  player.general.international_apps = doc['International Apps'];
  player.general.international_goals = doc['International Goals'];
  player.general.international_assists = doc['International Assists'];
  player.general.estimated_value = [ { date: date, value: doc['Estimated Value'] } ];
  player.general.club_playing = [ { date: date, value: doc['Club Playing'] } ];
  player.general.division_playing = [ { date: date, value: doc['Division Playing'] } ];
  player.general.club_contracted = [ { date: date, value: doc['Club Contracted'] } ];
  player.general.positions_short = doc['Positions Short'];
  player.general.stanley_cups_won = doc['Stanley Cups Won'];
  player.general.birth_town = doc['Birth Town'];
  player.general.date_of_birth = doc['Date Of Birth'];
  player.general.age = doc.Age;
  player.general.current_ability = [ { date: date, value: doc['Current Ability'] } ];
  player.general.home_reputation = [ { date: date, value: doc['Home Reputation'] } ];
  player.general.current_reputation = [ { date: date, value: doc['Current Reputation'] } ];
  player.general.world_reputation = [ { date: date, value: doc['World Reputation'] } ];
  player.general.handedness = doc.Handedness;
  player.general.junior_preference = doc['Junior Preference'];
  player.general.player_roles = doc['Player Roles'];
  player.general.defensive_role = [ { date: date, value: doc['Defensive Role'] } ];
  player.general.offensive_role = [ { date: date, value: doc['Offensive Role'] } ];
  // player.general.int = [ { date: date, value: doc.Int } ]; // TODO decide if I should track this stat, no idea what it's for
  player.general.morale = [ doc.Morale ];
  player.general.favorite_number = doc['Favorite Number'];
  player.general.squad_number = [ { date: date, value: doc['Squad Number'] } ];

  // stats hidden by the game, should probably not be revealed in UI
  player.stats.hidden.loyalty = [ { date: date, value: doc.Loyalty } ];
  player.stats.hidden.pressure = [ { date: date, value: doc.Pressure } ];
  player.stats.hidden.professionalism = [ { date: date, value: doc.Professionalism } ];
  player.stats.hidden.sportsmanship = [ { date: date, value: doc.Sportsmanship } ];
  player.stats.hidden.temperament = [ { date: date, value: doc.Temperament } ];
  player.stats.hidden.consistency = [ { date: date, value: doc.Consistency } ];
  player.stats.hidden.decisions = [ { date: date, value: doc.Decisions } ];
  player.stats.hidden.dirtiness = [ { date: date, value: doc.Dirtiness } ];
  player.stats.hidden.potential_ability = doc['Potential Ability'];
  player.stats.hidden.adaptability = [ { date: date, value: doc.Adaptability } ];
  player.stats.hidden.ambition = [ { date: date, value: doc.Ambition } ];
  player.stats.hidden.important_matches = [ { date: date, value: doc['Important Matches'] } ];
  player.stats.hidden.pass_tendency = [ { date: date, value: doc['Pass Tendency'] } ];
  player.stats.hidden.fighting = [ { date: date, value: doc.Fighting } ];
  player.stats.hidden.injury_proneness = [ { date: date, value: doc['Injury Proneness'] } ];
  player.stats.hidden.natural_fitness = [ { date: date, value: doc['Natural Fitness'] } ];
  player.stats.hidden.agitation = [ { date: date, value: doc.Agitation } ];
  player.stats.hidden.one_on_ones = [ { date: date, value: doc['One On Ones'] } ];
  player.stats.hidden.versatility = [ { date: date, value: doc.Versatility } ];

  // positional stat ratings
  player.stats.positional.goaltender = [ { date: date, value: doc.Goaltender } ];
  player.stats.positional.left_defence = [ { date: date, value: doc['Left Defence'] } ];
  player.stats.positional.right_defence = [ { date: date, value: doc['Right Defence'] } ];
  player.stats.positional.left_wing = [ { date: date, value: doc['Left Wing'] } ];
  player.stats.positional.right_wing = [ { date: date, value: doc['Right Wing'] } ];
  player.stats.positional.center = [ { date: date, value: doc.Center } ];

  // stats labeled as 'mental' in game
  player.stats.mental.aggression = [ { date: date, value: doc.Aggression } ];
  player.stats.mental.anticipation = [ { date: date, value: doc.Anticipation } ];
  player.stats.mental.bravery = [ { date: date, value: doc.Bravery } ];
  player.stats.mental.creativity = [ { date: date, value: doc.Creativity } ];
  player.stats.mental.determination = [ { date: date, value: doc.Determination } ];
  player.stats.mental.flair = [ { date: date, value: doc.Flair } ];
  player.stats.mental.influence = [ { date: date, value: doc.Influence } ];
  player.stats.mental.teamwork = [ { date: date, value: doc.Teamwork } ];
  player.stats.mental.work_rate = [ { date: date, value: doc['Work Rate'] } ];

  // stats labeled as 'physical' in game
  player.stats.physical.acceleration = [ { date: date, value: doc.Acceleration } ];
  player.stats.physical.agility = [ { date: date, value: doc.Agility } ];
  player.stats.physical.balance = [ { date: date, value: doc.Balance } ];
  player.stats.physical.speed = [ { date: date, value: doc.Speed } ];
  player.stats.physical.stamina = [ { date: date, value: doc.Stamina } ];
  player.stats.physical.strength = [ { date: date, value: doc.Strength } ];

  // stats labeled as 'technical' in game for forwards and defencemen
  player.stats.technical.checking = [ { date: date, value: doc.Checking } ];
  player.stats.technical.deflections = [ { date: date, value: doc.Deflections } ];
  player.stats.technical.deking = [ { date: date, value: doc.Deking } ];
  player.stats.technical.faceoffs = [ { date: date, value: doc.Faceoffs } ];
  player.stats.technical.hitting = [ { date: date, value: doc.Hitting } ];
  player.stats.technical.off_the_puck = [ { date: date, value: doc['Off The Puck'] } ];
  player.stats.technical.passing = [ { date: date, value: doc.Passing } ];
  player.stats.technical.pokecheck = [ { date: date, value: doc.Pokecheck } ];
  player.stats.technical.positioning = [ { date: date, value: doc.Positioning } ];
  player.stats.technical.slapshot = [ { date: date, value: doc.Slapshot } ];
  player.stats.technical.stickhandling = [ { date: date, value: doc.Stickhandling } ];
  player.stats.technical.wristshot = [ { date: date, value: doc.Wristshot } ];

  // stats labeled as 'technical' in game for goalies
  player.stats.technical.blocker = [ { date: date, value: doc.Blocker } ];
  player.stats.technical.glove = [ { date: date, value: doc.Glove } ];
  player.stats.technical.rebound_control = [ { date: date, value: doc['Rebound Control'] } ];
  player.stats.technical.recovery = [ { date: date, value: doc.Recovery } ];
  player.stats.technical.reflexes = [ { date: date, value: doc.Reflexes } ];

  return player;
};

var run = () => {
  MongoClient.connect(url).then((db) => {
    console.log("Connected to Mongo Instance.");
    var collection = db.collection('Player');

    var processStatArray = ({ csvPlayer, dbPlayer, category, stat }) => {
      console.log("Process Array Stat:", stat);
      var currentStatCsv = csvPlayer[category][stat].slice(-1)[0];
      var currentStatDb = dbPlayer[category][stat].slice(-1)[0];

      if (currentStatCsv.value && currentStatCsv.value !== currentStatDb.value) {
        if (currentStatCsv.date !== currentStatDb.date) {
          console.log(stat + " stat changed: ", currentStatDb, " to ", currentStatCsv );

          collection.updateOne({ ehm_id: dbPlayer.ehm_id }, { $push: { [category + "." + stat]: { date: date, value: currentStatCsv.value } } });
        } else {
          console.log("duplicate date");
        }
      }
    };

    var processStat = ({ csvPlayer, dbPlayer, category, stat }) => {
      console.log("Process Stat: ", stat);
      var currentStatCsv = csvPlayer[category][stat];
      var currentStatDb = dbPlayer[category][stat];

      if (currentStatCsv && currentStatCsv !== currentStatDb) {
        console.log(stat + " stat changed: ", currentStatDb, " to ", currentStatCsv );
        var key = category + "." + stat;

        collection.updateOne({ ehm_id: dbPlayer.ehm_id }, { $set: { [key]: currentStatCsv } });
      }
    };

    var getDoc = async (obj) => {
      return new Promise((resolve, reject) => {
        collection.findOne({ ehm_id: obj.Id }).then((doc) => {
          resolve(doc);
        })
      }).catch((err) => {
        reject(err);
      });
    }

    var updateDoc = async (csvDoc, dbPlayer) => {
      var csvPlayer = initPlayer(csvDoc);
      var categories = [ "general", "stats.hidden", "stats.positional", "stats.mental", "stats.physical", "stats.technical" ];

      for (let i = 0; i < categories.length; i++) {
        var category = categories[i]

        for (let stat in csvPlayer[category]) {
          // stats with array values tract stat history
          if (Array.isArray(csvPlayer[category][stat])) {
            processStatArray({ csvPlayer: csvPlayer, dbPlayer: dbPlayer, category: category, stat: stat });
          } else {
            processStat({ csvPlayer: csvPlayer, dbPlayer: dbPlayer, category: category, stat: stat });
          }

        }
      }
    };

    var insertDoc = async (obj) => {
      return new Promise((resolve, reject) => {
        collection.insert(initPlayer(obj)).then((doc) => {
          console.log("inserting.");
          resolve();
        })
      }).catch((err) => {
        reject(err);
      });
    }

    csv({ delimiter: ";" })
    .fromFile(csvFilePath)
    .on('json', async (jsonObj) => {
      var doc = await getDoc(jsonObj);

      if (doc) {
        await updateDoc(jsonObj, doc);
      } else {
        await insertDoc(jsonObj);
      }
    })
    .on('done', (error) => {
      console.log('Closed Mongo Connection.');
      // db.close();
    });
  }).catch((err) => {
    console.log("Mongo Error: ", err);
  });
};

run();
