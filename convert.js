const csvFilePath = './csv/test_players.csv';
const csv         = require('csvtojson');
const _           = require('lodash');
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

  // general non-stat player properties, TODO: why are height and weight not exported?
  player._id = doc._id;
  player.ehm_id = doc.Id;
  player.name = [ { date: date, value: doc.Name } ];
  player.nation = [ { date: date, value: doc.Nation } ];
  player.second_nation = [ { date: date, value: doc['Second Nation'] } ];
  player.international_apps = [ { date: date, value: doc['International Apps'] } ];
  player.international_goals = [ { date: date, value: doc['International Goals'] } ];
  player.international_assists = [ { date: date, value: doc['International Assists'] } ];
  player.estimated_value = [ { date: date, value: doc['Estimated Value'] } ];
  player.club_playing = [ { date: date, value: doc['Club Playing'] } ];
  player.division_playing = [ { date: date, value: doc['Division Playing'] } ];
  player.club_contracted = [ { date: date, value: doc['Club Contracted'] } ];
  player.positions_short = [ { date: date, value: doc['Positions Short'] } ];
  player.stanley_cups_won = [ { date: date, value: doc['Stanley Cups Won'] } ];
  player.birth_town = [ { date: date, value: doc['Birth Town'] } ];
  player.date_of_birth = [ { date: date, value: doc['Date Of Birth'] } ];
  player.age = [ { date: date, value: doc.Age } ];
  player.current_ability = [ { date: date, value: doc['Current Ability'] } ];
  player.home_reputation = [ { date: date, value: doc['Home Reputation'] } ];
  player.current_reputation = [ { date: date, value: doc['Current Reputation'] } ];
  player.world_reputation = [ { date: date, value: doc['World Reputation'] } ];
  player.handedness = [ { date: date, value: doc.Handedness } ];
  player.junior_preference = [ { date: date, value: doc['Junior Preference'] } ];
  player.player_roles = [ { date: date, value: doc['Player Roles'] } ];
  player.defensive_role = [ { date: date, value: doc['Defensive Role'] } ];
  player.offensive_role = [ { date: date, value: doc['Offensive Role'] } ];
  // player.int = [ { date: date, value: doc.Int } ]; // TODO decide if I should track this stat, no idea what it's for
  player.morale = [ { date: date, value: doc.Morale } ];
  player.favorite_number = [ { date: date, value: doc['Favorite Number'] } ];
  player.squad_number = [ { date: date, value: doc['Squad Number'] } ];

  // stats hidden by the game, should probably not be revealed in UI
  player.loyalty = [ { date: date, value: doc.Loyalty } ];
  player.pressure = [ { date: date, value: doc.Pressure } ];
  player.professionalism = [ { date: date, value: doc.Professionalism } ];
  player.sportsmanship = [ { date: date, value: doc.Sportsmanship } ];
  player.temperament = [ { date: date, value: doc.Temperament } ];
  player.consistency = [ { date: date, value: doc.Consistency } ];
  player.decisions = [ { date: date, value: doc.Decisions } ];
  player.dirtiness = [ { date: date, value: doc.Dirtiness } ];
  player.potential_ability = [ { date: date, value: doc['Potential Ability'] } ];
  player.adaptability = [ { date: date, value: doc.Adaptability } ];
  player.ambition = [ { date: date, value: doc.Ambition } ];
  player.important_matches = [ { date: date, value: doc['Important Matches'] } ];
  player.pass_tendency = [ { date: date, value: doc['Pass Tendency'] } ];
  player.fighting = [ { date: date, value: doc.Fighting } ];
  player.injury_proneness = [ { date: date, value: doc['Injury Proneness'] } ];
  player.natural_fitness = [ { date: date, value: doc['Natural Fitness'] } ];
  player.agitation = [ { date: date, value: doc.Agitation } ];
  player.one_on_ones = [ { date: date, value: doc['One On Ones'] } ];
  player.versatility = [ { date: date, value: doc.Versatility } ];

  // positional stat ratings
  player.goaltender = [ { date: date, value: doc.Goaltender } ];
  player.left_defence = [ { date: date, value: doc['Left Defence'] } ];
  player.right_defence = [ { date: date, value: doc['Right Defence'] } ];
  player.left_wing = [ { date: date, value: doc['Left Wing'] } ];
  player.right_wing = [ { date: date, value: doc['Right Wing'] } ];
  player.center = [ { date: date, value: doc.Center } ];

  // stats labeled as 'mental' in game
  player.aggression = [ { date: date, value: doc.Aggression } ];
  player.anticipation = [ { date: date, value: doc.Anticipation } ];
  player.bravery = [ { date: date, value: doc.Bravery } ];
  player.creativity = [ { date: date, value: doc.Creativity } ];
  player.determination = [ { date: date, value: doc.Determination } ];
  player.flair = [ { date: date, value: doc.Flair } ];
  player.influence = [ { date: date, value: doc.Influence } ];
  player.teamwork = [ { date: date, value: doc.Teamwork } ];
  player.work_rate = [ { date: date, value: doc['Work Rate'] } ];

  // stats labeled as 'physical' in game
  player.acceleration = [ { date: date, value: doc.Acceleration } ];
  player.agility = [ { date: date, value: doc.Agility } ];
  player.balance = [ { date: date, value: doc.Balance } ];
  player.speed = [ { date: date, value: doc.Speed } ];
  player.stamina = [ { date: date, value: doc.Stamina } ];
  player.strength = [ { date: date, value: doc.Strength } ];

  // stats labeled as 'technical' in game for forwards and defencemen
  player.checking = [ { date: date, value: doc.Checking } ];
  player.deflections = [ { date: date, value: doc.Deflections } ];
  player.deking = [ { date: date, value: doc.Deking } ];
  player.faceoffs = [ { date: date, value: doc.Faceoffs } ];
  player.hitting = [ { date: date, value: doc.Hitting } ];
  player.off_the_puck = [ { date: date, value: doc['Off The Puck'] } ];
  player.passing = [ { date: date, value: doc.Passing } ];
  player.pokecheck = [ { date: date, value: doc.Pokecheck } ];
  player.positioning = [ { date: date, value: doc.Positioning } ];
  player.slapshot = [ { date: date, value: doc.Slapshot } ];
  player.stickhandling = [ { date: date, value: doc.Stickhandling } ];
  player.wristshot = [ { date: date, value: doc.Wristshot } ];

  // stats labeled as 'technical' in game for goalies
  player.blocker = [ { date: date, value: doc.Blocker } ];
  player.glove = [ { date: date, value: doc.Glove } ];
  player.rebound_control = [ { date: date, value: doc['Rebound Control'] } ];
  player.recovery = [ { date: date, value: doc.Recovery } ];
  player.reflexes = [ { date: date, value: doc.Reflexes } ];

  return player;
};

var run = async () => {
  MongoClient.connect(url).then(async (db) => {
    console.log("Connected to Mongo Instance.");
    var playerCollection = db.collection('Player');
    var dateCollection = db.collection('Date');


    var getDates = () => {
      return new Promise((resolve, reject) => {
        dateCollection.find({}).toArray().then((docs) => {
          resolve(docs);
        });
      })
    };

    var addDate = (date) => {
      return new Promise((resolve, reject) => {
        dateCollection.insert({ date: date }).then((doc) => {
          console.log("inserting date.");
          resolve();
        })
      })
    };


    var processStat = ({ csvPlayer, dbPlayer, stat }) => {
      console.log("-- processing:", stat);
      var currentStatCsv = csvPlayer[stat].slice(-1)[0];
      var currentStatDb = dbPlayer[stat].slice(-1)[0];

      if (currentStatCsv.value && currentStatCsv.value !== currentStatDb.value) {
        if (currentStatCsv.date !== currentStatDb.date) {
          console.log(stat + " stat changed: ", currentStatDb, " to ", currentStatCsv );

          playerCollection.updateOne({ ehm_id: dbPlayer.ehm_id }, { $push: { [stat]: { date: date, value: currentStatCsv.value } } });
        } else {
          console.log("duplicate date");
        }
      }
    };

    var getDoc = async (csvPlayer) => {
      return new Promise((resolve, reject) => {
        playerCollection.findOne({ ehm_id: csvPlayer.Id }).then((doc) => {
          resolve(doc);
        })
      }).catch((err) => {
        reject(err);
      });
    }

    var updateDoc = async (csvDoc, dbPlayer) => {
      var csvPlayer = initPlayer(csvDoc);

      for (let stat in dbPlayer) {
        if (Array.isArray(dbPlayer[stat])) {
          processStat({ csvPlayer: csvPlayer, dbPlayer: dbPlayer, stat: stat });
        } else {
          console.log("Not a processable field: ", stat);
        }
      }
    };

    var insertDoc = async (obj) => {
      var newPlayer = initPlayer(obj);

      console.log(newPlayer)
      return new Promise((resolve, reject) => {
        playerCollection.insert(newPlayer).then((doc) => {
          console.log("inserting.");
          resolve();
        })
      }).catch((err) => {
        reject(err);
      });
    }

    var validateDate = async (date) => {
      var dates = await getDates();

      console.log(dates);

      return true;
    }

    var dateIsValid = await validateDate(date);
    await addDate(date);

    // csv({ delimiter: ";" })
    // .fromFile(csvFilePath)
    // .on('json', async (csvPlayer) => {
    //   var dbPlayer = await getDoc(csvPlayer);
    //
    //   if (dbPlayer) {
    //     await updateDoc(csvPlayer, dbPlayer);
    //   } else {
    //     await insertDoc(csvPlayer);
    //   }
    // })
    // .on('done', (error) => {
    //   console.log('Closed Mongo Connection.');
    //   // db.close();
    // });
  }).catch((err) => {
    console.log("Mongo Error: ", err);
  });
};

run();
