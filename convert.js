const csvFilePath = './csv/test_players2.csv';
const csv = require('csvtojson');
const MongoClient = require('mongodb').MongoClient;
const url = 'mongodb://localhost:27017/ehm';
// const Promise = require("bluebird");
var argv = require('minimist')(process.argv.slice(2));

if (argv.date) {
  var date = argv.date;
} else {
  throw("Date Arg is Required! [eg: --date 12-31-1976]");
}

console.log("Argv: ", argv.date);

var initPlayer = (doc) => {
  var player = {};

  // array values exist to track history
  // player._id = doc._id;
  player.ehm_id = doc.Id;
  player.name = doc.Name;
  player.nation = doc.Nation;
  player.second_nation = doc['Second Nation'];
  player.international_apps = doc['International Apps'];
  player.international_goals = doc['International Goals'];
  player.international_assists = doc['International Assists'];
  player.estimated_value = [ { date: date, value: doc['Estimated Value'] } ];
  player.club_playing = [ { date: date, value: doc['Club Playing'] } ];
  player.division_playing = [ { date: date, value: doc['Division Playing'] } ];
  player.club_contracted = [ { date: date, value: doc['Club Contracted'] } ];
  player.positions_short = doc['Positions Short'];
  player.stanley_cups_won = doc['Stanley Cups Won'];
  player.birth_town = doc['Birth Town'];
  player.date_of_birth = doc['Date Of Birth'];
  player.age = doc.Age;
  player.current_ability = [ { date: date, value: doc['Current Ability'] } ];
  player.potential_ability = doc['Potential Ability'];
  player.home_reputation = [ { date: date, value: doc['Home Reputation'] } ];
  player.current_reputation = [ { date: date, value: doc['Current Reputation'] } ];
  player.world_reputation = [ { date: date, value: doc['World Reputation'] } ];
  player.player_roles = doc['Player Roles'];
  player.handedness = doc.Handedness;
  player.junior_preference = doc['Junior Preference'];
  player.defensive_role = [ { date: date, value: doc['Defensive Role'] } ];
  player.offensive_role = [ { date: date, value: doc['Offensive Role'] } ];
  player.morale = [ doc.Morale ];
  player.favorite_number = doc['Favorite Number'];
  player.squad_number = [ { date: date, value: doc['Squad Number'] } ];
  player.int = [ { date: date, value: doc.Int } ];

  player.stats = {};
  player.stats.adaptability = [ { date: date, value: doc.Adaptability } ];
  player.stats.ambition = [ { date: date, value: doc.Ambition } ];
  player.stats.determination = [ { date: date, value: doc.Determination } ];
  player.stats.loyalty = [ { date: date, value: doc.Loyalty } ];
  player.stats.pressure = [ { date: date, value: doc.Pressure } ];
  player.stats.professionalism = [ { date: date, value: doc.Professionalism } ];
  player.stats.sportsmanship = [ { date: date, value: doc.Sportsmanship } ];
  player.stats.temperament = [ { date: date, value: doc.Temperament } ];
  player.stats.goaltender = [ { date: date, value: doc.Goaltender } ];
  player.stats.left_defence = [ { date: date, value: doc['Left Defence'] } ];
  player.stats.right_defence = [ { date: date, value: doc['Right Defence'] } ];
  player.stats.left_wing = [ { date: date, value: doc['Left Wing'] } ];
  player.stats.right_wing = [ { date: date, value: doc['Right Wing'] } ];
  player.stats.center = [ { date: date, value: doc.Center } ];
  player.stats.aggression = [ { date: date, value: doc.Aggression } ];
  player.stats.anticipation = [ { date: date, value: doc.Anticipation } ];
  player.stats.bravery = [ { date: date, value: doc.Bravery } ];
  player.stats.consistency = [ { date: date, value: doc.Consistency } ];
  player.stats.decisions = [ { date: date, value: doc.Decisions } ];
  player.stats.dirtiness = [ { date: date, value: doc.Dirtiness } ];
  player.stats.flair = [ { date: date, value: doc.Flair } ];
  player.stats.important_matches = [ { date: date, value: doc['Important Matches'] } ];
  player.stats.influence = [ { date: date, value: doc.Influence } ];
  player.stats.pass_tendency = [ { date: date, value: doc['Pass Tendency'] } ];
  player.stats.teamwork = [ { date: date, value: doc.Teamwork } ];
  player.stats.creativity = [ { date: date, value: doc.Creativity } ];
  player.stats.work_rate = [ { date: date, value: doc['Work Rate'] } ];
  player.stats.acceleration = [ { date: date, value: doc.Acceleration } ];
  player.stats.agility = [ { date: date, value: doc.Agility } ];
  player.stats.balance = [ { date: date, value: doc.Balance } ];
  player.stats.fighting = [ { date: date, value: doc.Fighting } ];
  player.stats.hitting = [ { date: date, value: doc.Hitting } ];
  player.stats.injury_proneness = [ { date: date, value: doc['Injury Proneness'] } ];
  player.stats.natural_fitness = [ { date: date, value: doc['Natural Fitness'] } ];
  player.stats.speed = [ { date: date, value: doc.Speed } ];
  player.stats.stamina = [ { date: date, value: doc.Stamina } ];
  player.stats.strength = [ { date: date, value: doc.Strength } ];
  player.stats.agitation = [ { date: date, value: doc.Agitation } ];
  player.stats.checking = [ { date: date, value: doc.Checking } ];
  player.stats.deflections = [ { date: date, value: doc.Deflections } ];
  player.stats.deking = [ { date: date, value: doc.Deking } ];
  player.stats.faceoffs = [ { date: date, value: doc.Faceoffs } ];
  player.stats.off_the_puck = [ { date: date, value: doc['Off The Puck'] } ];
  player.stats.one_on_ones = [ { date: date, value: doc['One On Ones'] } ];
  player.stats.passing = [ { date: date, value: doc.Passing } ];
  player.stats.pokecheck = [ { date: date, value: doc.Pokecheck } ];
  player.stats.positioning = [ { date: date, value: doc.Positioning } ];
  player.stats.slapshot = [ { date: date, value: doc.Slapshot } ];
  player.stats.stickhandling = [ { date: date, value: doc.Stickhandling } ];
  player.stats.versatility = [ { date: date, value: doc.Versatility } ];
  player.stats.wristshot = [ { date: date, value: doc.Wristshot } ];
  player.stats.blocker = [ { date: date, value: doc.Blocker } ];
  player.stats.glove = [ { date: date, value: doc.Glove } ];
  player.stats.rebound_control = [ { date: date, value: doc['Rebound Control'] } ];
  player.stats.recovery = [ { date: date, value: doc.Recovery } ];
  player.stats.reflexes = [ { date: date, value: doc.Reflexes } ];

  return player;
};

var run = () => {
  MongoClient.connect(url, (err, db) => {
    console.log("Connected to Mongo Instance.");
    var collection = db.collection('Player');

    var getDoc = async (obj) => {
      return new Promise(function (resolve) {
        collection.findOne({ ehm_id: obj.Id }).then((doc) => {
          resolve(doc);
        });
      });
    }

    var updateDoc = async (csvDoc, dbPlayer) => {
      var csvPlayer = initPlayer(csvDoc);

      for (var key in csvPlayer.stats) {
        var currentStatCsv = csvPlayer.stats[key].slice(-1)[0];
        var currentStatDb = dbPlayer.stats[key].slice(-1)[0];

        if (currentStatCsv.value && currentStatCsv.value !== currentStatDb.value) {
          if (currentStatCsv.date !== currentStatDb.date) {
            console.log(key + " stat changed: ", currentStatDb, " to ", currentStatCsv );

            collection.updateOne({ ehm_id: dbPlayer.ehm_id }, { $push: { ["stats." + key]: { date: date, value: currentStatCsv.value } } });
          } else {
            console.log("duplicate date");
          }
        }
      }
    };

    var insertDoc = async (obj) => {
      collection.insert(initPlayer(obj)).then((doc) => {
        // console.log(doc);
      });
    };

    csv({ delimiter: ";" })
    .fromFile(csvFilePath)
    .on('json', async (jsonObj) => {
      var playerDoc = await getDoc(jsonObj);

      // console.log(playerDoc)
      if (playerDoc) {
        // console.log("Updating Doc")
        await updateDoc(jsonObj, playerDoc);
      } else {
        console.log("Inserting Doc")
        await insertDoc(jsonObj);
      }
    })
    .on('done', (error) => {
      console.log('Closed Mongo Connection.');
      // db.close();
    });

  });
}

run();
