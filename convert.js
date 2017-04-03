const csvFilePath = './csv/players.csv';
const csv = require('csvtojson');
const MongoClient = require('mongodb').MongoClient;
const url = 'mongodb://localhost:27017/ehm';

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
  player.estimated_value = [ doc['Estimated Value'] ];
  player.club_playing = [ doc['Club Playing'] ];
  player.division_playing = [ doc['Division Playing'] ];
  player.club_contracted = [ doc['Club Contracted'] ];
  player.positions_short = doc['Positions Short'];
  player.stanley_cups_won = doc['Stanley Cups Won'];
  player.birth_town = doc['Birth Town'];
  player.date_of_birth = doc['Date Of Birth'];
  player.age = doc.Age;
  player.current_ability = [ doc['Current Ability'] ];
  player.potential_ability = doc['Potential Ability'];
  player.home_reputation = [ doc['Home Reputation'] ];
  player.current_reputation = [ doc['Current Reputation'] ];
  player.world_reputation = [ doc['World Reputation'] ];
  player.player_roles = doc['Player Roles'];
  player.handedness = doc.Handedness;
  player.junior_preference = doc['Junior Preference'];
  player.defensive_role = [ doc['Defensive Role'] ];
  player.offensive_role = [ doc['Offensive Role'] ];
  player.morale = [ doc.Morale ];
  player.favorite_number = doc['Favorite Number'];
  player.squad_number = [ doc['Squad Number'] ];
  player.int = [ doc.Int ];

  player.stats = {};
  player.stats.adaptability = [ doc.Adaptability ];
  player.stats.ambition = [ doc.Ambition ];
  player.stats.determination = [ doc.Determination ];
  player.stats.loyalty = [ doc.Loyalty ];
  player.stats.pressure = [ doc.Pressure ];
  player.stats.professionalism = [ doc.Professionalism ];
  player.stats.sportsmanship = [ doc.Sportsmanship ];
  player.stats.temperament = [ doc.Temperament ];
  player.stats.goaltender = [ doc.Goaltender ];
  player.stats.left_defence = [ doc['Left Defence'] ];
  player.stats.right_defence = [ doc['Right Defence'] ];
  player.stats.left_wing = [ doc['Left Wing'] ];
  player.stats.right_wing = [ doc['Right Wing'] ];
  player.stats.center = [ doc.Center ];
  player.stats.aggression = [ doc.Aggression ];
  player.stats.anticipation = [ doc.Anticipation];
  player.stats.bravery = [ doc.Bravery ];
  player.stats.consistency = [ doc.Consistency ];
  player.stats.decisions = [ doc.Decisions ];
  player.stats.dirtiness = [ doc.Dirtiness];
  player.stats.flair = [ doc.Flair ];
  player.stats.important_matches = [ doc['Important Matches'] ];
  player.stats.influence = [ doc.Influence ];
  player.stats.pass_tendency = [ doc['Pass Tendency'] ];
  player.stats.teamwork = [ doc.Teamwork ];
  player.stats.creativity = [ doc.Creativity ];
  player.stats.work_rate = [ doc['Work Rate'] ];
  player.stats.acceleration = [ doc.Acceleration ];
  player.stats.agility = [ doc.Agility ];
  player.stats.balance = [ doc.Balance ];
  player.stats.fighting = [ doc.Fighting ];
  player.stats.hitting = [ doc.Hitting ];
  player.stats.injury_proneness = [ doc['Injury Proneness'] ];
  player.stats.natural_fitness = [ doc['Natural Fitness'] ];
  player.stats.speed = [ doc.Speed ];
  player.stats.stamina = [ doc.Stamina ];
  player.stats.strength = [ doc.Strength ];
  player.stats.agitation = [ doc.Agitation ];
  player.stats.checking = [ doc.Checking ];
  player.stats.deflections = [ doc.Deflections ];
  player.stats.deking = [ doc.Deking ];
  player.stats.faceoffs = [ doc.Faceoffs ];
  player.stats.off_the_puck = [ doc['Off The Puck'] ];
  player.stats.one_on_ones = [ doc['One On Ones'] ];
  player.stats.passing = [ doc.Passing ];
  player.stats.pokecheck = [ doc.Pokecheck ];
  player.stats.positioning = [ doc.Positioning ];
  player.stats.slapshot = [ doc.Slapshot ];
  player.stats.stickhandling = [ doc.Stickhandling ];
  player.stats.versatility = [ doc.Versatility ];
  player.stats.wristshot = [ doc.Wristshot ];
  player.stats.blocker = [ doc.Blocker ];
  player.stats.glove = [ doc.Glove ];
  player.stats.rebound_control = [ doc['Rebound Control'] ];
  player.stats.recovery = [ doc.Recovery ];
  player.stats.reflexes = [ doc.Reflexes ];

  return player;
}

var run = () => {
  MongoClient.connect(url, (err, db) => {
    console.log("Connected to Mongo Instance.");

    var insertDocument = (obj) => {
      return new Promise((resolve, reject) => {
        var collection = db.collection('Player');

        // test if document already exists
        collection.findOne({ Id: obj.Id }, (err, doc) => {
          if (err) console.log(err);

          if (doc) {
            console.log(doc);
            resolve()
          } else {
            var player = initPlayer(obj);
            collection.insert(player, (err, doc) => {
              if (err) console.log(err);
              console.log(doc);
              resolve()
            });
          }
        });
      })
    }

    csv({ delimiter: ";" })
    .fromFile(csvFilePath)
    .on('json', async (jsonObj) => {
      await insertDocument(jsonObj);
    })
    .on('done', (error) => {
      console.log('end');
      db.close();
    });

  });
}

run();
