const rp = require("request-promise-native");
const fsp = require("fs").promises;

const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

// Number of milliseconds to wait between API requests
// Set to 1 second by default to avoid throttling
const requestDelay = 1000;

// Start and end season for data collection
// Currently set to collect all box score data since the league's inception
const startYear = 1917;
const endYear = 2018;

writeAllData(startYear, endYear);

async function writeAllData(startYear, endYear) {
  await createDirectories();

  let currentYear = startYear;
  let gameNumber = 1;
  while (currentYear <= endYear) {
    let gameResult = await getBoxscore(
      `${currentYear}02${padGameNumber(gameNumber)}`
    );
    if (gameResult == null) {
      gameNumber = 1;
      currentYear++;
    } else {
      gameNumber++;
      let game = getGameOutput(gameResult);
      let officials = getOfficialOutput(gameResult);
      let gamelogs = getBoxscoreOutput(gameResult);
      let players = getPlayersOutput(gameResult);
      let plays = getPlaysOutput(gameResult);

      appendJsonToFile(game, `./games/${currentYear}`);
      appendJsonArrayToFile(officials, `./officials/${currentYear}`);
      appendJsonArrayToFile(gamelogs, `./boxscores/${currentYear}`);
      appendJsonArrayToFile(plays, `./plays/${currentYear}`);

      writeJsonArrayToFile(players, "id", "./players");
    }
    await delay(requestDelay);
  }
}

async function createDirectories() {
  const requiredFolders = [
    "boxscores/",
    "games/",
    "officials/",
    "players/",
    "plays/"
  ];

  for (const folder of requiredFolders) {
    try {
      await fsp.access(folder);
    } catch {
      await fsp.mkdir(folder);
    }
  }
}

function appendJsonArrayToFile(objArray, path) {
  objArray.forEach(obj => {
    appendJsonToFile(obj, path);
  });
}

async function appendJsonToFile(obj, path) {
  let done = false;
  while (!done) {
    try {
      await fsp.appendFile(path, JSON.stringify(obj) + "\n");
      done = true;
    } catch (error) {
      console.error(error);
    }
  }
}

function writeJsonArrayToFile(objArray, objElement, folder) {
  objArray.forEach(obj => {
    writeJsonToFile(obj, `${folder}/${obj[objElement]}`);
  });
}

async function writeJsonToFile(obj, path) {
  let done = false;
  while (!done) {
    try {
      await fsp.writeFile(path, JSON.stringify(obj));
      done = true;
    } catch (error) {
      console.error(error);
    }
  }
}

function getGameOutput(gameResult) {
  let gameOutput = {};
  gameOutput.datetime = gameResult.gameData.datetime;
  gameOutput.game = gameResult.gameData.game;
  gameOutput.teams = gameResult.gameData.teams;
  gameOutput.venue = gameResult.gameData.venue;
  gameOutput.decisions = gameResult.liveData.decisions;
  gameOutput.awayCoach = gameResult.liveData.boxscore.teams.away.coaches[0];
  gameOutput.homeCoach = gameResult.liveData.boxscore.teams.home.coaches[0];
  return gameOutput;
}

function getOfficialOutput(gameResult) {
  let officialsOutput = [];
  gameResult.liveData.boxscore.officials.forEach(official => {
    officialsOutput.push({
      game: gameResult.gameData.game.pk,
      official: official
    });
  });
  return officialsOutput;
}

function getBoxscoreOutput(gameResult) {
  let boxscoreOutput = [];
  for (const player in gameResult.liveData.boxscore.teams.home.players) {
    boxscoreOutput.push({
      game: gameResult.gameData.game.pk,
      player: gameResult.liveData.boxscore.teams.home.players[player],
      team: gameResult.liveData.boxscore.teams.home.team,
      location: "home"
    });
  }
  for (const player in gameResult.liveData.boxscore.teams.away.players) {
    boxscoreOutput.push({
      game: gameResult.gameData.game.pk,
      player: gameResult.liveData.boxscore.teams.away.players[player],
      team: gameResult.liveData.boxscore.teams.away.team,
      location: "away"
    });
  }

  return boxscoreOutput;
}

function getPlayersOutput(gameResult) {
  let playersOutput = [];
  for (const player in gameResult.gameData.players) {
    playersOutput.push(gameResult.gameData.players[player]);
  }
  return playersOutput;
}

function getPlaysOutput(gameResult) {
  let playsOutput = [];
  gameResult.liveData.plays.allPlays.forEach(play => {
    if (play.players) {
      play.players.forEach(player => {
        playsOutput.push({
          game: gameResult.gameData.game.pk,
          about: play.about,
          coordinates: play.coordinates,
          player: player,
          result: play.result,
          team: play.team
        });
      });
    } else {
      playsOutput.push({
        game: gameResult.gameData.game.pk,
        about: play.about,
        coordinates: play.coordinates,
        result: play.result,
        team: play.team
      });
    }
  });
  return playsOutput;
}

function padGameNumber(number) {
  if (number < 10) {
    return `000${number}`;
  } else if (number < 100) {
    return `00${number}`;
  } else if (number < 1000) {
    return `0${number}`;
  } else {
    return number;
  }
}

/**
 * Get the boxscore for a game from NHL.com APIs
 *
 * @param {Number} gameId The ID of the NHL game to retrieve
 * @returns {Object} A JSON formatted response if the game exists; null otherwise
 */
async function getBoxscore(gameId) {
  let response;
  while (!response) {
    try {
      response = await rp({
        simple: false,
        resolveWithFullResponse: true,
        url: `https://statsapi.web.nhl.com/api/v1/game/${gameId}/feed/live`,
        timeout: 1500
      });
      if (response.statusCode != 200 && response.statusCode != 404) {
        response = null;
      }
    } catch (error) {
      console.error(error);
    } finally {
      if (!response) await delay(requestDelay);
    }
  }
  return response.statusCode != 404 ? JSON.parse(response.body) : null;
}
