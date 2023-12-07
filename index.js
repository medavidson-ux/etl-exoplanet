const { promisify } = require("util");
const fs = require("fs");
const writeFilePromised = promisify(fs.writeFile);

// getAllPlanets().then((planets) => console.log(planets));

const EXO_API_URL =
  "https://exoplanetarchive.ipac.caltech.edu/cgi-bin/nstedAPI/nph-nstedAPI?table=mission_exocat&format=json";

const getAllPlanets = async () => {
  const response = await fetch(EXO_API_URL);
  return await response.json();
};

function transformOnePlanet(planet) {
  return {
    name: planet.star_name,
    otherPlanetsInSystem: planet.st_ppnum,
    distanceToSystem_Parsecs: planet.st_dist,
    distanceError: computeErrorThreshold(
      planet.st_disterr1,
      planet.st_disterr2
    ),
  };
}

function computeErrorThreshold(err1, err2) {
  if (err1 === null || err2 === null) {
    return null;
  }
  if (Math.abs(err1) === Math.abs(err2)) {
    return `\u00B1${Math.abs(err1)}`;
  } else {
    const max = Math.max(err1, err2);
    const min = Math.min(err1, err2);
    return `+${max}/${min}`;
  }
}

function bulkLoadPlanetsToFile(planets, outputFilePath) {
  if (!outputFilePath) {
    throw new Error("File path required as second argument.");
  }
  return writeFilePromised(outputFilePath, JSON.stringify(planets, null, 2));
}

function insertRecord(record) {
  //return a Promise that resolves when the record wws inserted
  //mock function
  return Promise.resolve();
}

const startEtlPipeline = async () => {
  const outputFile = `${__dirname}/out.json`;
  try {
    //Extract planets

    const planets = await getAllPlanets();
    console.log(planets[1]);
    console.log(`Extracted ${planets.length} planets from the API.`);
    //multiple extractions
    // const allExtractPromises = Promise.all([
    //   getAllPlanets(),
    //   getAllPlanets(),
    //   getAllPlanets(),
    // ]);
    // const [dataA, dataB, dataC] = await allExtractPromises;
    // console.log(dataA.length, dataB.length, dataC.length);

    //Transform data

    const planetsTransformed = planets.map((planet) =>
      transformOnePlanet(planet)
    );

    //TODO Load step
    // bulk records
    await bulkLoadPlanetsToFile(planetsTransformed, outputFile);
    // individual records
    // await planetsTransformed.reduce(async (previousPromise, planet) => {
    //   await previousPromise;
    //   return insertRecord(planet);
    // }, Promise.resolve());

    console.log("Loaded planets successfully.");
  } catch (err) {
    console.log(err);
  }
};

startEtlPipeline();
