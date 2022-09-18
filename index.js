const MongoClient = require("mongodb").MongoClient;
const fs = require("fs");
const mongoClient = new MongoClient("mongodb://localhost:27017/");
const first = JSON.parse(fs.readFileSync("first.json"));
const second = JSON.parse(fs.readFileSync("second.json"));
const insertMany = async (database, collectionName, collectionData) =>
  await database.collection(collectionName).insertMany(collectionData);
const pipeline = [
  {
    $unwind: {
      path: "$location.ll",
    },
  },
  {
    $group: {
      _id: "$_id",
      country: {
        $first: "$country",
      },
      students: {
        $first: "$students",
      },
      longitude: {
        $first: "$location.ll",
      },
      latitude: {
        $last: "$location.ll",
      },
    },
  },
  {
    $lookup: {
      from: "second",
      localField: "country",
      foreignField: "country",
      as: "overallStudents",
    },
  },
  {
    $unwind: {
      path: "$overallStudents",
    },
  },
  {
    $unwind: {
      path: "$students",
    },
  },
  {
    $group: {
      _id: "$_id",
      country: {
        $first: "$country",
      },
      currentStudents: {
        $sum: "$students.number",
      },
      overallStudents: {
        $max: "$overallStudents",
      },
      longitude: {
        $first: "$longitude",
      },
      latitude: {
        $last: "$latitude",
      },
    },
  },

  {
    $project: {
      _id: "$country",
      allDiffs: {
        $subtract: ["$overallStudents.overallStudents", "$currentStudents"],
      },
      longitude: 1,
      latitude: 1,
    },
  },
  {
    $group: {
      _id: "$_id",
      allDiffs: {
        $push: "$allDiffs",
      },
      count: {
        $count: {},
      },
      longitude: {
        $push: "$longitude",
      },
      latitude: {
        $push: "$latitude",
      },
    },
  },
  {
    $out: "third",
  },
];

async function run() {
  try {
    await mongoClient.connect();
    const db = mongoClient.db("mongoTest");
    await insertMany(db, "first", first);
    await insertMany(db, "second", second);
    await db.collection("first").aggregate(pipeline).toArray();
  } catch (err) {
    console.log(err);
  } finally {
    await mongoClient.close();
  }
}
run();
