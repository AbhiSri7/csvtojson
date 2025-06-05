// Modules
const express = require("express");
const fs = require("fs");
const path = require("path");
const { Pool } = require("pg");
const csv = require("csv-parser");
require("dotenv").config();

const app = express();
const PORT = process.env.PORT || 3000;
const FILE_PATH = process.env.CSV_FILE_PATH;
const DB_URL = process.env.DATABASE_URL;

if (!FILE_PATH || !DB_URL) { //check file and connection string availability in .env
  console.error("CSV_FILE_PATH or DATABASE_URL missing in .env");
  process.exit(1);
}

const db = new Pool({ connectionString: DB_URL }); //setup pool connection

function insertNestedKey(obj, dottedPath, value) {
  const keys = dottedPath.split(".");
  return keys.reduce((acc, key, index) => {
    if (index === keys.length - 1) {
      acc[key] = value;
    } else {
      acc[key] = acc[key] || {};
    }
    return acc[key];
  }, obj);
}

function isolateExtras(rowData, baseKeys) { // handle additional fields
  const extraFields = {};
  for (let key in rowData) {
    if (!baseKeys.includes(key)) {
      insertNestedKey(extraFields, key, rowData[key]);
    }
  }
  return extraFields;
}

function buildNestedObject(flatData) { //extract nested object from row
  const result = {};
  for (let prop in flatData) {
    insertNestedKey(result, prop, flatData[prop]);
  }
  return result;
}

async function computeAgeStatistics() {
  const ageGroups = {
    under20: 0,
    from20to40: 0,
    from40to60: 0,
    above60: 0
  };

  try {
    const data = await db.query("SELECT age FROM public.users");
    const totalUsers = data.rowCount;
    if (totalUsers === 0) return console.log("No users to calculate stats.");

    for (let { age } of data.rows) {
      if (typeof age !== 'number' || isNaN(age)) continue;
      if (age < 20) ageGroups.under20++;
      else if (age <= 40) ageGroups.from20to40++;
      else if (age <= 60) ageGroups.from40to60++;
      else ageGroups.above60++;
    }

    console.log("\nAge-Group - % Distribution");
    console.log(`< 20 - ${(ageGroups.under20 / totalUsers * 100).toFixed(2)}`);
    console.log(`20 to 40 - ${(ageGroups.from20to40 / totalUsers * 100).toFixed(2)}`);
    console.log(`40 to 60 - ${(ageGroups.from40to60 / totalUsers * 100).toFixed(2)}`);
    console.log(`> 60 - ${(ageGroups.above60 / totalUsers * 100).toFixed(2)}`);
  } catch (err) {
    console.error("Error generating age group report:", err);
  }
}

app.get("/upload", async (req, res) => {
  const requiredFields = ["name.firstName", "name.lastName", "age"];

  try {
    if (!fs.existsSync(FILE_PATH)) {
      return res.status(400).send("CSV file not found.");
    }

    const csvStream = fs.createReadStream(path.resolve(FILE_PATH)).pipe(csv());

    for await (const flatRow of csvStream) {
      try {
        const structuredData = buildNestedObject(flatRow);
        const first = structuredData.name?.firstName;
        const last = structuredData.name?.lastName;
        const ageValue = parseInt(structuredData.age, 10);

        if (!first || !last || isNaN(ageValue)) {
          console.warn("Skipping invalid row:", flatRow);
          continue;
        }

        const fullName = `${first}${last}`;
        const addressObj = structuredData.address || null;
        const extraProps = isolateExtras(
          flatRow,
          [...requiredFields, ...Object.keys(addressObj || {}).map(k => `address.${k}`)]
        );

        console.log(
            "Query values: ",
            JSON.stringify([fullName, ageValue, addressObj, extraProps])
        );

        await db.query(
          `INSERT INTO public.users ("name", age, address, additional_info) VALUES ($1, $2, $3, $4)`,
          [fullName, ageValue, addressObj, extraProps]
        );
      } catch (insertErr) {
        console.error("Row processing failed:", flatRow, insertErr);
      }
    }

    await computeAgeStatistics();
    res.send("CSV upload and import finished.");
  } catch (mainErr) {
    console.error("Upload failed:", mainErr);
    res.status(500).send("Upload error.");
  }
});

app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});
