# CSV to JSON Uploader (Node.js + PostgreSQL)

This is a Node.js-based API that reads a CSV file, converts each row into a deeply nested JSON object, and stores it in a PostgreSQL database. After importing, it prints the age group distribution to the console.

---

## Running the server

- npm install
- node index.js
- GET http://localhost:3000/upload


## Sample CSV format

name.firstName,name.lastName,age,address.line1,address.line2,address.city,address.state,gender,nickname.first.home.hey<br>
Rohit,Prasad,35,A-563 Rakshak Society,New Pune Road,Pune,Maharashtra,male,roho
