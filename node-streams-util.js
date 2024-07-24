import { Readable, Writable } from "node:stream";
import db from "./database.js";

// Function to fetch data from the database
const fetchData = (offset, chunkSize) =>
  new Promise((resolve, reject) => {
    db.all(
      "SELECT title, release_date, tagline FROM movies LIMIT ? OFFSET ?",
      [chunkSize, offset],
      (err, rows) => (err ? reject(err) : resolve(rows))
    );
  });

// Create a readable stream
const CHUNK_SIZE = 10;
let offset = 0;

export const createMovieReadableStream = new Readable({
  objectMode: true,
  async read() {
    try {
      const rows = await fetchData(offset, CHUNK_SIZE);
      if (rows.length) {
        rows.forEach((row) => this.push(row));
        offset += CHUNK_SIZE;
      } else {
        this.push(null); // End of stream
      }
    } catch (err) {
      this.destroy(err);
    }
  },
});

export const createMovieWritableStream = () => {
  return new Writable({
    objectMode: true,
    write(movie, encoding, callback) {
      console.log(movie); // Process the movie data here
      callback();
    },
  });
};
