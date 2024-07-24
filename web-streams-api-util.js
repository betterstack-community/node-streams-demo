import db from "./database.js";

// Function to fetch data from the database
const fetchData = (offset, chunkSize) => {
  return new Promise((resolve, reject) => {
    db.all(
      "SELECT title, release_date, tagline FROM movies LIMIT ? OFFSET ?",
      [chunkSize, offset],
      (err, rows) => {
        if (err) {
          reject(err);
        } else {
          resolve(rows);
        }
      }
    );
  });
};

// Function to create a readable stream from the database
export const createMovieReadableStream = () => {
  let offset = 0;
  const CHUNK_SIZE = 10;

  return new ReadableStream({
    async pull(controller) {
      try {
        const rows = await fetchData(offset, CHUNK_SIZE);
        if (rows.length > 0) {
          rows.forEach((row) => controller.enqueue(row));
          offset += CHUNK_SIZE;
        } else {
          controller.close(); // Signal the end of the stream
        }
      } catch (err) {
        controller.error(err);
      }
    },
    cancel(reason) {
      console.log("Stream cancelled:", reason);
    },
  });
};

// Writable stream to process the output
export const createMovieWritableStream = () => {
  return new WritableStream({
    write(movie) {
      console.log(movie);
    },
    close() {
      console.log("Stream ended");
    },
    abort(err) {
      console.error("Stream error:", err);
    },
  });
};
