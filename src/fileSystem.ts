import fs from "fs";

export function writeJSON<T>(fileName: string, content: T): Promise<T> {
  return new Promise((resolve, reject) => {
    fs.writeFile(
      "./message-broker/" + fileName,
      JSON.stringify(content, undefined, 2),
      (err) => {
        if (err) return reject(err);
        resolve(content);
      }
    );
  });
}

export function readJSON<T>(fileName: string): Promise<null | T> {
  return new Promise((resolve, reject) => {
    fs.readFile("./message-broker/" + fileName, (err, data) => {
      if (err) {
        if (err.code === "ENOENT") return resolve(null);
        else return reject(err);
      }
      resolve(JSON.parse(data.toString()));
    });
  });
}
