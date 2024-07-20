import { bcs } from "@mysten/bcs";
import fs from "fs";

// Generate a list of 511 instances of the integer 255
function generateIntArray(length: number, value: number): Array<number> {
    return Array(length).fill(value);
}

// Define a vector of u8
const vectorU8 = bcs.vector(bcs.u8());

// Define a vector of vectors of u8
const vectorOfVectorU8 = bcs.vector(vectorU8);

// Sample data: vector of vectors of u8
const data = [
    generateIntArray(511, 255),
    generateIntArray(511, 255),
    generateIntArray(511, 255),
];

// Serialize the data
const serializedData = vectorOfVectorU8.serialize(data).toBytes();
console.log(serializedData);

// Save serializedData to a JSON file
fs.writeFileSync(
    "data.json",
    JSON.stringify(Array.from(serializedData), null, 2),
);

// const deserializedData = vectorOfVectorU8.parse(serializedData);
// console.log(deserializedData);
