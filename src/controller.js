const fs = require('fs');
const { parse } = require('fast-csv');
const { pipeline, Transform } = require('stream');
const Records = require('./records.model');
const util = require('util');

const BATCH_SIZE = 1000;
const asyncPipeline = util.promisify(pipeline)

const upload = async (req, res) => {
    const { file } = req;

    /* Acá va tu código! Recordá que podés acceder al archivo desde la constante file */
    if (!file) {
        return res.status(400).json({
            error: 'No file uploaded'
        });
    }

    console.time('Total processing time');
    console.time('Read and parse file time');

    // Check if the file is a CSV
    if (file.mimetype !== 'text/csv' && file.mimetype !== 'application/vnd.ms-excel') {
        return res.status(400).json({
            error: 'Invalid file type. Only CSV files are allowed.'
        });
    }

    let total = 0;
    let batch = []

    // Create a Transform stream to handle batch writing to MongoDB
    // This will read the CSV file, parse it, and write to MongoDB in batches
    // Using Transform stream to handle backpressure and batch processing
    // This allows to process large files without running out of memory
    const mongoBatchWriter = new Transform({
        objectMode: true,
        transform: async (record, _, callback) => {
            batch.push(record)

            if (batch.length >= BATCH_SIZE) {
                console.time("InsertMany (batch) time");
                try {
                    await Records.insertMany(batch, { ordered: false });
                    total += batch.length;
                    console.log(`Inserted ${total} records...`);
                    batch = [];
                    callback();
                } catch (err) {
                    console.error('Error inserting batch:', err);
                    return callback(err);
                }

                console.timeEnd("InsertMany (batch) time");

            } else {
                callback();
            }
        },

        // Flush method to handle any remaining records in the batch when the stream ends
        flush: async (callback) => {
            if (batch.length > 0) {
                console.time("InsertMany (final batch) time");
                try {
                    await Records.insertMany(batch, { ordered: false });
                    total += batch.length;
                    console.log(`Inserted final batch of ${batch.length} records. Total: ${total}`);
                } catch (err) {
                    console.error('Error inserting final batch:', err);
                    return callback(err);
                }
                console.timeEnd("InsertMany (final batch) time");
            }
            callback();
        }
    })

    // Use the pipeline to read the file, parse it, and write to MongoDB in batches
    try {
        await asyncPipeline(
            fs.createReadStream(file.path),
            parse({ headers: true }),
            mongoBatchWriter
        );

        console.timeEnd('Read and parse file time');
        console.timeEnd('Total processing time');

        // Clean up the file after processing
        fs.unlink(file.path, (err) => {
            if (err) {
                console.error('Error deleting file:', err);
            } else {
                console.log('File deleted successfully');
            }
        })

        return res.status(200).json({
            message: `File processed successfully. Total records inserted: ${total}`
        });

    } catch (err) {
        console.error('Error processing file:', err);
        console.timeEnd('Total processing time');
        return res.status(500).json({ error: 'Error processing file' });
    }
};

const list = async (_, res) => {
    try {
        const data = await Records
            .find({})
            .limit(10)
            .lean();

        return res.status(200).json(data);
    } catch (err) {
        return res.status(500).json(err);
    }
};

module.exports = {
    upload,
    list,
};
