import { FieldDescriptor, validateFieldDescriptor } from "./field-descriptor";
import { isValidFileVersion } from "./file-version";
import { Encoding, normaliseOpenOptions, OpenOptions } from "./options";
import { createDate, parseVfpDateTime, parse8CharDate } from "./utils";

/** Represents a DBF file. */
export class DBFFile {
  /** Opens an existing DBF file. */
  static async open(
    data: ArrayBuffer,
    memoData?: ArrayBuffer,
    options?: OpenOptions,
  ) {
    return openDBF(data, memoData, options);
  }

  /** DBF file. */
  data = new ArrayBuffer(0);

  /** Total number of records in the DBF file. (NB: includes deleted records). */
  recordCount = 0;

  /** Date of last update as recorded in the DBF file header. */
  dateOfLastUpdate!: Date;

  /** Metadata for all fields defined in the DBF file. */
  fields = [] as FieldDescriptor[];

  /**
   * Reads a subset of records from this DBF file. If the `includeDeletedRecords` option is set, then deleted records
   * are included in the results, otherwise they are skipped. Deleted records have the property `[DELETED]: true`,
   * using the `DELETED` symbol exported from this library.
   */
  readRecords(maxCount = 10000000) {
    return readRecordsFromDBF(this, maxCount);
  }

  /**
   * Iterates over each record in this DBF file. If the `includeDeletedRecords` option is set, then deleted records
   * are yielded, otherwise they are skipped. Deleted records have the property `[DELETED]: true`, using the `DELETED`
   * symbol exported from this library.
   */
  async *[Symbol.asyncIterator]() {
    while (this._recordsRead !== this.recordCount) {
      yield* await this.readRecords(100);
    }
  }

  // Private.
  _readMode = "strict" as "strict" | "loose";
  _encoding = "" as Encoding;
  _includeDeletedRecords = false;
  _recordsRead = 0;
  _headerLength = 0;
  _recordLength = 0;
  _memoData?: ArrayBuffer;
  _version? = 0;
}

/** Symbol used for detecting deleted records when the `includeDeletedRecords` option is used. */
export const DELETED = Symbol();

//-------------------- Private implementation starts here --------------------
async function openDBF(
  data: ArrayBuffer,
  memoData?: ArrayBuffer,
  opts?: OpenOptions,
): Promise<DBFFile> {
  let options = normaliseOpenOptions(opts);
  let buffer = new Uint8Array(data);
  let offset = 0;

  // Read various properties from the header record.
  let fileVersion = buffer[offset++];
  let lastUpdateY = buffer[offset++]; // number of years after 1900
  let lastUpdateM = buffer[offset++]; // 1-based
  let lastUpdateD = buffer[offset++]; // 1-based
  const dateOfLastUpdate = createDate(
    lastUpdateY + 1900,
    lastUpdateM,
    lastUpdateD,
  );
  let recordCount = new DataView(data.slice(offset, offset + 4)).getInt32(
    0,
    true,
  );
  offset += 4;
  let headerLength = new DataView(data.slice(offset, offset + 2)).getInt16(
    0,
    true,
  );
  offset += 2;
  let recordLength = new DataView(data.slice(offset, offset + 2)).getInt16(
    0,
    true,
  );
  offset += 2;

  // Validate the file version. Skip validation if reading in 'loose' mode.
  if (options.readMode !== "loose" && !isValidFileVersion(fileVersion)) {
    throw new Error(`Invalid file version: ${fileVersion}.`);
  }

  // Parse and validate all field descriptors. Skip validation if reading in 'loose' mode.
  offset = 32;
  let fields: FieldDescriptor[] = [];
  const encoding = getEncoding(options.encoding);
  const decoder = new TextDecoder(encoding);
  while (headerLength > offset) {
    if (buffer[offset] === 0x0d) break;
    let field: FieldDescriptor = {
      name: decoder
        .decode(new Uint8Array(buffer.slice(offset, offset + 11)))
        .split("\0")[0],
      type: String.fromCharCode(buffer[offset + 11]) as FieldDescriptor["type"],
      size: buffer[offset + 16],
      decimalPlaces: buffer[offset + 17],
    };
    offset += 32;
    if (options.readMode !== "loose") {
      validateFieldDescriptor(field, fileVersion);
      if (!fields.every((f) => f.name !== field.name)) {
        throw new Error(`Duplicate field name: ${field.name}.`);
      }
    }
    fields.push(field);
  }

  // Parse the header terminator.
  if (buffer[offset++] !== 0x0d) {
    throw new Error("Invalid DBF file: header terminator not found.");
  }

  // Validate the record length.
  const computedRecordLength = calculateRecordLengthInBytes(fields);
  if (options.readMode === "loose") recordLength = computedRecordLength;
  if (recordLength !== computedRecordLength) {
    throw new Error(
      `Invalid record length: ${recordLength}. Expected ${computedRecordLength}.`,
    );
  }

  // Return a new DBFFile instance.
  let result = new DBFFile();
  result.data = data;
  result.recordCount = recordCount;
  result.dateOfLastUpdate = dateOfLastUpdate;
  result.fields = fields;
  result._readMode = options.readMode;
  result._encoding = options.encoding;
  result._includeDeletedRecords = options.includeDeletedRecords;
  result._recordsRead = 0;
  result._headerLength = headerLength;
  result._recordLength = recordLength;
  result._memoData = memoData;
  result._version = fileVersion;
  return result;
}

// Private implementation of DBFFile#readRecords
async function readRecordsFromDBF(dbf: DBFFile, maxCount: number) {
  try {
    // Prepare to create a buffer to read through.
    let recordCountPerBuffer = Math.min(maxCount, 1000);
    let recordLength = dbf._recordLength;
    let buffer = new Uint8Array(
      dbf.data,
      dbf._headerLength,
      recordLength * recordCountPerBuffer,
    );

    // If there is a memo file, get the block size. Also get the total file size for overflow checking.
    // The code below assumes the block size is at offset 4 in the .dbt for dBase IV files, and defaults to 512 if
    // all zeros. For dBase III files, the block size is always 512 bytes.
    let memoBlockSize = 0;
    let memoView: Uint8Array | undefined;
    if (dbf._memoData) {
      if (dbf._version === 0x30 || 0xf5) {
        // FoxPro9
        memoBlockSize =
          new DataView(dbf._memoData.slice(6, 8)).getUint16(0) || 512;
      } else {
        // dBASE
        memoBlockSize =
          (dbf._version === 0x8b
            ? new DataView(dbf._memoData.slice(4, 8)).getInt32(0)
            : 0) || 512;
      }
      memoView = new Uint8Array(dbf._memoData);
    }

    // Create convenience functions for extracting values from the buffer.
    let substrAt = (start: number, len: number, enc: string) => {
      const decoder = new TextDecoder(enc);
      return decoder.decode(new Uint8Array(buffer.slice(start, start + len)));
    };

    // Read records in chunks, until enough records have been read.
    let records: Array<Record<string, unknown> & { [DELETED]?: true }> = [];
    while (dbf._recordsRead < dbf.recordCount && records.length < maxCount) {
      // Work out how many records to read in this chunk.
      let recordCountToRead = Math.min(
        dbf.recordCount - dbf._recordsRead,
        maxCount - records.length,
        recordCountPerBuffer,
      );

      // Quit when there are no more records to read.
      if (recordCountToRead === 0) break;

      // Read the chunk of records into the buffer.
      buffer = new Uint8Array(
        dbf.data,
        dbf._headerLength + dbf._recordsRead * recordLength,
        recordLength * recordCountToRead,
      );
      dbf._recordsRead += recordCountToRead;

      // Parse each record.
      for (let i = 0, offset = 0; i < recordCountToRead; ++i) {
        let record: Record<string, unknown> & { [DELETED]?: true } = {};
        let isDeleted = buffer[offset++] === 0x2a;
        if (isDeleted && !dbf._includeDeletedRecords) {
          offset += recordLength - 1;
          continue;
        }

        // Parse each field.
        for (let j = 0; j < dbf.fields.length; ++j) {
          let field = dbf.fields[j];
          let len = field.size;
          let value: any = null;
          let encoding = getEncoding(dbf._encoding, field);

          // Decode the field from the buffer, according to its type.
          switch (field.type) {
            case "C": // Text
              while (len > 0 && buffer[offset + len - 1] === 0x20) --len;
              value = substrAt(offset, len, encoding);
              offset += field.size;
              break;
            case "N": // Number
            case "F": // Float - appears to be treated identically to Number
              while (len > 0 && buffer[offset] === 0x20) ++offset, --len;
              value =
                len > 0 ? parseFloat(substrAt(offset, len, encoding)) || 0 : 0;
              offset += len;
              break;
            case "L": // Boolean
              let c = String.fromCharCode(buffer[offset++]);
              value =
                "TtYy".indexOf(c) >= 0
                  ? true
                  : "FfNn".indexOf(c) >= 0
                    ? false
                    : null;
              break;
            case "T": // DateTime
              if (buffer[offset] === 0x20) {
                value = null;
              } else {
                const julianDay = new DataView(
                  buffer.buffer,
                  offset,
                  4,
                ).getInt32(0, true);
                const msSinceMidnight =
                  new DataView(buffer.buffer, offset + 4, 4).getInt32(0, true) +
                  1;
                value = parseVfpDateTime({ julianDay, msSinceMidnight });
              }
              offset += 8;
              break;
            case "D": // Date
              value =
                buffer[offset] === 0x20
                  ? null
                  : parse8CharDate(substrAt(offset, 8, encoding));
              offset += 8;
              break;
            case "B": // Double
              value = new DataView(
                buffer.buffer,
                offset,
                field.size,
              ).getFloat64(0, true);
              offset += field.size;
              break;
            case "I": // Integer
              value = new DataView(buffer.buffer, offset, field.size).getInt32(
                0,
                true,
              );
              offset += field.size;
              break;
            case "M": // Memo
              let blockIndex =
                dbf._version === 0x30
                  ? new DataView(buffer.buffer, offset, len).getInt32(0, true)
                  : parseInt(substrAt(offset, len, encoding));
              offset += len;
              if (isNaN(blockIndex) || blockIndex === 0) {
                value = null;
                break;
              }

              // If the memo file is missing and we get this far, we must be in 'loose' read mode.
              // Skip reading the memo value and continue with the next field.
              if (!memoView) continue;

              // Start with an empty memo value, and concatenate to it until the memo value is fully read.
              value = "";
              let mergedBuffer = new Uint8Array(0);

              // Read the memo data from the memo file. We use a while loop here to read one block-sized
              // chunk at a time, since memo values can be larger than the block size.
              while (true) {
                // Read the next block-sized chunk from the memo file.
                let memoBuffer = memoView.subarray(
                  blockIndex * memoBlockSize,
                  (blockIndex + 1) * memoBlockSize,
                );

                // Handle first/next block of dBase III memo data.
                if (dbf._version === 0x83) {
                  // dBase III memos don't have a length header, rather they are terminated with two
                  // 0x1A bytes. However when FoxPro is used to modify a dBase III file, it writes
                  // only a single 0x1A byte to mark the end of a memo. Some files therefore have both
                  // markers (ie 0x1A1A and 0x1A) present in the same file for different records. This
                  // reader therefore only looks for a single 0x1A byte to mark the end of the memo,
                  // so that it picks up both dBase III and FoxPro variations. (Previously this code
                  // only checked for 0x1A1A in 0x83 files, and read past the end of the memo file
                  // for some user-submitted test files because it missed single 0x1A markers).
                  // If the terminator is not found in the current block-sized buffer, then the memo
                  // value must be larger than a single block size. In that case, we continue the loop
                  // and read the next block-sized chunk, and so on until the terminator is found.
                  let eos = memoBuffer.indexOf(0x1a);
                  mergedBuffer = new Uint8Array([
                    ...mergedBuffer,
                    ...memoBuffer.subarray(0, eos === -1 ? memoBlockSize : eos),
                  ]);
                  if (eos !== -1) {
                    const decoder = new TextDecoder(encoding);
                    value = decoder.decode(mergedBuffer);
                    break; // break out of the loop once we've found the terminator.
                  }
                }

                // Handle first/next block of dBase IV memo data.
                else if (dbf._version === 0x8b) {
                  // dBase IV memos start with FF-FF-08-00, then a four-byte memo length, which
                  // includes the eight-byte memo 'header' in the length. The memo length can be
                  // larger than a block, so we loop over blocks until done.

                  // If this is the first block of the memo, then read the field length.
                  // Otherwise, we must have already read the length in a previous loop iteration.
                  let isFirstBlockOfMemo =
                    new DataView(memoBuffer.buffer, 0, 4).getInt32(0, false) ===
                    0x0008ffff;
                  if (isFirstBlockOfMemo)
                    len =
                      new DataView(memoBuffer.buffer, 4, 4).getUint32(0, true) -
                      8;

                  // Read the chunk of memo data, and break out of the loop when all read.
                  let skip = isFirstBlockOfMemo ? 8 : 0;
                  let take = Math.min(len, memoBlockSize - skip);
                  mergedBuffer = new Uint8Array([
                    ...mergedBuffer,
                    ...memoBuffer.subarray(skip, skip + take),
                  ]);
                  len -= take;
                  if (len === 0) {
                    const decoder = new TextDecoder(encoding);
                    value = decoder.decode(mergedBuffer);
                    break;
                  }
                }

                // Handle first/next block of FoxPro9 memo data.
                else if (dbf._version === 0x30 || 0xf5) {
                  // Memo header
                  // 00 - 03: Next free block
                  // 04 - 05: Not used
                  // 06 - 07: Block size
                  // 08 - 511: Not used

                  // Memo Block
                  // 00 - 03: Type: 0 = image, 1 = text
                  // 04 - 07: Length
                  // 08 - N : Data

                  let skip = 0;
                  if (!mergedBuffer.length) {
                    const memoType = new DataView(
                      memoBuffer.buffer,
                      memoBuffer.byteOffset,
                      4,
                    ).getInt32(0, false);
                    if (memoType != 1) break;
                    len = new DataView(
                      memoBuffer.buffer,
                      memoBuffer.byteOffset + 4,
                      4,
                    ).getInt32(0, false);
                    skip = 8;
                  }

                  // Read the chunk of memo data, and break out of the loop when all read.
                  let take = Math.min(len, memoBlockSize - skip);
                  mergedBuffer = new Uint8Array([
                    ...mergedBuffer,
                    ...memoBuffer.subarray(skip, skip + take),
                  ]);
                  len -= take;
                  if (len === 0) {
                    const decoder = new TextDecoder(encoding);
                    value = decoder.decode(mergedBuffer);
                    break;
                  }
                } else {
                  throw new Error(
                    `Reading version ${dbf._version} memo fields is not supported.`,
                  );
                }
                ++blockIndex;
                if (blockIndex * memoBlockSize > dbf._memoData!.byteLength) {
                  throw new Error(`Error reading memo file (read past end).`);
                }
              }
              break;

            default:
              // Throw an error if reading in 'strict' mode
              if (dbf._readMode === "strict")
                throw new Error(`Type '${field.type}' is not supported`);

              // Skip over the field data if reading in 'loose' mode
              if (dbf._readMode === "loose") {
                offset += field.size;
                continue;
              }
          }
          record[field.name] = value;
        }

        // If the record is marked as deleted, add the `[DELETED]` flag.
        if (isDeleted) record[DELETED] = true;

        // Add the record to the result.
        records.push(record);
      }
    }

    // Return all the records that were read.
    return records;
  } catch (error) {
    throw error;
  }
}

// Private helper function
function calculateRecordLengthInBytes(fields: FieldDescriptor[]): number {
  let len = 1; // 'Record deleted flag' adds one byte
  for (let i = 0; i < fields.length; ++i) len += fields[i].size;
  return len;
}

// Private helper function
function getEncoding(encoding: Encoding, field?: FieldDescriptor) {
  if (typeof encoding === "string") return encoding;
  return encoding[field?.name ?? "default"] || encoding.default;
}
