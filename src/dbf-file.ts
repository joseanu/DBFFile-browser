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
    let buffer = new Uint8Array(0);

    // If there is a memo file, get the block size. Also get the total file size for overflow checking.
    // The code below assumes the block size is at offset 4 in the .dbt for dBase IV files, and defaults to 512 if
    // all zeros. For dBase III files, the block size is always 512 bytes.
    let memoBlockSize = 0;
    let memoView: Uint8Array | undefined;
    if (dbf._memoData) {
      if (dbf._version === 0x30 || dbf._version === 0xf5) {
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
    const decoderCache = new Map<string, TextDecoder>();
    const decodeBytes = (bytes: Uint8Array, enc: string) => {
      let decoder = decoderCache.get(enc);
      if (!decoder) {
        decoder = new TextDecoder(enc);
        decoderCache.set(enc, decoder);
      }
      return decoder.decode(bytes);
    };
    const decodeAt = (start: number, length: number, enc: string) =>
      decodeBytes(buffer.subarray(start, start + length), enc);

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
            case "C": {
              let effectiveLen = len;
              while (
                effectiveLen > 0 &&
                buffer[offset + effectiveLen - 1] === 0x20
              ) {
                --effectiveLen;
              }
              value =
                effectiveLen > 0
                  ? decodeAt(offset, effectiveLen, encoding)
                  : "";
              offset += field.size;
              break;
            }
            case "N": // Number
            case "F": {
              const text = decodeAt(offset, field.size, encoding).trim();
              if (!text) {
                value = 0;
              } else {
                const parsed = Number.parseFloat(text);
                value = Number.isNaN(parsed) ? 0 : parsed;
              }
              offset += field.size;
              break;
            }
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
                  buffer.byteOffset + offset,
                  4,
                ).getInt32(0, true);
                const msSinceMidnight =
                  new DataView(
                    buffer.buffer,
                    buffer.byteOffset + offset + 4,
                    4,
                  ).getInt32(0, true) + 1;
                value = parseVfpDateTime({ julianDay, msSinceMidnight });
              }
              offset += 8;
              break;
            case "D": // Date
              value =
                buffer[offset] === 0x20
                  ? null
                  : parse8CharDate(decodeAt(offset, 8, encoding));
              offset += 8;
              break;
            case "B": // Double
              value = new DataView(
                buffer.buffer,
                buffer.byteOffset + offset,
                field.size,
              ).getFloat64(0, true);
              offset += field.size;
              break;
            case "I": // Integer
              value = new DataView(
                buffer.buffer,
                buffer.byteOffset + offset,
                field.size,
              ).getInt32(0, true);
              offset += field.size;
              break;
            case "M": {
              let blockIndex =
                dbf._version === 0x30
                  ? new DataView(
                      buffer.buffer,
                      buffer.byteOffset + offset,
                      len,
                    ).getInt32(0, true)
                  : parseInt(decodeAt(offset, len, encoding));
              offset += len;
              if (isNaN(blockIndex) || blockIndex === 0) {
                value = null;
                break;
              }

              if (!memoView) {
                if (dbf._readMode === "strict") {
                  throw new Error(`Error reading memo file (read past end).`);
                }
                continue;
              }

              if (memoBlockSize <= 0) {
                throw new Error(`Invalid memo block size ${memoBlockSize}.`);
              }

              const ensureRange = (absoluteOffset: number, length: number) => {
                if (
                  absoluteOffset < 0 ||
                  absoluteOffset >= memoView!.byteLength ||
                  absoluteOffset + length > memoView!.byteLength
                ) {
                  throw new Error(`Error reading memo file (read past end).`);
                }
              };

              const readInt32 = (
                absoluteOffset: number,
                littleEndian: boolean,
              ) => {
                ensureRange(absoluteOffset, 4);
                return new DataView(
                  memoView!.buffer,
                  memoView!.byteOffset + absoluteOffset,
                  4,
                ).getInt32(0, littleEndian);
              };

              const readUint32 = (
                absoluteOffset: number,
                littleEndian: boolean,
              ) => {
                ensureRange(absoluteOffset, 4);
                return new DataView(
                  memoView!.buffer,
                  memoView!.byteOffset + absoluteOffset,
                  4,
                ).getUint32(0, littleEndian);
              };

              const collectBytes = (
                startBlockIndex: number,
                skipFirstBytes: number,
                totalLength: number,
              ) => {
                if (totalLength <= 0) return new Uint8Array(0);
                const startOffset =
                  startBlockIndex * memoBlockSize + skipFirstBytes;
                ensureRange(startOffset, totalLength);
                return memoView!.subarray(
                  startOffset,
                  startOffset + totalLength,
                );
              };

              const blockOffset = blockIndex * memoBlockSize;
              ensureRange(blockOffset, 0);

              if (dbf._version === 0x83) {
                const terminator = memoView.indexOf(0x1a, blockOffset);
                const end =
                  terminator === -1 ? memoView.byteLength : terminator;
                const memoBytes = memoView.subarray(blockOffset, end);
                const decoded = decodeBytes(memoBytes, encoding);
                value = decoded
                  .replace(/\r\n/g, "\n")
                  .replace(/\n(?!$)/g, "\n                ");
                break;
              }

              if (dbf._version === 0x8b) {
                let memoLength = readUint32(blockOffset + 4, true) - 8;
                if (memoLength < 0) memoLength = 0;
                const memoBytes = collectBytes(blockIndex, 8, memoLength);
                value = decodeBytes(memoBytes, encoding);
                break;
              }

              if (dbf._version === 0x30 || dbf._version === 0xf5) {
                const memoType = readInt32(blockOffset, false);
                if (memoType !== 1) {
                  value = "";
                  break;
                }
                let memoLength = readInt32(blockOffset + 4, false);
                if (memoLength < 0) memoLength = 0;
                const memoBytes = collectBytes(blockIndex, 8, memoLength);
                value = decodeBytes(memoBytes, encoding);
                break;
              }

              throw new Error(
                `Reading version ${dbf._version} memo fields is not supported.`,
              );
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
