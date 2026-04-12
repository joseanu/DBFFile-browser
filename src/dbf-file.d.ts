import { FieldDescriptor } from "./field-descriptor";
import { Encoding, OpenOptions } from "./options";
/** Represents a DBF file. */
export declare class DBFFile {
    /** Opens an existing DBF file. */
    static open(data: ArrayBuffer, memoData?: ArrayBuffer, options?: OpenOptions): Promise<DBFFile>;
    /** DBF file. */
    data: ArrayBuffer;
    /** Total number of records in the DBF file. (NB: includes deleted records). */
    recordCount: number;
    /** Date of last update as recorded in the DBF file header. */
    dateOfLastUpdate: Date;
    /** Metadata for all fields defined in the DBF file. */
    fields: FieldDescriptor[];
    /**
     * Reads a subset of records from this DBF file. If the `includeDeletedRecords` option is set, then deleted records
     * are included in the results, otherwise they are skipped. Deleted records have the property `[DELETED]: true`,
     * using the `DELETED` symbol exported from this library.
     */
    readRecords(maxCount?: number): Promise<(Record<string, unknown> & {
        [DELETED]?: true;
    })[]>;
    /**
     * Iterates over each record in this DBF file. If the `includeDeletedRecords` option is set, then deleted records
     * are yielded, otherwise they are skipped. Deleted records have the property `[DELETED]: true`, using the `DELETED`
     * symbol exported from this library.
     */
    [Symbol.asyncIterator](): AsyncGenerator<Record<string, unknown> & {
        [DELETED]?: true;
    }, void, unknown>;
    _readMode: "strict" | "loose";
    _encoding: Encoding;
    _includeDeletedRecords: boolean;
    _recordsRead: number;
    _headerLength: number;
    _recordLength: number;
    _memoData?: ArrayBuffer;
    _version?: number | undefined;
}
/** Symbol used for detecting deleted records when the `includeDeletedRecords` option is used. */
export declare const DELETED: unique symbol;
