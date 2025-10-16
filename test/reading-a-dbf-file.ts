import { promises as fs } from "fs";
import * as path from "path";
import { DBFFile, OpenOptions, DELETED } from "../src";

describe("Reading a DBF file", () => {
  interface Test {
    description: string;
    filename: string;
    options?: OpenOptions;
    recordCount?: number;
    dateOfLastUpdate?: Date;
    numberOfRecordsToRead?: number;
    firstRecord?: Record<string, unknown> & { [DELETED]?: true };
    lastRecord?: Record<string, unknown> & { [DELETED]?: true };
    deletedCount?: number;
    error?: string;
  }

  let tests: Test[] = [
    {
      description: "DBF with default encoding",
      filename: "PYACFL.DBF",
      recordCount: 45,
      dateOfLastUpdate: new Date("2014-04-14"),
      firstRecord: {
        AFCLPD: "W",
        AFHRPW: 2.92308,
        AFLVCL: 0.0,
        AFCRDA: new Date("1999-03-25"),
        AFPSDS: "",
      },
      lastRecord: {
        AFCLPD: "W",
        AFHRPW: 0,
        AFLVCL: 0.0,
        AFCRDA: new Date("1991-04-15"),
        AFPSDS: "",
      },
      deletedCount: 30,
    },
    {
      description: "DBF with duplicated field name",
      filename: "dbase_03.dbf",
      error: `Duplicate field name: Point_ID.`,
    },
    {
      description:
        "DBF stored with non-default encoding, read using default encoding",
      filename: "WSPMST.DBF",
      recordCount: 6802,
      dateOfLastUpdate: new Date("1919-05-03"),
      firstRecord: {
        DISPNAME: "ÃÍ§à·éÒºØÃØÉADDA 61S02-M1",
        GROUP: "5",
        LEVEL: "N",
      },
      lastRecord: { DISPNAME: "", GROUP: "W", LEVEL: "S" },
      deletedCount: 5,
    },
    {
      description:
        "DBF stored with non-default encoding, read using correct encoding",
      filename: "WSPMST.DBF",
      options: { encoding: "tis620" },
      error: "Unsupported character encoding",
    },
    {
      description: "DBF read with multiple field-specific encodings",
      filename: "WSPMST.DBF",
      options: { encoding: { default: "tis620", PNAME: "latin1" } },
      error: "Unsupported character encoding",
    },
    {
      description: "DBF with memo file (version 0x83)",
      filename: "dbase_83.dbf",
      recordCount: 67,
      dateOfLastUpdate: new Date("2003-12-18"),
      firstRecord: {
        ID: 87,
        CODE: "1",
        NAME: "Assorted Petits Fours",
        WEIGHT: 5.51,
        DESC: `Our Original assortment...a little taste of heaven for everyone.  Let us
select a special assortment of our chocolate and pastel favorites for you.
Each petit four is its own special hand decorated creation. Multi-layers of
moist cake with combinations of specialty fillings create memorable cake
confections. Varietes include; Luscious Lemon, Strawberry Hearts, White
Chocolate, Mocha Bean, Roasted Almond, Triple Chocolate, Chocolate Hazelnut,
Grand Orange, Plum Squares, Milk chocolate squares, and Raspberry Blanc.`,
      },
      lastRecord: {
        ID: 94,
        CODE: "BD02",
        NAME: "Trio of Biscotti",
        WEIGHT: 0,
        DESC: "This tin is filled with a tempting trio of crunchy pleasures that can be enjoyed by themselves or dunked into fresh cup of coffee. Our perennial favorite Biscotti di Divine returns, chockfull of toasted almonds, flavored with a hint of cinnamon, and half dipped into bittersweet chocolate. Two new twice-baked delights make their debut this season; Heavenly Chocolate Hazelnut and Golden Orange Pignoli. 16 biscotti are packed in a tin.  (1Lb. 2oz.)",
      },
      deletedCount: 0,
    },
    {
      description: "DBF with memo file (version 0x8b)",
      filename: "dbase_8b.dbf",
      recordCount: 10,
      dateOfLastUpdate: new Date("2000-06-12"),
      firstRecord: {
        CHARACTER: "One",
        NUMERICAL: 1,
        LOGICAL: true,
        FLOAT: 1.23456789012346,
        MEMO: "First memo\r\n",
      },
      lastRecord: {
        CHARACTER: "Ten records stored in this database",
        NUMERICAL: 10,
        DATE: null,
        LOGICAL: null,
        FLOAT: 0.1,
        MEMO: null,
      },
      deletedCount: 0,
    },
    {
      description: "VFP9 DBF without memo file (version 0x30)",
      filename: "vfp9_30.dbf",
      recordCount: 3,
      dateOfLastUpdate: new Date("1919-11-06"),
      firstRecord: {
        FIELD1: "carlos manuel",
        FIELD2: new Date("2013-12-12"),
        FIELD3: new Date("2013-12-12 08:30:00 GMT"),
        FIELD4: 17000000000,
        FIELD5: 2500.55,
        FIELD6: true,
      },
      lastRecord: {
        FIELD1: "ricardo enrique",
        FIELD2: new Date("2017-08-07"),
        FIELD3: new Date("2017-08-07 20:30:00 GMT"),
        FIELD4: 17000000000,
        FIELD5: 2500.45,
        FIELD6: true,
      },
      deletedCount: 1,
    },
    {
      description: "VFP9 DBF with memo file (version 0x30)",
      filename: "vfp9_30_memo.dbf",
      recordCount: 3,
      dateOfLastUpdate: new Date("1921-10-12"),
      firstRecord: {
        ID: 1,
        MEMO: "Memo of record 1. Which needs to be very long to be bigger than 64 bytes and take 2 memo blocks",
        CHAR: "Text1",
        NUM: 999.5,
        DATE: new Date("2021-10-11"),
        TIME: new Date("2021-10-11 22:34:50 GMT"),
      },
      lastRecord: {
        ID: 3,
        MEMO: "Memo of record 3",
        CHAR: "Text3",
        NUM: 10.11,
        DATE: new Date("2020-01-01"),
        TIME: new Date("2020-01-01 12:12:12 GMT"),
      },
      deletedCount: 1,
    },
    {
      description: `DBF with unsupported file version and field types in 'strict' (default) read mode`,
      filename: "dbase_31.dbf",
      error: "Invalid file version: 49",
    },
    {
      description: `DBF with unsupported file version and field types in 'loose' read mode`,
      filename: "dbase_31.dbf",
      options: { readMode: "loose" },
      recordCount: 77,
      dateOfLastUpdate: new Date("1902-08-02"),
      firstRecord: {
        PRODUCTID: 1,
        PRODUCTNAM: "Chai",
        REORDERLEV: 10,
        DISCONTINU: false,
      },
      lastRecord: {
        PRODUCTID: 77,
        PRODUCTNAM: "Original Frankfurter grüne Soáe",
        REORDERLEV: 15,
        DISCONTINU: false,
      },
      deletedCount: 0,
    },
    {
      description: `DBF with missing memo file in 'strict' (default) read mode`,
      filename: "dbase_8b_missing_memo.dbf",
      error: `Error reading memo file (read past end).`,
    },
    {
      description: `DBF with missing memo file in 'loose' read mode`,
      filename: "dbase_8b_missing_memo.dbf",
      options: { readMode: "loose" },
      recordCount: 10,
      dateOfLastUpdate: new Date("2000-06-12"),
      firstRecord: { NUMERICAL: 1, LOGICAL: true, FLOAT: 1.23456789012346 },
      lastRecord: { NUMERICAL: 10, DATE: null, LOGICAL: null, FLOAT: 0.1 },
      deletedCount: 0,
    },
    {
      description: "DBF with deleted records included in results",
      filename: "PYACFL.DBF",
      options: { includeDeletedRecords: true },
      recordCount: 45,
      dateOfLastUpdate: new Date("2014-04-14"),
      firstRecord: {
        [DELETED]: true,
        AFCLPD: "W",
        AFHRPW: 0,
        AFACCL: "P",
        AFCRDA: new Date("1991-04-15"),
        AFPSDS: "",
      },
      lastRecord: {
        AFCLPD: "W",
        AFHRPW: 0,
        AFLVCL: 0.0,
        AFCRDA: new Date("1991-04-15"),
        AFPSDS: "",
      },
      deletedCount: 0,
    },
    {
      description: `DBF with a multibyte character straddling two memo blocks`,
      filename: "vfp_gb2312_memo.dbf",
      options: { readMode: "loose", encoding: "gb2312" },
      error: "Unsupported character encoding",
    },
    {
      description:
        "DBF with deleted records included in results and non latin-1 headers",
      filename: "dbase_not_latin1.dbf",
      options: { includeDeletedRecords: true, encoding: "big5" },
      recordCount: 2,
      dateOfLastUpdate: new Date("1998-09-13"),
      firstRecord: {
        畜主姓名: "徐",
        電話: "292",
        地址: "台北縣永和",
        微晶片號碼: "00013",
        狂犬病牌號: "87A",
        犬名: "小小",
        品種: "約克夏",
        性別: "公",
        出生日期: new Date("1991-04-16"),
        登入日期: new Date("1998-04-28"),
      },
      lastRecord: {
        [DELETED]: true,
        畜主姓名: "Chen",
        電話: "0123456789",
        地址: "Fake address",
        微晶片號碼: "000000",
        狂犬病牌號: "000000",
        犬名: "Dog",
        品種: "Dog",
        性別: "母",
        出生日期: new Date("2023-09-13"),
        登入日期: new Date("2023-09-13"),
      },
      deletedCount: 0,
    },
    {
      description: `DBF with currency field type ('Y')`,
      filename: "dbase_currency.dbf",
      recordCount: 2,
      firstRecord: {
        HCODE: "43659",
        LCCODE: "6505-003-0001",
        STDCODE: "1033097",
        UP: 350.0,
        DATEBEG: new Date("2023-05-13"),
        DATEEND: new Date("9999-12-31"),
      },
      lastRecord: {
        HCODE: "43659",
        LCCODE: "6505-003-0002",
        STDCODE: "690439",
        UP: 560.0,
        DATEBEG: new Date("2023-05-17"),
        DATEEND: new Date("9999-12-31"),
      },
      dateOfLastUpdate: new Date("1924-04-10"),
      deletedCount: 0,
    },
    {
      description: `VFP9 DBF with memo file with block size 1 (version 0x30)`,
      filename: "vfp9_memo_bs1.dbf",
      recordCount: 5,
      dateOfLastUpdate: new Date("1924-06-30"),
      firstRecord: {
        CODE: "a1",
        NOTES: "Some notes here\r\n",
      },
      lastRecord: {
        CODE: "b3",
        NOTES: " ",
      },
      deletedCount: 0,
    },
  ];

  function toArrayBuffer(buf: Buffer): ArrayBuffer {
    return buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength);
  }

  async function getMemoArrayBuffer(
    dbfFilePath: string
  ): Promise<ArrayBuffer | undefined> {
    const { dir, name } = path.parse(dbfFilePath);
    const fptPath = path.join(dir, name + ".fpt");
    const dbtPath = path.join(dir, name + ".dbt");
    try {
      return toArrayBuffer(await fs.readFile(fptPath));
    } catch {
      /* File not found */
    }
    try {
      return toArrayBuffer(await fs.readFile(dbtPath));
    } catch {
      /* File not found */
    }
    return undefined;
  }

  tests.forEach((test) => {
    it(`readRecords: ${test.description}`, async () => {
      const {
        filename,
        options,
        recordCount,
        dateOfLastUpdate,
        numberOfRecordsToRead,
        firstRecord,
        lastRecord,
        deletedCount,
        error,
      } = test;

      try {
        const dbfFilePath = path.join(__dirname, "fixtures", filename);
        const dbfArrayBuffer = toArrayBuffer(await fs.readFile(dbfFilePath));
        const memoArrayBuffer = await getMemoArrayBuffer(dbfFilePath);

        const dbf = await DBFFile.open(
          dbfArrayBuffer,
          memoArrayBuffer,
          options
        );
        const records = await dbf.readRecords(numberOfRecordsToRead);

        if (error) throw new Error("Expected an error but none was thrown.");

        expect(dbf.recordCount).toBe(recordCount);
        expect(dbf.dateOfLastUpdate).toEqual(dateOfLastUpdate);
        expect(records[0]).toMatchObject(firstRecord!);
        if (firstRecord && DELETED in firstRecord) {
          expect(records[0][DELETED]).toBe(firstRecord[DELETED]);
        }
        expect(records[records.length - 1]).toMatchObject(lastRecord!);
        if (lastRecord && DELETED in lastRecord) {
          expect(records[records.length - 1][DELETED]).toBe(
            lastRecord[DELETED]
          );
        }
        expect(dbf.recordCount - records.length).toBe(deletedCount);
      } catch (e: any) {
        if (!error) throw e;
        expect(e.message).toContain(error);
      }
    });
  });

  tests.forEach((test) => {
    it(`asyncIterator: ${test.description}`, async () => {
      const {
        filename,
        options,
        recordCount,
        dateOfLastUpdate,
        numberOfRecordsToRead,
        firstRecord,
        lastRecord,
        deletedCount,
        error,
      } = test;

      const records: Array<Record<string, unknown> & { [DELETED]?: true }> = [];
      try {
        const dbfFilePath = path.join(__dirname, "fixtures", filename);
        const dbfArrayBuffer = toArrayBuffer(await fs.readFile(dbfFilePath));
        const memoArrayBuffer = await getMemoArrayBuffer(dbfFilePath);

        const dbf = await DBFFile.open(
          dbfArrayBuffer,
          memoArrayBuffer,
          options
        );
        for await (const record of dbf) {
          records.push(record);
          if (
            numberOfRecordsToRead !== undefined &&
            records.length >= numberOfRecordsToRead
          )
            break;
        }

        if (error) throw new Error("Expected an error but none was thrown.");

        expect(dbf.recordCount).toBe(recordCount);
        expect(dbf.dateOfLastUpdate).toEqual(dateOfLastUpdate);
        expect(records[0]).toMatchObject(firstRecord!);
        if (firstRecord && DELETED in firstRecord) {
          expect(records[0][DELETED]).toBe(firstRecord[DELETED]);
        }
        expect(records[records.length - 1]).toMatchObject(lastRecord!);
        if (lastRecord && DELETED in lastRecord) {
          expect(records[records.length - 1][DELETED]).toBe(
            lastRecord[DELETED]
          );
        }
        expect(dbf.recordCount - records.length).toBe(deletedCount);
      } catch (e: any) {
        if (!error) throw e;
        expect(e.message).toContain(error);
      }
    });
  });
});
