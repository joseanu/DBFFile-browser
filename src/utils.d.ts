/** Creates a date with no local timezone offset. `month` and `day` are 1-based. */
export declare function createDate(year: number, month: number, day: number): Date;
/** Parses an 8-character date string of the form 'YYYYMMDD' into a UTC Date object. */
export declare function parse8CharDate(s: string): Date;
/** Formats the given Date object as a string, in 8-character 'YYYYMMDD' format. `d` is assumed to be in UTC. */
export declare function format8CharDate(d: Date): string;
/** Parses the given Visual FoxPro DateTime representation into a UTC Date object. */
export declare function parseVfpDateTime(dt: {
    julianDay: number;
    msSinceMidnight: number;
}): Date;
/** Formats the given Date object as a Visual FoxPro DateTime representation. `d` is assumed to be in UTC. */
export declare function formatVfpDateTime(d: Date): {
    julianDay: number;
    msSinceMidnight: number;
};
