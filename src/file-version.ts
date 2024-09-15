export type FileVersion =
    | 0x03 // dBase III without memo file
    | 0x83 // dBase III with memo file
    | 0x8b // dBase IV with memo file
    | 0x30 // Visual FoxPro 9 (may have memo file)
    | 0xf5 // FoxPro 2.x (may have memo file)
;




export function isValidFileVersion(fileVersion: number): fileVersion is FileVersion {
    return [0x03, 0x83, 0x8b, 0x30, 0xf5].includes(fileVersion);
}
