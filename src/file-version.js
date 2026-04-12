"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.isValidFileVersion = isValidFileVersion;
function isValidFileVersion(fileVersion) {
    return [0x03, 0x83, 0x8b, 0x30, 0xf5].includes(fileVersion);
}
//# sourceMappingURL=file-version.js.map