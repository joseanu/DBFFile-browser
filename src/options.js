"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.normaliseOpenOptions = normaliseOpenOptions;
exports.normaliseCreateOptions = normaliseCreateOptions;
const file_version_1 = require("./file-version");
/** Validates the given OpenOptions and substitutes defaults for missing properties. Returns a new options object. */
function normaliseOpenOptions(options) {
    var _a, _b, _c;
    // Validate `encoding`.
    let encoding = (_a = options === null || options === void 0 ? void 0 : options.encoding) !== null && _a !== void 0 ? _a : "ISO-8859-1";
    assertValidEncoding(encoding);
    // Validate `readMode`.
    let readMode = (_b = options === null || options === void 0 ? void 0 : options.readMode) !== null && _b !== void 0 ? _b : "strict";
    if (readMode !== "strict" && readMode !== "loose") {
        throw new Error(`Invalid read mode ${readMode}`);
    }
    // Validate `includeDeletedRecords`.
    let includeDeletedRecords = (_c = options === null || options === void 0 ? void 0 : options.includeDeletedRecords) !== null && _c !== void 0 ? _c : false;
    if (typeof includeDeletedRecords !== "boolean") {
        throw new Error(`Invalid value 'includeDeletedRecords' value ${includeDeletedRecords}`);
    }
    // Return a new normalised options object.
    return { encoding, readMode, includeDeletedRecords };
}
/** Validates the given CreateOptions and substitutes defaults for missing properties. Returns a new options object. */
function normaliseCreateOptions(options) {
    var _a, _b;
    // Validate `fileVersion`.
    let fileVersion = (_a = options === null || options === void 0 ? void 0 : options.fileVersion) !== null && _a !== void 0 ? _a : 0x03;
    if (!(0, file_version_1.isValidFileVersion)(fileVersion))
        throw new Error(`Invalid file version ${fileVersion}`);
    // Validate `encoding`.
    let encoding = (_b = options === null || options === void 0 ? void 0 : options.encoding) !== null && _b !== void 0 ? _b : "ISO-8859-1";
    assertValidEncoding(encoding);
    // Return a new normalised options object.
    return { fileVersion, encoding };
}
// Helper function for validating encodings.
function assertValidEncoding(encoding) {
    if (typeof encoding === "string") {
        if (UNSUPPORTED_ENCODINGS.has(encoding.toLowerCase())) {
            throw new Error(`Unsupported character encoding '${encoding}'`);
        }
        try {
            new TextDecoder(encoding);
        }
        catch (e) {
            throw new Error(`Unsupported character encoding '${encoding}'`);
        }
    }
    else if (typeof encoding === "object" && encoding !== null) {
        let encodingObject = encoding;
        if (!encodingObject.default) {
            throw new Error(`No default encoding specified`);
        }
        for (let key of Object.keys(encodingObject)) {
            const encodingName = encodingObject[key];
            if (UNSUPPORTED_ENCODINGS.has(encodingName.toLowerCase())) {
                if (key === "default") {
                    throw new Error(`Unsupported character encoding '${encodingName}'`);
                }
                throw new Error(`Unsupported character encoding '${encodingName}' for field '${key}'`);
            }
            try {
                new TextDecoder(encodingObject[key]);
            }
            catch (e) {
                throw new Error(`Unsupported character encoding '${encodingObject[key]}' for field '${key}'`);
            }
        }
    }
    else {
        throw new Error(`Invalid encoding value ${encoding}`);
    }
}
const UNSUPPORTED_ENCODINGS = new Set(["gb2312"].map((enc) => enc.toLowerCase()));
//# sourceMappingURL=options.js.map