# DBF Reader for the Browser

A TypeScript library to read `.dbf` files (dBase, FoxPro, etc.) in the browser or any modern JavaScript environment.

> Nota: El código fuente principal vive en `src/`. El archivo `index.ts` en la raíz solo reexporta la API pública para mantener compatibilidad con consumidores legacy.

This project is a *fork* of the popular [`DBFFile`](https://github.com/yortus/DBFFile) by @yortus. The main differences are:
- **Read-Only**: All write functionality has been removed to reduce bundle size and complexity.
- **Environment-Agnostic API**: All Node.js file system dependencies have been removed. The library now operates on an `ArrayBuffer`, making it universal.
- **Native APIs**: Uses the native `TextDecoder` API for character encoding.

## Installation

Currently, you can build the library from the source:

```bash
npm install
npm run build
```

You can then use the `dist/index.js` file in your project.

## Usage Example

The following example shows how to read a `.dbf` file and its corresponding `.fpt` or `.dbt` memo file from a file input in an HTML page.

**HTML:**
```html
<label for="dbf-input">Select a .dbf file:</label>
<input type="file" id="dbf-input" />

<label for="memo-input">Select a .fpt/.dbt file (optional):</label>
<input type="file" id="memo-input" />

<button id="read-button">Read DBF</button>

<pre id="output"></pre>
```

**JavaScript:**
```javascript
import { DBFFile } from './dist/index.js'; // Adjust the path for your project

const dbfInput = document.getElementById('dbf-input');
const memoInput = document.getElementById('memo-input');
const readButton = document.getElementById('read-button');
const output = document.getElementById('output');

readButton.addEventListener('click', async () => {
    const dbfFile = dbfInput.files[0];
    const memoFile = memoInput.files[0];

    if (!dbfFile) {
        alert('Please select a .dbf file');
        return;
    }

    try {
        // Read files as ArrayBuffer
        const dbfBuffer = await dbfFile.arrayBuffer();
        const memoBuffer = memoFile ? await memoFile.arrayBuffer() : undefined;

        // Open the DBF file with the buffers
        const dbf = await DBFFile.open(dbfBuffer, memoBuffer);

        console.log(`DBF file has ${dbf.recordCount} records.`);
        console.log(`Field names: ${dbf.fields.map(f => f.name).join(', ')}`);

        // Read all records
        const records = await dbf.readRecords();
        
        // Display the first 5 records as an example
        output.textContent = JSON.stringify(records.slice(0, 5), null, 2);

    } catch (error) {
        console.error('An error occurred:', error);
        output.textContent = `Error: ${error.message}`;
    }
});
```

## Core API

The API has been simplified to focus on reading.

### `DBFFile.open(dbfBuffer, memoBuffer?, options?)`

Opens an existing DBF file from its content in an `ArrayBuffer`.

- `dbfBuffer` (required): An `ArrayBuffer` containing the content of the `.dbf` file.
- `memoBuffer` (optional): An `ArrayBuffer` containing the content of the memo file (`.dbt` or `.fpt`).
- `options` (optional): Opening options.

Returns a `Promise<DBFFile>`.

### `dbf.readRecords(maxCount?)`

Reads a batch of records from the file.

- `maxCount` (optional): The maximum number of records to read. Defaults to reading all records.

Returns a `Promise<object[]>`.

### `for await (const record of dbf)`

You can also iterate over the records asynchronously.

```javascript
const dbf = await DBFFile.open(dbfBuffer);
for await (const record of dbf) {
    console.log(record);
}
```

### Options (`OpenOptions`)

- `encoding`: Specifies the character encoding. Defaults to `latin1`. Only encodings supported by the browser's native `TextDecoder` API are supported.
- `includeDeletedRecords`: If `true`, records marked for deletion are included in the results.

## Development

To contribute or make changes to the library:

1.  **Clone the repository.**
2.  **Install dependencies:**
    ```bash
    npm install
    ```
3.  **Build the code:**
    ```bash
    npm run build
    ```
4.  **Run tests:**
    ```bash
    npm test
    ```

## License

MIT
