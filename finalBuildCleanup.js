//Custom script called by the build script after rollup has finished

import fs from 'fs';


//Delete the 'dist/esm/types' & 'dist/cjs/types' folders
//Since types have already been bundled into a single dist/types/index.d.ts file
fs.rmSync('./dist/cjs/types', { recursive: true, force: true });
fs.rmSync('./dist/esm/types', { recursive: true, force: true });