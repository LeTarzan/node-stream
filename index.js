import { dirname, join } from 'path'
import { promisify } from 'util'
import { promises, createReadStream, createWriteStream } from 'fs' 
import { pipeline, Transform } from 'stream' 
const pipelineASync = promisify(pipeline)
const { readdir } = promises

import csvtojson from 'csvtojson'
import jsontocsv from 'json-to-csv-stream'
import StreamConcat from 'stream-concat'

import debug from 'debug'
const log = debug('app:concat')

const { pathname: currentFile } = new URL(import.meta.url)
const cwd = dirname(currentFile)
const filesDir = `${cwd}/dataset`
const output = `${cwd}/final.csv`

console.time('concat-data')
const files = (await readdir(filesDir)).filter(file => file.endsWith('.csv'))

log(`processing ${files}`)
const ONE_SECOND = 1000

// when all others process dies, he dies too
setInterval(() => process.stdout.write('.'), ONE_SECOND).unref()

// combine multiple stream into a single
const streamsFiles = files.map(file => createReadStream(join(filesDir, file)))
const combinedStreams = new StreamConcat(streamsFiles)

const finalStream = createWriteStream(output)
const handleStream = new Transform({
  transform: (chunk, encoding, cb) => {
    const data = JSON.parse(chunk)
    const output =  {
      id: data.Respondent,
      country: data.Country
    }
    // log(`id: ${output.id}`)
    return cb(null, JSON.stringify(output))
  }
})

await pipelineASync(
  combinedStreams,
  csvtojson(),
  handleStream,
  jsontocsv(),
  finalStream
)
log(`${files.length} files merged! on ${output}`)
console.timeEnd('concat-data') 