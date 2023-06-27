const fs = require('fs');
const readline = require('readline');
const { once } = require('events');

const CHUNK_SIZE = 500 * 1024 * 1024; // размер куска файла в байтах
const TEMP_DIR = './tmp'; // директория для временных файлов
const TEMP_PREFIX = 'chunk'; // префикс имени временных файлов

async function main() {
  // создаем директорию для временных файлов
  if (!fs.existsSync(TEMP_DIR)) {
    fs.mkdirSync(TEMP_DIR);
  }

  // делим файл на куски и сортируем их
  const chunks = await splitAndSort('input.txt');

  // сливаем отсортированные куски в один файл
  await mergeChunks(chunks,  'input.txt','output.txt' );

  console.log('Done!');
}

async function splitAndSort(inputFile) {
  const chunks = [];

  const input = fs.createReadStream(inputFile);
  const reader = readline.createInterface({
    input,
    crlfDelay: Infinity
  });

  let chunk = '';
  let byteCount = 0;
  let chunkNum = 0;

  for await (const line of reader) {
    // добавляем строку к текущему куску
    chunk += line + '\n';
    byteCount += Buffer.byteLength(line) + 1; // учитываем байты перевода строки

    if (byteCount >= CHUNK_SIZE) {
      // сохраняем кусок во временный файл и очищаем текущий кусок
      const chunkFile = `${TEMP_DIR}/${TEMP_PREFIX}${chunkNum}.txt`;
      fs.writeFileSync(chunkFile, chunk);
      chunks.push(chunkFile);
      chunk = '';
      byteCount = 0;
      chunkNum++;
    }
  }

  // сохраняем последний кусок
  if (chunk !== '') {
    const chunkFile = `${TEMP_DIR}/${TEMP_PREFIX}${chunkNum}.txt`;
    fs.writeFileSync(chunkFile, chunk);
    chunks.push(chunkFile);
  }

  // сортируем каждый кусок
  for (const chunkFile of chunks) {
    const lines = fs.readFileSync(chunkFile, 'utf8').split('\n').filter(Boolean);
    lines.sort();
    fs.writeFileSync(chunkFile, lines.join('\n'));
  }
  console.log('Chunks:', chunks);
  return chunks;
}

async function mergeChunks(chunks, outputFile, inputFile) {
  const chunkReaders = [];

  // создаем read stream для каждого куска
  for (const chunkFile of chunks) {
    const reader = readline.createInterface({
      input: fs.createReadStream(chunkFile),
      crlfDelay: Infinity
    });
    chunkReaders.push(reader);
  }

  // создаем write stream для выходного файла
  const output = fs.createWriteStream(outputFile);

  // сливаем отсортированные куски в один файл
  let currentLines = [];
  for (const reader of chunkReaders) {
    const [line] = await once(reader, 'line');
    currentLines.push({ reader, line });
  }

  while (currentLines.length > 0) {
    // выбираем минимальную строку из текущих строк каждого куска
    let minLine = currentLines[0].line;
    let minIndex = 0;
    for (let i = 1; i < currentLines.length; i++) {
      if (currentLines[i].line < minLine) {
        minLine = currentLines[i].line;
        minIndex = i;
      }
    }

    // записываем минимальную строку в выходной файл
    output.write(`${minLine}\n`);

    // читаем следующую строку из выбранного куска
    const { reader } = currentLines[minIndex];
    const [line] = await once(reader, 'line');

    if (line !== undefined) {
      // если есть еще строки в куске, добавляем следующую строку в текущие строки
      currentLines[minIndex].line = line;
    } else {
      // если кусок закончился, удаляем его из текущих кусков
      currentLines.splice(minIndex, 1);
      reader.close();
    }
  }

  // закрываем все потоки
  output.end();
  for (const reader of chunkReaders) {
    reader.close();
  }

  try {
    await fs.promises.access(inputFile, fs.constants.R_OK);
  } catch (err) {
    console.error(`Cannot read input file "${inputFile}":`, err);
    process.exit(1);
  }
}

try {
  main();
} catch (err) {
  console.error(err);
  process.exit(1);
}

