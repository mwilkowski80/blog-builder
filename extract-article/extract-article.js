const { JSDOM } = require("jsdom");
const { Readability } = require("@mozilla/readability");
const axios = require('axios');
const fs = require('fs');
const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');

async function main() {
    const argv = yargs(hideBin(process.argv)).option('url', {
        describe: 'URL or file path of the HTML document',
        type: 'string',
        demandOption: true
    }).argv;

    let htmlContent;
    if (argv.url.startsWith('http://') || argv.url.startsWith('https://')) {
        try {
            const response = await axios.get(argv.url);
            htmlContent = response.data;
        } catch (error) {
            console.error("Failed to fetch the URL:", error);
            process.exit(1);
        }
    } else {
        try {
            htmlContent = fs.readFileSync(argv.url, 'utf8');
        } catch (error) {
            console.error("Failed to read the file:", error);
            process.exit(1);
        }
    }

    const dom = new JSDOM(htmlContent);
    const reader = new Readability(dom.window.document);
    const article = reader.parse();

    if (article) {
        console.log(article.textContent);
    } else {
        console.error("Failed to extract the article from the provided HTML.");
    }
}

main();