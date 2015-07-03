'use strict';
/**
 * Exports a function to parse a QB log file
 */

var LineByLine = require('line-by-line');
var config = require('config');
var eol = require('os').EOL;
var moment = require('moment');
var _ = require('lodash');

var dateRegex = /(\d{8} \d\d:\d\d:\d\d):[A-Za-z]+:/;
var filterRegex = new RegExp(config.get('queryFilterRegex'));
var dateFormat = 'YYYYMMDD HH:mm:ss';

var queryCount = 0;
var lineCount = 0;
var discardedCount = 0;

/**
 * Test if this line is the start of a new query / log entry
 * @param {string} line is the current line
 * @return {boolean} whether the line is the start of a new query or not
 */
var testIsLineNewQuery = function(line) {
    return !!line.match(dateRegex);
};



/**
 * add the current query to the provided array
 * @param {array} arr is the array to adde the query to
 * @retun null
 */
var addQuery = function(queryArray, currentQuery, currentQueryDate, lineCount) {
    var previousQuery = _.last(queryArray);

    queryCount++;

    if (currentQuery.match(filterRegex)) {
        // mark previous log entry as lastInHour if this is a new hour.
        if (previousQuery && previousQuery.date.isBefore(currentQueryDate)) {
            previousQuery.lastInHour = true;
        }
        queryArray.push({
            date: currentQueryDate,
            query: currentQuery,
            lineNumber: lineCount,
            lastInHour: false
        });
    } else {
        discardedCount++;
    }

    if (queryCount % 5000 === 0) {
        logProgress(currentQuery, currentQueryDate);
    }
};



/**
 * parse date from first line of query
 * @param {string} line is the line to be parsed
 * @return {Date}
 */
var getLineQueryDate = function(line) {
    var regex = /(\d{8} \d\d:\d\d:\d\d):[A-Za-z]+/;
    var matches = line.match(regex);
    var dateString;
    var date;

    if (!matches || matches.length != 2) {
        throw new Error('Could not locate date string in line: `' + line + '`');
    }

    dateString = matches[1];
    date = moment(dateString, dateFormat).minutes(0).seconds(0);

    if (!date.isValid()) {
        throw new Error('Could not parse date string in line: `' + line + '`');
    }

    return date;
};



/**
 * log the current query progress
 */
var logProgress = function(currentQuery, currentQueryDate) {
    console.log(currentQueryDate.format() + ': ' + lineCount + ' Lines, ' + queryCount + ' Queries, ' + discardedCount + ' Discarded, ' + (queryCount - discardedCount) + ' Retained');
};

/**
 * parse the log at logPath, and place results in queryArray
 * @param {string} logPath is the path to the log file to be parsed
 * @param {callback} a function to be called after parsing is complete with queryArray as its parameter
 * @return {array} an array of {date: , query: } objects
 */
module.exports = function parseLog(logPath, callback) {
    var lineReader = new LineByLine(logPath);
    var queryArray = [];
    var currentQuery = '';
    var currentQueryDate = null;

    /**
     * initialize the currentQuery and currentQueryDate using the current line
     * @param {string} line is the first line of the new query
     * @return null
     */
    var initQuery = function(line) {
        currentQuery = line;
        currentQueryDate = getLineQueryDate(line);
    };



    //initialize counters
    lineCount = 0;
    queryCount = 0;
    discardedCount = 0;

    console.log('Beginning parsing of ' + logPath);

    // lineReader event handlers
    lineReader.on('error', function(err) {
        console.error(err);
    });

    lineReader.on('line', function parseLine(line) {
        lineCount++;
        if (testIsLineNewQuery(line)) {
            //line is the start of a new query
            addQuery(queryArray, currentQuery, currentQueryDate, lineCount);
            initQuery(line);
        } else {
            currentQuery += eol + line;
        }
    });

    lineReader.on('end', function() {
        addQuery(queryArray, currentQuery, currentQueryDate, lineCount);
        logProgress(currentQuery, currentQueryDate);
        console.log('Completed parsing of ' + logPath);
        callback(queryArray);
    });
};
