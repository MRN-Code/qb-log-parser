'use strict';
//TODO: decide how to format results: nest preview queries inside subject queries?

/**
 * Program to parse log files containing subject insertion queries and assessment queries
 * The queries will be stored in an array, then sorted by date
 */

var config = require('config');
var _ = require('lodash');
var eol = require('os').EOL;
var moment = require('moment');

var assessmentQueries = [];
var subjectQueries = [];
var allQueries = [];
var parseLog = require('./lib/log_parser.js');

/**
 * parse URSIs from query in log entry, placing results in 'ursis' parameter
 * @param {object} log is the log entry with a date: property and a query property
 * @return {object} the log object with ursis appended to it;
 */
function setUrsis(log) {
    var regex = /M\d{8}/g;
    var ursis = log.query.match(regex);
    if (!ursis || ursis.length === 0) {
        console.log('Could not parse any URSIs from query: `' + log.query + '`');
        ursis = ['Could not parse any URSIs from query'];
    }
    log.ursis = ursis;
    return log;
}



/**
 * parse instrument_ids from query in log entry, placing results in 'instruments' parameter
 * @param {object} log is the log entry with a date: property and a query property
 * @return {object} the log object with instruments appended to it;
 */
function setInstruments(log) {
    var regex = /instrument_id ?= ?\d+/g;
    var matches = log.query.match(regex);
    var instruments;
    if (!matches || matches.length === 0) {
        //console.log('Could not parse any instruments query strings from query: `' + log.query + '`');
        instruments = ['*']; //'Could not parse instrument query strings from query';
    } else {
        instruments = matches.map(function matchNumbers(str) {
            return parseInt(str.match(/\d+/));
        });

        if (!instruments || instruments.length === 0) {
            console.log('Could not parse any instrument_ids from query: `' + log.query + '`');
            instruments = 'Could not parse instruments_ids from query';
        }
    }
    log.instruments = instruments;
    return log;
}


/**
 * parse study_ids from query in log entry, placing results in 'studies' parameter
 * @param {object} log is the log entry with a date: property and a query property
 * @return {object} the log object with studies appended to it;
 */
function setStudies(log) {
    var regex = /study_id ?= ?\d+/g;
    var matches = log.query.match(regex);
    var studies;
    if (!matches || matches.length === 0) {
        //console.log('Could not parse any studies query strings from query: `' + log.query + '`');
        studies = ['*']; //'Could not parse instrument query strings from query';
    } else {
        studies = matches.map(function matchNumbers(str) {
            return parseInt(str.match(/\d+/));
        });

        if (!studies || studies.length === 0) {
            console.log('Could not parse any studie_ids from query: `' + log.query + '`');
            studies = 'Could not parse instruments_ids from query';
        }
    }
    log.studies = studies;
    return log;
}

/**
 * locate and return the subject queries that might have been used for this preview
 * @prarm {array} array of subject queries
 * @param {object} the current object query
 * @return {array} previewQuery with subjecQueries property added to it
 */
function setSubjectQueries(subjectQueries, previewQuery) {
    //walk subjectQueries, looking for queries that have the same date as the preview query
    //if we pass the date of the preview query without locating any queries in the same 
    //hour as the preview query, return the last query before the current one.
    var currentQuery;
    var msg;
    var matchingQueries = _.filter(subjectQueries, function testIfQueryIsMatch(query) {
        if (query.date.isSame(previewQuery.date)) {
            return true;
        }
        return false;
    });
    var lastQuery = _.findLast(subjectQueries, function testIfQueryIsLast(query) {
        if (
            query.date.isBefore(previewQuery.date) &&
            moment(query.date).add(8, 'hours').isAfter(previewQuery.date)
        ) {
            return true;
        }
        return false;
    });

    if (lastQuery) {
        matchingQueries.unshift(lastQuery);
    }

    if (!matchingQueries.length) {
        msg = 'No subject queries found for query at `' + previewQuery.date.format() + '`';
        console.log(msg);
        matchingQueries.push({date: moment(), query: msg, ursis: [msg]});
    }

    // only retain unique matching queries (based on the ursi list)
    matchingQueries = _.uniq(matchingQueries, function(log) {
        return log.ursis.join('');
    });

    previewQuery.subjectQueries = matchingQueries;
    return previewQuery;
}



parseLog(config.get('input.assessment'), function(logs) {
    var assessmentQueries = logs.map(setInstruments).map(setStudies);
    require('fs').writeFileSync('./temp/assessmentQueries', JSON.stringify(assessmentQueries));
    parseLog(config.get('input.subject'), function(logs) {
        var subjectQueries = logs.map(setUrsis);
        var stream = require('fs').createWriteStream('./temp/queries.csv');
        var currentSubjectQuery;
        var undefinedSubjectQuery = {
            date: moment(),
            ursis: ['no subject query found'],
            query: 'no subject query found'
        };
        var rows = [];
        var headerRow = [ 
            'preview log ID', 
            'preview date (MST)',
            'studies',
            'instruments',
            'subject log ID',
            'subject date (MST)',
            'URSIs',
            //'raw asmt query',
            //'raw subject query'
        ];

        require('fs').writeFileSync('./temp/subjectQueries', JSON.stringify(subjectQueries));

        stream.write(headerRow.map(function addQuotes(str) {
            return '"' + str + '"';
        }) + eol);

        rows = _.map(assessmentQueries, _.partial(setSubjectQueries, subjectQueries));

        rows.forEach(function writeRow(previewQuery) {
            
            // format each subjectQuery for printing
            var rowArrays = previewQuery.subjectQueries.map(function formatSubjectQueries(subjectQuery) {
                return [
                    '',
                    '',
                    '',
                    '',
                    subjectQuery.lineNumber.toString(),
                    subjectQuery.date.format(),
                    subjectQuery.ursis.join(';'),
                    '',
                    //subjectQuery.query
                ];
            });

            // prepend the previewQuery row to the array of rows
            rowArrays.unshift(
                [
                    previewQuery.lineNumber.toString(),
                    previewQuery.date.format(),
                    previewQuery.studies.join(';'),
                    previewQuery.instruments.join(';'),
                    '',
                    '',
                    '',
                    //previewQuery.query.replace(/(?:\r\n|\r|\n)+/g, ' \\n '),
                    ''
                ]
            );

            // stringify and print rows
            rowArrays.forEach(function stringifyAndPrintRow(rowArray) {
                var rowStr = rowArray.map(function convertToString(val) {
                    return '"' + val.replace(/"/, '&quote') + '"';
                }).join(',');

                //write to stream
                stream.write(rowStr + eol);
            });
        });
    });
});

/* old code: not useful now that we know that the minute value of the log timestamps is corrupted.
        // combine both query sets
        allQueries = assessmentQueries.concat(subjectQueries);

        //unset asmtQueries and subjectQueries to free up mem
        assessmentQueries = [];
        subjectQueries = [];

        // sort query sets by date
        sortedQueries = _.sortBy(allQueries, function getUTCTimestamp(log) {
            return log.date.valueOf();
        });
    */
