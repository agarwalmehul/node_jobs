console.log('Creating Localities from Locations...\n');

var fs = require('fs');
var superAgent = require('superagent');
var csvParse = require('csv-parse');
var async = require('async');

var LOCATIONS_FILE = __dirname + '/locations.csv';
var LOCALITY_FILE_NAME = __dirname + '/localities.csv';
var API_CALL_DELAY = 0;
var API_CALL_LIMIT = 50000;
var API_DELAY = 120000;
var GOOGLE_API_KEY = 'AIzaSyB6qspxM7NajH50Eko_Ady8kqMZ4jrvMSw';
var AUTOCOMPLETE_API = function (input) {
    return encodeURI('https://maps.googleapis.com/maps/api/place/autocomplete/json?components=country:in&key=' + GOOGLE_API_KEY + '&input=' + input);
};
var REVERSE_GEO_API = function (placeId) {
    return encodeURI('https://maps.googleapis.com/maps/api/geocode/json?key=' + GOOGLE_API_KEY + '&place_id=' + placeId);
};

async.waterfall([
    // Read Locations from Local File
    function (callback) {
        console.log('Reading Locations...');
        fs.readFile(LOCATIONS_FILE, 'utf8', function (err, data) {
            if (err) {
                console.log('Error reading file: ', LOCATIONS_FILE);
                return callback(err);
            }
            console.log('Done!\n');
            callback(err, data);
        });
    },

    // Parse Location's CSV File
    function (data, callback) {
        console.log('Parsing Locations...');
        csvParse(data, {}, function (err, csv) {
            if (err) {
                console.log('\nError parsing file: ', LOCATIONS_FILE);
                return callback(err);
            }
            console.log('Done!\n');
            callback(err, csv)
        });
    },

    // Fetch Predictions for all Locations
    function (csv, callback) {
        console.log('Fetching Predictions...');
        var predictions = {};
        async.eachSeries(csv, function (line, next) {
            var locationName = line[0];
            process.stdout.write('\rProcessing: ' + (csv.indexOf(line)+1) + '/' + csv.length);
            // Fetch Google Autocomplete Predictions for each Location
            getPredictions(locationName, function (err, locPredictions) {
                if (err) {
                    console.log('\nError fetching predictions: ');
                    return next(err);
                }

                // Store all Predictions in Hash for Reverse Geo
                locPredictions.forEach(function (prediction) {
                    if (isLocality(prediction)) {
                        predictions[prediction.place_id] = prediction;
                    }
                });
                setTimeout(next, API_CALL_DELAY);
            });
        }, function (err) {
            if (err) { return callback(err); }
            console.log('\nDone!                                      \n');
            callback(err, predictions);
        });
    },

    // Fetch Reverse Geo of Predictions
    function (predictions, callback) {
        console.log('Fetching Reverse Geo...');
        var geocodes = [];
        var placeIds = Object.keys(predictions);
        async.eachSeries(placeIds, function (placeId, next) {
            process.stdout.write('\rProcessing: ' + (placeIds.indexOf(placeId)+1) + '/' + placeIds.length);
            getGeocode(placeId, function (err, geocode) {
                if (err) {
                    console.log('\nError fetching geocode: ');
                    return next(err);
                }

                if (geocode) {
                    geocodes.push(geocode);
                }

                setTimeout(next, API_CALL_DELAY);
            })
        }, function (err) {
            if (err) { return callback(err); }
            console.log('\nDone!                                      \n');
            callback(err, geocodes);
        });
    },

    // Create Locality CSV from Geocodes
    function (geocodes, callback) {
        console.log('Parsing Geocodes...');
        var LOCALITY_HEADER = 'place_id,types,short_name,long_name,city,formatted_address,lat,lng,url_key';
        var localities = [LOCALITY_HEADER];
        geocodes.forEach(function (geocode) {
            var locality = [];
            var address = geocode.address_components[0];
            var location = geocode.geometry.location;
            if (address) {
                var city = getCityFromGeocode(geocode);
                locality.push("\"" + geocode.place_id + "\"");
                locality.push("\"" + JSON.stringify(address.types).replace(/"/g, "'") + "\"");
                locality.push("\"" + address.short_name + "\"");
                locality.push("\"" + address.long_name + "\"");
                locality.push("\"" + city.long_name + "\"");
                locality.push("\"" + geocode.formatted_address + "\"");
                locality.push(location.lat);
                locality.push(location.lng);
                locality.push(address.long_name.toLowerCase().replace(/ /g, '-'));
                locality = locality.join();
                localities.push(locality);
            }
        });

        localities = localities.join('\n');
        fs.writeFile(LOCALITY_FILE_NAME, localities, function (err) {
            if (err) {
                console.log('Error writing file: ', LOCALITY_FILE_NAME, '\n', err);
                return callback(err);
            }
            console.log('Done!');
            callback()
        });
    }
], function (err) {
    if (err) { return console.log('\n', err, '\n'); }
    console.log('\nLocalities Created!');
});


//////////////////////////////////////////////////////  HELPER FUNCTIONS  //////////////////////////////////////////////////////
var predictionCounter = 0;
function getPredictions(input, callback) {
    if (predictionCounter && predictionCounter%API_CALL_LIMIT === 0) {
        return addDelay(function() {
            predictionCounter = 0;
            getPredictions(input, callback);
        });
    }

    superAgent.get(AUTOCOMPLETE_API(input)).end(function (error, response) {
        predictionCounter++;
        var err, predictions;
        var responseText = JSON.parse(response.text);
        var status = responseText.status;
        var errorMessage = responseText.error_message;
        if (status === 'OK' || status === 'ZERO_RESULTS') {
            predictions = responseText.predictions;
        } else {
            err = { status: status, errorMessage: errorMessage };
        }

        callback(err, predictions);
    });
}

function isLocality(prediction) {
    var typesWhitelist = ['sublocality_level_1'];
    var flag = false;
    var predictionTypes = prediction.types;
    typesWhitelist.forEach(function (type) {
        if (predictionTypes.indexOf(type) !== -1) {
            flag = true;
        }
    });
    return flag;
}

var geocodeCounter = 0;
function getGeocode(placeId, callback) {
    if (geocodeCounter && geocodeCounter%API_CALL_LIMIT === 0) {
        return addDelay(function() {
            geocodeCounter = 0;
            getGeocode(placeId, callback);
        });
    }

    superAgent.get(REVERSE_GEO_API(placeId)).end(function (error, response) {
        geocodeCounter++;
        var err, geocode;
        var responseText = JSON.parse(response.text);
        var status = responseText.status;
        var errorMessage = responseText.error_message;
        if (status === 'OK') {
            geocode = responseText.results[0];
        } else {
            err = { status: status, errorMessage: errorMessage };
        }

        callback(err, geocode);
    });
}

function getCityFromGeocode(geocode) {
    return geocode.address_components.filter(function (addressComponent) {
        if (addressComponent.types.indexOf('locality') !== -1) {
            return true;
        }
    })[0];
}

function addDelay(callback) {
    console.log('\nDelaying Execution by:', (API_DELAY/1000) + 's');
    setTimeout(function() {
        callback();
    }, API_DELAY);
}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
