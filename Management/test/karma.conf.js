// Karma configuration
// http://karma-runner.github.io/0.12/config/configuration-file.html
// Generated on 2014-10-20 using
// generator-karma 0.8.3

module.exports = function(config) {
  'use strict';

    config.set({
        // enable / disable watching file and executing tests whenever any file changes
        autoWatch: true,

        // base path, that will be used to resolve files and exclude
        basePath: '../',

        // testing framework to use (jasmine/mocha/qunit/...)
        frameworks: ['jasmine'],

        // list of files / patterns to load in the browser
        files: [
            'bower_components/jquery/dist/jquery.js',
            'bower_components/toastr/toastr.js',
            'bower_components/angular/angular.js',
            'bower_components/angular-route/angular-route.js',
            'bower_components/angular-mocks/angular-mocks.js',
            'bower_components/angular-touch/angular-touch.js',


            'app/scripts/app.js',
            'app/scripts/modules.js',
            'app/scripts/**/*.js',
            'app/scripts/*.js',
//        'app/scripts/controllers/*.js',
//        'app/scripts/directives/*.js',
//        'app/scripts/services/*.js',
//        'app/scripts/config/config.js',
            'test/mock/**/*.js',
            'test/spec/**/*.js'
//        //   'test/spec/controllers/*.js'
            // 'test/spec/controllers/admin.js'
        ],

        // list of files / patterns to exclude
        exclude: [],

        // web server port
        port: 8080,

        // Start these browsers, currently available:
        // - Chrome
        // - ChromeCanary
        // - Firefox
        // - Opera
        // - Safari (only Mac)
        // - PhantomJS
        // - IE (only Windows)
        browsers: [
            'PhantomJS',
            'Chrome'
        ],
        reporters: ['progress', 'html'],
        htmlReporter: {
            outputFile: 'test/units.html'
        },
        // Which plugins to enable
        plugins: [
            'karma-chrome-launcher',
            'karma-firefox-launcher',
            'karma-ie-launcher',
            'karma-phantomjs-launcher',
            'karma-jasmine',
            'karma-htmlfile-reporter'

        ],

        // Continuous Integration mode
        // if true, it capture browsers, run tests and exit
        singleRun: false,

        colors: true,

        // level of logging
        // possible values: LOG_DISABLE || LOG_ERROR || LOG_WARN || LOG_INFO || LOG_DEBUG
        logLevel: config.LOG_INFO,

        // Uncomment the following lines if you are using grunt's server to run the tests
        // proxies: {
        //   '/': 'http://localhost:9000/'
        // },
        // URL root prevent conflicts with the site root
        // urlRoot: '_karma_'
    });
};
