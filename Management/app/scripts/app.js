
'use strict';

/**
 * @ngdoc overview
 * @name impala2GoApp
 * @description
 * # impala2GoApp
 *
 * Main module of the application.
 */
angular.module('impala2GoApp', [
        'ngRoute',                   // routing
        'ui.bootstrap',              // bootstrap for angular
        'impala2GoApp.config' ,      // config
        'impala2GoApp.services',     // common services like data access, logger, etc.
        'impala2GoApp.directives',   // directives
        'impala2GoApp.tilesGrid'     // tiles grid
  ]);


