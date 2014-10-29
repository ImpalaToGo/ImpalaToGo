'use strict';

/**
 * @ngdoc function
 * @name impala2GoApp.controller:SettingsCtrl
 * @description
 * # SettingsCtrl
 * Controller of the impala2GoApp
 */
angular.module('impala2GoApp')
  .controller('SettingsCtrl', function ($scope,logger) {
        var log= logger.getLogFn("SettingsCtrl");
        log("settings controller","",true);
  });
