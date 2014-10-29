'use strict';

/**
 * @ngdoc function
 * @name impala2GoApp.controller:AdminCtrl
 * @description
 * # AdminCtrl
 * Controller of the impala2GoApp
 */
angular.module('impala2GoApp')
  .controller('AdminCtrl', function ($scope,logger) {
        var log= logger.getLogFn("AdminCtrl");
        log("admin controller","",true);
  });
