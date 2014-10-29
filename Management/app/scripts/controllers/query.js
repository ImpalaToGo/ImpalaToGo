'use strict';

/**
 * @ngdoc function
 * @name impala2GoApp.controller:QueryCtrl
 * @description
 * # QueryCtrl
 * Controller of the impala2GoApp
 */
angular.module('impala2GoApp')
  .controller('QueryCtrl', function ($scope,logger) {
        var log= logger.getLogFn("QueryCtrl");
        log("query controller","",true);
  });
