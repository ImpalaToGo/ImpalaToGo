'use strict';

/**
 * @ngdoc function
 * @name impala2GoApp.controller:NavigationCtrl
 * @description
 * # NavigationCtrl
 * Controller of the impala2GoApp
 */
angular.module('impala2GoApp')
  .controller('NavigationCtrl', function ($scope,logger,$route,routes) {
        var logSuccess= logger.getLogFn("MainCtrl", 'success');
        logSuccess('impala2GoApp loaded!', "", true);
        $scope.navRoutes=routes;
        $scope.isCurrent=function(route) {
            if (!route.config.title || !$route.current || !$route.current.title) {
                return '';
            }
            var menuName = route.config.title;
            return $route.current.title.substr(0, menuName.length) === menuName ? 'active' : '';
        }
  });
