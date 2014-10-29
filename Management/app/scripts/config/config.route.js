'use strict';

var app=angular.module('impala2GoApp.config');
app.constant('routes', getRoutes());

// Configure the routes and route resolvers
app.config(['$routeProvider', 'routes', routeConfigurator]);
function routeConfigurator($routeProvider, routes) {

    routes.forEach(function (r) {
        $routeProvider.when(r.url, r.config);
    });
    $routeProvider.otherwise({ redirectTo: '/' });
}

// Define the routes
function getRoutes() {
    return [
         {
            url: '/',
            config: {
                title: 'admin',
                templateUrl: '../../views/admin.html',
                controller: 'AdminCtrl'
            }
        },
        {
            url: '/settings',
            config: {
                templateUrl: '../../views/settings.html',
                controller: 'SettingsCtrl',
                title: 'Settings'
            }
        },
        {   url: '/cluster',
            config: {
                templateUrl: '../../views/cluster.html',
                controller: 'ClusterCtrl',
                title: 'Cluster'
            }
        },
        {   url: '/redim',
            config: {
                templateUrl: '../../views/redim.html',
                controller: 'RedimCtrl',
                title: 'Redim'
            }
        },
        {   url: '/query',
            config: {
                templateUrl: '../../views/query.html',
                controller: 'QueryCtrl',
                title: 'Query'
            }
        }
    ];
}



