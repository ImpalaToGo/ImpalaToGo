'use strict';

/**
 * @ngdoc service
 * @name impala2GoApp.dataContext
 * @description
 * # dataContext
 * Service in the impala2GoApp.
 */

angular.module('impala2GoApp.services')
  .factory('dataContext', function dataContext($q) {
        var service = {
            getAllClusters: getAllClusters,
            getSumClusters: getSumClusters
        };

        return service;

        function getAllClusters() {
            var clusters =  [
                {
                    "name": "Node 1",
                    "id": "34",
                    "ram": {
                        "value": "10",
                        "info": "used"
                    },
                    "diskSpace": {
                        "value": "20",
                        "info": "free"
                    },
                    "cpu": {
                        "value": "50",
                            "info": "free"
                    },
                    "networkUsage": {
                        "value": "20",
                        "info": "used"

                    },
                    "dfsBandwith": {
                        "value": "30",
                        "info": "free"
                    },
                    "localDiskIO": {
                        "value": "65",
                        "info": "free"
                    }
                },
                {
                    "name": "Node 2",
                    "id": "34",
                    "ram": {
                        "value": "10",
                        "info": "used"
                    },
                    "diskSpace": {
                        "value": "20",
                        "info": "free"
                    },
                    "cpu": {
                        "value": "24",
                        "info": "free"
                    },
                    "networkUsage": {
                        "value": "20",
                        "info": "used"

                    },
                    "dfsBandwith": {
                        "value": "30",
                        "info": "free"
                    },
                    "localDiskIO": {
                        "value": "65",
                        "info": "free"
                    }
                },
                {
                    "name": "Node 3",
                    "id": "35",
                    "ram": {
                        "value": "2",
                        "info": "used"
                    },
                    "diskSpace": {
                        "value": "79",
                        "info": "free"
                    },
                    "cpu": {
                        "value": "56",
                        "info": "free"
                    },
                    "networkUsage": {
                        "value": "86",
                        "info": "used"

                    },
                    "dfsBandwith": {
                        "value": "45",
                        "info": "free"
                    },
                    "localDiskIO": {
                        "value": "22",
                        "info": "free"
                    }
                },
                {
                    "name": "Node 4",
                    "id": "34",
                    "ram": {
                        "value": "45",
                        "info": "used"
                    },
                    "diskSpace": {
                        "value": "88",
                        "info": "free"
                    },
                    "cpu": {
                        "value": "58",
                        "info": "free"
                    },
                    "networkUsage": {
                        "value": "25",
                        "info": "used"

                    },
                    "dfsBandwith": {
                        "value": "11",
                        "info": "free"
                    },
                    "localDiskIO": {
                        "value": "78",
                        "info": "free"
                    }
                },
                {
                    "name": "Node 5",
                    "id": "34",
                    "ram": {
                        "value": "26",
                        "info": "used"
                    },
                    "diskSpace": {
                        "value": "20",
                        "info": "free"
                    },
                    "cpu": {
                        "value": "5",
                        "info": "free"
                    },
                    "networkUsage": {
                        "value": "50",
                        "info": "used"

                    },
                    "dfsBandwith": {
                        "value": "67",
                        "info": "free"
                    },
                    "localDiskIO": {
                        "value": "65",
                        "info": "free"
                    }
                },
            ];
            return $q.when(clusters);
        }
        function getSumClusters() {
            var clusters =   [
                {
                    "name": "Node 1",
                    "id": "34",
                    "ram": {
                        "value": "78",
                        "info": "used"
                    },
                    "diskSpace": {
                        "value": "45",
                        "info": "free"
                    },
                    "cpu": {
                        "value": "50",
                        "info": "free"
                    },
                    "networkUsage": {
                        "value": "27",
                        "info": "used"

                    },
                    "dfsBandwith": {
                        "value": "50",
                        "info": "free"
                    },
                    "localDiskIO": {
                        "value": "75",
                        "info": "free"
                    }
                },
                {
                    "name": "Node 2",
                    "id": "34",
                    "ram": {
                        "value": "70",
                        "info": "used"
                    },
                    "diskSpace": {
                        "value": "30",
                        "info": "free"
                    },
                    "cpu": {
                        "value": "27",
                        "info": "free"
                    },
                    "networkUsage": {
                        "value": "36",
                        "info": "used"

                    },
                    "dfsBandwith": {
                        "value": "67",
                        "info": "free"
                    },
                    "localDiskIO": {
                        "value": "65",
                        "info": "free"
                    }
                },
                {
                    "name": "Node 3",
                    "id": "35",
                    "ram": {
                        "value": "2",
                        "info": "used"
                    },
                    "diskSpace": {
                        "value": "79",
                        "info": "free"
                    },
                    "cpu": {
                        "value": "56",
                        "info": "free"
                    },
                    "networkUsage": {
                        "value": "86",
                        "info": "used"

                    },
                    "dfsBandwith": {
                        "value": "45",
                        "info": "free"
                    },
                    "localDiskIO": {
                        "value": "22",
                        "info": "free"
                    }
                },
                {
                    "name": "Node 4",
                    "id": "34",
                    "ram": {
                        "value": "88",
                        "info": "used"
                    },
                    "diskSpace": {
                        "value": "48",
                        "info": "free"
                    },
                    "cpu": {
                        "value": "28",
                        "info": "free"
                    },
                    "networkUsage": {
                        "value": "25",
                        "info": "used"

                    },
                    "dfsBandwith": {
                        "value": "28",
                        "info": "free"
                    },
                    "localDiskIO": {
                        "value": "78",
                        "info": "free"
                    }
                },
                {
                    "name": "Node 5",
                    "id": "34",
                    "ram": {
                        "value": "29",
                        "info": "used"
                    },
                    "diskSpace": {
                        "value": "30",
                        "info": "free"
                    },
                    "cpu": {
                        "value": "55",
                        "info": "free"
                    },
                    "networkUsage": {
                        "value": "30",
                        "info": "used"

                    },
                    "dfsBandwith": {
                        "value": "47",
                        "info": "free"
                    },
                    "localDiskIO": {
                        "value": "25",
                        "info": "free"
                    }
                },
            ];
            return $q.when(clusters);
        }
  });
