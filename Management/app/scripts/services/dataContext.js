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
            getAllCluster: getAllCluster,
            getSumCluster: getSumCluster,
            getResizeData:getResizeData
        };

        return service;

        function getAllCluster() {
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

            ];
            return $q.when(clusters);
        }
        function getSumCluster() {
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
        function getResizeData() {
            var data = {
                sign:"$",
                bottleneckList:["CPU","RAM"],
                costInfo:{current:0.75,provisioned:0,speedup:30},
                clusterList: [
                {
                    "name": "Node 1",
                    "id": "1",
                    "ram": "24GB",
                    "disk": "2x80 GB flash disk",
                    "cpu": "2 core"
                },
                {
                    "name": "Node 2",
                    "id": "2",
                    "ram": "20GB",
                    "disk": "2x80 GB flash disk",
                    "cpu": "8 core"
                },
                {
                    "name": "Node 4",
                    "id": "4",
                    "ram": "16GB",
                    "disk": "2x80 GB flash disk",
                    "cpu": "4 core"
                },
                {
                    "name": "Node 4",
                    "id": "4",
                    "ram": "16GB",
                    "disk": "2x80 GB flash disk",
                    "cpu": "4 core"
                },
                {
                    "name": "Node 4",
                    "id": "4",
                    "ram": "16GB",
                    "disk": "2x80 GB flash disk",
                    "cpu": "4 core"
                },
                {
                    "name": "Node 4",
                    "id": "4",
                    "ram": "16GB",
                    "disk": "2x80 GB flash disk",
                    "cpu": "4 core"
                },
                {
                    "name": "Node 4",
                    "id": "4",
                    "ram": "16GB",
                    "disk": "2x80 GB flash disk",
                    "cpu": "4 core"
                },
                {
                    "name": "Node 4",
                    "id": "4",
                    "ram": "16GB",
                    "disk": "2x80 GB flash disk",
                    "cpu": "4 core"
                },
            ],
                priceList:[
                    {
                        name:"C3.2xlarge, 8 Cores, 15 GB RAM 2x80 GB flash disk",
                        price:0.5,
                       // purchased:5,
                        priority:1,
                        sign:"$",
                        description:"",
                        items:[
                            {
                                name:"Ram",
                                value:"15",
                                description:"GB"
                            },
                            {
                                name:"Disks",
                                value:"2x80",
                                description:"GB flash disk"
                            },
                            {
                                name:"Cpu",
                                value:"8",
                                description:"cores"
                            }
                        ]
                    },
                    {
                        name:"C3.4xlarge, 16 Cores, 30 GB RAM 2x160 GB flash disk",
                        price:0.25,
                        priority:2,
                    //    purchased:1,
                        sign:"$",
                        description:"",
                        items:[
                            {
                                name:"Ram",
                                value:"30",
                                description:"GB"
                            },
                            {
                                name:"Disks",
                                value:"2x160",
                                description:"GB flash disk"
                            },
                            {
                                name:"Cpu",
                                value:"16",
                                description:"cores"
                            }
                        ]
                    },
                    {
                        name:"C3.8large, 8 Cores, 60 GB RAM 2x320 GB flash disk",
                        price:1,
                   //     purchased:0,
                        priority:3,
                        sign:"$",
                        description:"",
                        items:[
                            {
                                name:"Ram",
                                value:"360",
                                description:"GB"
                            },
                            {
                                name:"Disks",
                                value:"2x320",
                                description:"GB flash disk"
                            },
                            {
                                name:"Cpu",
                                value:"8",
                                description:"cores"
                            }
                        ]
                    }

                ]
            } ;
            return $q.when(data);
        }

    });
