'use strict';

/**
 * @ngdoc function
 * @name impala2GoApp.controller:ClusterCtrl
 * @description
 * # ClusterCtrl
 * Controller of the impala2GoApp
 */
angular.module('impala2GoApp')
    .controller('ClusterCtrl', function ($scope,logger,$interval,dataContext) {
        var logName="ClusterCtrl;"
        var log= logger.getLogFn(logName);
        log("cluster controller","",true);

        //fetch data from server
        dataContext.getAllCluster().then(function(data){
            $scope.dataList= data;
        });

        $scope.getSumClusters=function(state){
            if(state){
                if($scope.warmUp){
                    $scope.allProcess=true;
                }
                else{
                    $scope.allProcess=false;
                }
                dataContext.getSumClusters().then(function(data){
                    $scope.dataList= data;
                });
            }
            else{
                $scope.allProcess=false;
            }
        }
        $scope.getAllClusters=function(state){
            if(state){
                $scope.sum=true;
                $scope.warmUp=true;
                //fetch data from server
                dataContext.getAllClusters().then(function(data){
                    $scope.dataList= data;
                });
            }
        }

        $scope.gridOptions={
            data:"dataList",
            valueSign:{sign:"%",name:"value"},
            valueSort:{predicate:"value",reverse:true},
            controlsTemplate:"<button type='button' ng-click='configuration()' class='btn btn-info btn-sm margin-right-4'><i class='glyphicon glyphicon-cog'></i></button>" +
                "<button type='button' ng-click='configuration()' class='btn btn-info btn-sm margin-right-4'><i class='glyphicon glyphicon-wrench'></i></button>" +
                "<button type='button' ng-click='configuration()'    class='btn btn-info btn-sm'><i class='glyphicon glyphicon-th'></i></button>",

            //  ignoreProperties:["severity"],
            colorState:[{name:"value",min:0,max:25,background:"#9cb4c5",color:"#305d8c"},{name:"value",min:26,max:50,background:"#8ac38b",color:"#356635"},{name:"value",min:51,max:75,background:"#dfb56c",color:"#826430"},{name:"value",min:76,max:100,background:"#953b39",color:"#fff"}],
            columns:
                [   {name:"name" ,displayName:"Nodes" },
                    {name:"ram" ,displayName:"Ram"},
                    {name:"diskSpace" ,displayName:"Disk Space"},
                    {name:"cpu" ,displayName:"Cpu"},
                    {name:"networkUsage" ,displayName:"Network Usage"},
                    {name:"localDiskIO" ,displayName:"Local Disk IO"}
                ]
        }
        $scope.configuration=function(){
            logger.logWarning("Button clicked","","",true);
        }
        //test live data change
        $interval(function(){
            $scope.dataList[0].ram.value++;
        },2000);
        //test live data change
        $interval(function(){
            $scope.dataList[1].networkUsage.value++;
        },1000);

        $scope.$on('tileGridUpdated',function(event,data){
            logger.log("Tile Grid updated","",logName,true);
        })

    });




