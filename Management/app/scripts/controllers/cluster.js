'use strict';

/**
 * @ngdoc function
 * @name impala2GoApp.controller:ClusterCtrl
 * @description
 * # ClusterCtrl
 * Controller of the impala2GoApp
 */
angular.module('impala2GoApp')
    .controller('ClusterCtrl', function ($scope,logger,$interval,$timeout) {
        var log= logger.getLogFn("ClusterCtrl");
        log("cluster controller","",true);
         // some mock data
        $scope.dataList= [
            {node:"Node 1",ram:10, diskSpace:20, cpu:80, networkUsage:20, dfsBandwith:10, localDiskIO:50},
            {node:"Node 2",ram:10, diskSpace:25, cpu:30, networkUsage:22, dfsBandwith:15, localDiskIO:90},
            {node:"Node 3",ram:10, diskSpace:25, cpu:30, networkUsage:22, dfsBandwith:15, localDiskIO:90},
            {node:"Node 4",ram:10, diskSpace:25, cpu:30, networkUsage:22, dfsBandwith:15, localDiskIO:90},
            {node:"Node 5",ram:10, diskSpace:25, cpu:40, networkUsage:22, dfsBandwith:15, localDiskIO:20}
        ];





        $scope.gridOptions={
            data:"dataList",
            controlsTemplate:"<button type='button' class='btn btn-info btn-sm margin-right-4'><span class='glyphicon glyphicon-cog'></span> LinkA</button><button type='button' class='btn btn-info btn-sm'><span class='glyphicon glyphicon-wrench'></span> LinkB</button>",
            columns:
                [   {name:"node" ,displayName:"Nodes"},
                    {name:"ram" ,displayName:"Ram"},
                    {name:"diskSpace" ,displayName:"Disk Space"},
                    {name:"cpu" ,displayName:"Cpu"},
                    {name:"networkUsage" ,displayName:"Network Usage"},
                    {name:"localDiskIO" ,displayName:"Local Disk IO"}
                ]
        }

    //test live data change
//        $interval(function(){
//            $scope.dataList[0].ram++;
//        },1000);
//        $interval(function(){
//            $scope.dataList[1].networkUsage++;
//            $scope.dataList[2].cpu++;
//        },2000);

    });




