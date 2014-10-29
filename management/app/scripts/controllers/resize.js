'use strict';

/**
 * @ngdoc function
 * @name impala2GoApp.controller:ResizeCtrl
 * @description
 * # RedimCtrl
 * Controller of the impala2GoApp
 */
angular.module('impala2GoApp')
    .controller('ResizeCtrl', function ($scope,logger,dataContext,$timeout) {
        var log= logger.getLogFn("ResizeCtrl");
        log("resize controller","",true);
        //fetch data from server
        dataContext.getResizeData().then(function(data){
            $scope.data=data;
        });
        $scope.gridOptions={
            data:"data.clusterList",
            tileContainer:{"textAlign":"left",dataTemplate:[{name:"ram",template:'<div class="form-group tiles-grid-form-group"><label class="col-sm-3 control-label tiles-grid-label">Ram:</label><div class="col-sm-9"><p >$item$</p></div></div>'},{name:"disk",template:'<div class="form-group tiles-grid-form-group"><label class="col-sm-3 control-label tiles-grid-label">Disk:</label><div class="col-sm-9"><p >$item$</p></div></div>'},{name:"cpu",template:'<div class="form-group tiles-grid-form-group"><label class="col-sm-3 control-label tiles-grid-label">Cpu:</label><div class="col-sm-9"><p >$item$</p></div></div>'}],
            },
            columns:
                [   {name:"name" ,displayName:"Nodes" }]
        }

        $scope.onDecrease=function(value){
            if($scope.data.costInfo.provisioned>0){
                $scope.data.costInfo.provisioned-=value;
            }
        }
        $scope.onIncrease=function(value){
            $scope.data.costInfo.provisioned+=value;
        }
    });
